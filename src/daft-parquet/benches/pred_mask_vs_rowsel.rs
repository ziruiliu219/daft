//! Microbenchmark: pred_mask (full decode + vectorized filter) vs RowSelection
//! (skip_records/read_records interleaving).
//!
//! Generates parquet files with varying selectivity predicates and measures
//! both approaches at the parquet-crate level (no Daft overhead).
//!
//! Run: cargo bench -p daft-parquet --bench pred_mask_vs_rowsel

use std::{fs, path::PathBuf, sync::Arc};

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, RecordBatch},
    compute::filter,
    datatypes::{DataType, Field, Schema},
};
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector},
    },
    basic::Compression,
    file::properties::WriterProperties,
};
use tango_bench::{
    Benchmark, DEFAULT_SETTINGS, IntoBenchmarks, MeasurementSettings, benchmark_fn,
    tango_benchmarks, tango_main,
};

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

const NUM_ROWS: usize = 1_000_000;
const ROW_GROUP_SIZE: usize = 100_000; // 10 RGs

/// Selectivities to benchmark (fraction of rows matching predicate).
const SELECTIVITIES: &[(&str, f64)] = &[
    ("sel_1pct", 0.01),   // very sparse → lots of small skips
    ("sel_10pct", 0.10),  // moderate
    ("sel_50pct", 0.50),  // half selected
    ("sel_90pct", 0.90),  // mostly selected
];

// ---------------------------------------------------------------------------
// File generation
// ---------------------------------------------------------------------------

fn bench_data_dir() -> PathBuf {
    let dir = std::env::temp_dir().join("daft_bench_pred_mask_vs_rowsel");
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// Write a parquet file with columns: id(i64), pred_col(f64), data_col(i64).
/// `pred_col` values are uniform in [0, 1) so we can control selectivity
/// by filtering pred_col < threshold.
fn write_bench_file() -> PathBuf {
    let path = bench_data_dir().join("bench_data.parquet");
    if path.exists() {
        return path;
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("pred_col", DataType::Float64, false),
        Field::new("data_col", DataType::Int64, false),
    ]));

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(ROW_GROUP_SIZE)
        .build();

    let file = fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    // Write in chunks to keep memory bounded
    let chunk = 50_000;
    let mut rng = fastrand::Rng::with_seed(42);
    for offset in (0..NUM_ROWS).step_by(chunk) {
        let n = chunk.min(NUM_ROWS - offset);
        let ids: Vec<i64> = (offset..offset + n).map(|i| i as i64).collect();
        let preds: Vec<f64> = (0..n).map(|_| rng.f64()).collect();
        let data: Vec<i64> = (0..n).map(|_| rng.i64(..)).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(Float64Array::from(preds)) as ArrayRef,
                Arc::new(Int64Array::from(data)) as ArrayRef,
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    path
}

// ---------------------------------------------------------------------------
// Approach A: Full decode + vectorized filter (pred_mask style)
// ---------------------------------------------------------------------------

/// Read ALL rows from data_col, then filter by mask.
fn read_full_decode_filter(path: &PathBuf, threshold: f64) -> usize {
    let file = fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(8192)
        .build()
        .unwrap();

    let mut total = 0usize;
    for batch in reader {
        let batch = batch.unwrap();
        let pred_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let data_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Build mask
        let threshold_scalar = Float64Array::new_scalar(threshold);
        let mask: arrow::array::BooleanArray =
            arrow::compute::kernels::cmp::lt(pred_col, &threshold_scalar).unwrap();

        // Filter data col
        let filtered = filter(data_col, &mask).unwrap();
        total += filtered.len();
    }
    total
}

// ---------------------------------------------------------------------------
// Approach B: RowSelection-based skip/read (old style)
// ---------------------------------------------------------------------------

/// Two-pass: first pass reads pred_col to build RowSelection, second pass
/// reads data_col with that selection applied.
fn read_rowsel_skip(path: &PathBuf, threshold: f64) -> usize {
    // Pass 1: read only pred_col to build the RowSelection
    let file = fs::File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let metadata = builder.metadata().clone();

    // Read pred_col (index 1) to build mask
    let file = fs::File::open(path).unwrap();
    let pred_reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_projection(parquet::arrow::ProjectionMask::leaves(
            metadata.file_metadata().schema_descr(),
            [1], // pred_col
        ))
        .with_batch_size(8192)
        .build()
        .unwrap();

    let mut selectors: Vec<RowSelector> = Vec::new();
    for batch in pred_reader {
        let batch = batch.unwrap();
        let pred_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // RLE-encode the mask into selectors
        let mut current_select = false;
        let mut current_count = 0usize;
        for i in 0..pred_col.len() {
            let val = pred_col.value(i) < threshold;
            if val == current_select {
                current_count += 1;
            } else {
                if current_count > 0 {
                    selectors.push(if current_select {
                        RowSelector::select(current_count)
                    } else {
                        RowSelector::skip(current_count)
                    });
                }
                current_select = val;
                current_count = 1;
            }
        }
        if current_count > 0 {
            selectors.push(if current_select {
                RowSelector::select(current_count)
            } else {
                RowSelector::skip(current_count)
            });
        }
    }
    let row_selection = RowSelection::from(selectors);

    // Pass 2: read data_col with RowSelection
    let file = fs::File::open(path).unwrap();
    let data_reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_projection(parquet::arrow::ProjectionMask::leaves(
            metadata.file_metadata().schema_descr(),
            [2], // data_col
        ))
        .with_row_selection(row_selection)
        .with_batch_size(8192)
        .build()
        .unwrap();

    let mut total = 0usize;
    for batch in data_reader {
        total += batch.unwrap().num_rows();
    }
    total
}

// ---------------------------------------------------------------------------
// Benchmark definitions
// ---------------------------------------------------------------------------

fn bench_pred_mask(name: &'static str, path: PathBuf, threshold: f64) -> Benchmark {
    benchmark_fn(format!("pred_mask_{name}"), move |b| {
        let path = path.clone();
        b.iter(move || read_full_decode_filter(&path, threshold))
    })
}

fn bench_rowsel(name: &'static str, path: PathBuf, threshold: f64) -> Benchmark {
    benchmark_fn(format!("rowsel_{name}"), move |b| {
        let path = path.clone();
        b.iter(move || read_rowsel_skip(&path, threshold))
    })
}

fn all_benchmarks() -> impl IntoBenchmarks {
    let path = write_bench_file();

    let mut benchmarks: Vec<Benchmark> = Vec::new();
    for &(name, threshold) in SELECTIVITIES {
        benchmarks.push(bench_pred_mask(name, path.clone(), threshold));
        benchmarks.push(bench_rowsel(name, path.clone(), threshold));
    }
    benchmarks
}

const SETTINGS: MeasurementSettings = MeasurementSettings {
    min_iterations_per_sample: 3,
    cache_firewall: Some(64),
    yield_before_sample: true,
    randomize_stack: Some(4096),
    ..DEFAULT_SETTINGS
};

tango_benchmarks!(all_benchmarks());
tango_main!(SETTINGS);
