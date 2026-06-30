//! Microbenchmark: RowSelection skip/read interleaving vs read-only baseline.
//!
//! Tests 9 RowSelection patterns at the ParquetRecordBatchReader level to
//! measure the cost of skip_records() vs just reading everything.
//!
//! Run: cargo bench -p daft-parquet --bench row_selection_skip_vs_read
//!
//! Cases:
//!   1. select(all)                    — pure read via RowSelection (overhead test)
//!   2. skip(all)                      — pure skip baseline
//!   3. repeat(select 100, skip 1)     — high selectivity, fragmented (99%)
//!   4. repeat(skip 100, select 1)     — low selectivity, fragmented (1%)
//!   5. skip(N-1), select(1)           — one big skip, read last row
//!   6. select(N-1), skip(1)           — one big read, skip last row
//!   7. random runs [1..20], ~50% sel  — realistic random predicate, small runs
//!   8. random runs [1..200], ~50% sel — realistic random predicate, medium runs
//!   9. random runs [1..5], ~50% sel   — worst-case fragmentation

use std::{fs, path::PathBuf, sync::Arc};

use arrow::{
    array::{ArrayRef, Int64Array, RecordBatch, StringBuilder},
    datatypes::{DataType, Field, Schema},
};
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector},
    },
    basic::Compression,
    file::{properties::WriterProperties, reader::{FileReader, SerializedFileReader}},
};
use tango_bench::{
    Benchmark, DEFAULT_SETTINGS, IntoBenchmarks, MeasurementSettings, benchmark_fn,
    tango_benchmarks, tango_main,
};

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

/// Total rows in the benchmark file. Single RG so we isolate skip/read cost.
const NUM_ROWS: usize = 1_000_000;

// ---------------------------------------------------------------------------
// RowSelector construction
// ---------------------------------------------------------------------------

fn build_selectors(case_id: usize, n: usize) -> Vec<RowSelector> {
    match case_id {
        // Case 1: select all
        1 => vec![RowSelector::select(n)],

        // Case 2: skip all
        2 => vec![RowSelector::skip(n)],

        // Case 3: repeat(select 100, skip 1) — ~99% selectivity, very fragmented
        3 => {
            let mut selectors = Vec::new();
            let mut remaining = n;
            while remaining > 0 {
                let s = remaining.min(100);
                selectors.push(RowSelector::select(s));
                remaining -= s;
                if remaining > 0 {
                    selectors.push(RowSelector::skip(1));
                    remaining -= 1;
                }
            }
            selectors
        }

        // Case 4: repeat(skip 100, select 1) — ~1% selectivity, fragmented
        4 => {
            let mut selectors = Vec::new();
            let mut remaining = n;
            while remaining > 0 {
                let k = remaining.min(100);
                selectors.push(RowSelector::skip(k));
                remaining -= k;
                if remaining > 0 {
                    selectors.push(RowSelector::select(1));
                    remaining -= 1;
                }
            }
            selectors
        }

        // Case 5: skip(N-1), select(1)
        5 => {
            if n <= 1 {
                vec![RowSelector::select(n)]
            } else {
                vec![RowSelector::skip(n - 1), RowSelector::select(1)]
            }
        }

        // Case 6: select(N-1), skip(1)
        6 => {
            if n <= 1 {
                vec![RowSelector::skip(n)]
            } else {
                vec![RowSelector::select(n - 1), RowSelector::skip(1)]
            }
        }

        // Case 7: random lengths, ~50% selectivity.
        // Alternating select/skip with random run lengths [1..20].
        // Simulates a real predicate that produces an unpredictable pattern.
        7 => {
            let mut selectors = Vec::new();
            let mut remaining = n;
            let mut rng = fastrand::Rng::with_seed(123);
            let mut is_select = rng.bool();
            while remaining > 0 {
                let run = rng.usize(1..=20).min(remaining);
                if is_select {
                    selectors.push(RowSelector::select(run));
                } else {
                    selectors.push(RowSelector::skip(run));
                }
                remaining -= run;
                is_select = !is_select;
            }
            selectors
        }

        // Case 8: random lengths, ~50% selectivity, larger runs [1..200].
        // Less fragmented than case 7 but still random.
        8 => {
            let mut selectors = Vec::new();
            let mut remaining = n;
            let mut rng = fastrand::Rng::with_seed(456);
            let mut is_select = rng.bool();
            while remaining > 0 {
                let run = rng.usize(1..=200).min(remaining);
                if is_select {
                    selectors.push(RowSelector::select(run));
                } else {
                    selectors.push(RowSelector::skip(run));
                }
                remaining -= run;
                is_select = !is_select;
            }
            selectors
        }

        // Case 9: random lengths, ~50% selectivity, tiny runs [1..5].
        // Maximally fragmented — worst case for skip/read interleaving.
        9 => {
            let mut selectors = Vec::new();
            let mut remaining = n;
            let mut rng = fastrand::Rng::with_seed(789);
            let mut is_select = rng.bool();
            while remaining > 0 {
                let run = rng.usize(1..=5).min(remaining);
                if is_select {
                    selectors.push(RowSelector::select(run));
                } else {
                    selectors.push(RowSelector::skip(run));
                }
                remaining -= run;
                is_select = !is_select;
            }
            selectors
        }

        _ => panic!("unknown case_id: {case_id}"),
    }
}

// ---------------------------------------------------------------------------
// File generation — single RG, SNAPPY, one column per file
// ---------------------------------------------------------------------------

fn bench_data_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("daft_bench_rowsel_skip_read_{NUM_ROWS}"));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_int64_file() -> PathBuf {
    let path = bench_data_dir().join("single_rg_int64.parquet");
    if path.exists() {
        return path;
    }

    let schema = Arc::new(Schema::new(vec![Field::new(
        "col",
        DataType::Int64,
        false,
    )]));

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(NUM_ROWS)
        .build();

    let file = fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let chunk = 100_000;
    let mut rng = fastrand::Rng::with_seed(42);
    for offset in (0..NUM_ROWS).step_by(chunk) {
        let n = chunk.min(NUM_ROWS - offset);
        let data: Vec<i64> = (0..n).map(|_| rng.i64(..)).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(data)) as ArrayRef],
        )
        .unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    path
}

fn write_string_file() -> PathBuf {
    let path = bench_data_dir().join("single_rg_string.parquet");
    if path.exists() {
        return path;
    }

    let schema = Arc::new(Schema::new(vec![Field::new(
        "col",
        DataType::Utf8,
        false,
    )]));

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(NUM_ROWS)
        .build();

    let file = fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    // Random strings of length 5..30 (variable length stress-tests skip_records)
    let chunk = 100_000;
    let mut rng = fastrand::Rng::with_seed(99);
    for offset in (0..NUM_ROWS).step_by(chunk) {
        let n = chunk.min(NUM_ROWS - offset);
        let mut builder = StringBuilder::with_capacity(n, n * 16);
        for _ in 0..n {
            let len = rng.usize(5..30);
            let s: String = (0..len).map(|_| rng.alphanumeric()).collect();
            builder.append_value(&s);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(builder.finish()) as ArrayRef],
        )
        .unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    path
}

/// Validate the file is exactly 1 RG with NUM_ROWS rows.
fn assert_single_row_group(path: &PathBuf) {
    let file = fs::File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    assert_eq!(
        metadata.num_row_groups(),
        1,
        "Expected 1 row group, got {} (file: {})",
        metadata.num_row_groups(),
        path.display()
    );
    assert_eq!(
        metadata.row_group(0).num_rows() as usize,
        NUM_ROWS,
        "Expected {} rows in RG, got {} (file: {})",
        NUM_ROWS,
        metadata.row_group(0).num_rows(),
        path.display()
    );
}

// ---------------------------------------------------------------------------
// Read modes
// ---------------------------------------------------------------------------

/// Mode A: selection-aware read (uses RowSelection with skip/read pattern).
fn read_with_selection(path: &PathBuf, selectors: Vec<RowSelector>) -> usize {
    let file = fs::File::open(path).unwrap();
    let row_selection = RowSelection::from(selectors);
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_row_selection(row_selection)
        .with_batch_size(8192)
        .build()
        .unwrap();

    let mut total = 0usize;
    for batch in reader {
        total += batch.unwrap().num_rows();
    }
    total
}

/// Mode B: read-only (no selection, reads all rows).
fn read_all(path: &PathBuf) -> usize {
    let file = fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(8192)
        .build()
        .unwrap();

    let mut total = 0usize;
    for batch in reader {
        total += batch.unwrap().num_rows();
    }
    total
}

// ---------------------------------------------------------------------------
// Benchmark definitions
// ---------------------------------------------------------------------------

fn bench_selection(case_name: &str, dtype: &str, selectors: Vec<RowSelector>, path: PathBuf) -> Benchmark {
    let selectors = Arc::new(selectors);
    let name = format!("{dtype}/selection_{case_name}");
    benchmark_fn(name, move |b| {
        let path = path.clone();
        let selectors = selectors.clone();
        b.iter(move || read_with_selection(&path, (*selectors).clone()))
    })
}

fn bench_read_all_baseline(dtype: &str, path: PathBuf) -> Benchmark {
    let name = format!("{dtype}/read_all_baseline");
    benchmark_fn(name, move |b| {
        let path = path.clone();
        b.iter(move || read_all(&path))
    })
}

fn all_benchmarks() -> impl IntoBenchmarks {
    let int64_path = write_int64_file();
    assert_single_row_group(&int64_path);

    let string_path = write_string_file();
    assert_single_row_group(&string_path);

    let cases: &[(&str, usize)] = &[
        ("case1_select_all", 1),
        ("case2_skip_all", 2),
        ("case3_sel100_skip1", 3),
        ("case4_skip100_sel1", 4),
        ("case5_skip_N1_sel1", 5),
        ("case6_sel_N1_skip1", 6),
        ("case7_rand_1_20", 7),
        ("case8_rand_1_200", 8),
        ("case9_rand_1_5", 9),
    ];

    let mut benchmarks: Vec<Benchmark> = Vec::new();

    // --- int64 ---
    benchmarks.push(bench_read_all_baseline("int64", int64_path.clone()));
    for &(name, case_id) in cases {
        let selectors = build_selectors(case_id, NUM_ROWS);
        benchmarks.push(bench_selection(name, "int64", selectors, int64_path.clone()));
    }

    // --- string ---
    benchmarks.push(bench_read_all_baseline("string", string_path.clone()));
    for &(name, case_id) in cases {
        let selectors = build_selectors(case_id, NUM_ROWS);
        benchmarks.push(bench_selection(name, "string", selectors, string_path.clone()));
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
