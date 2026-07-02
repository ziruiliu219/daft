//! Microbenchmark: RowSelection skip/read vs read-all baseline.
//!
//! Systematically varies selectivity, fragmentation, and skip granularity
//! (within-page vs cross-page) to find the threshold where RowSelection
//! becomes slower than full decode + filter.
//!
//! Run: cargo bench -p daft-parquet --bench row_selection_skip_vs_read
//!
//! Dimensions tested:
//!   - Selectivity: 1%, 5%, 10%, 20%, 30%, 50%, 70%, 90%
//!   - Fragmentation: clustered (one block), periodic (fixed run), random (varying run)
//!   - Skip granularity: tiny (1-5), small (1-20), medium (50-150), large (cross-page ~200k)
//!   - Data type: int64, string

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

const NUM_ROWS: usize = 1_000_000;

// Approximate rows per page for int64 with default 1MB page:
// 1MB / 8 bytes = 131072 rows/page
const APPROX_PAGE_ROWS: usize = 131_072;

// ---------------------------------------------------------------------------
// Selector builders
// ---------------------------------------------------------------------------

/// Clustered: one contiguous block of selected rows at the start,
/// rest skipped. Best case for RowSelection.
fn build_clustered(n: usize, selectivity: f64) -> Vec<RowSelector> {
    let selected = (n as f64 * selectivity) as usize;
    let skipped = n - selected;
    let mut v = Vec::new();
    if skipped > 0 {
        v.push(RowSelector::skip(skipped));
    }
    if selected > 0 {
        v.push(RowSelector::select(selected));
    }
    v
}

/// Periodic: fixed-size runs alternating skip/select to achieve target selectivity.
/// `run_len` controls the select run; skip run is computed from selectivity.
fn build_periodic(n: usize, selectivity: f64, select_run: usize) -> Vec<RowSelector> {
    // For selectivity s: select_run / (select_run + skip_run) = s
    // → skip_run = select_run * (1-s) / s
    let skip_run = ((select_run as f64) * (1.0 - selectivity) / selectivity).round() as usize;
    let skip_run = skip_run.max(1);

    let mut selectors = Vec::new();
    let mut remaining = n;
    let mut is_skip = true; // start with skip so selected rows are spread out
    while remaining > 0 {
        if is_skip {
            let take = remaining.min(skip_run);
            selectors.push(RowSelector::skip(take));
            remaining -= take;
        } else {
            let take = remaining.min(select_run);
            selectors.push(RowSelector::select(take));
            remaining -= take;
        }
        is_skip = !is_skip;
    }
    selectors
}

/// Random: random run lengths in [min_run..=max_run], alternating skip/select.
/// Selectivity is approximate (depends on random seed).
fn build_random(n: usize, selectivity: f64, min_run: usize, max_run: usize, seed: u64) -> Vec<RowSelector> {
    let mut selectors = Vec::new();
    let mut remaining = n;
    let mut rng = fastrand::Rng::with_seed(seed);

    // Bias: to achieve target selectivity, we adjust the probability of
    // starting with select vs skip and the relative run lengths.
    // Simple approach: alternate but weight the run lengths.
    let select_weight = selectivity;
    let skip_weight = 1.0 - selectivity;

    let mut is_select = rng.f64() < selectivity;
    while remaining > 0 {
        let base_run = rng.usize(min_run..=max_run);
        // Weight the run length by selectivity to approximate target
        let run = if is_select {
            ((base_run as f64) * select_weight * 2.0).round() as usize
        } else {
            ((base_run as f64) * skip_weight * 2.0).round() as usize
        };
        let run = run.max(1).min(remaining);

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

/// Cross-page skip: large skips that span entire pages, with small select runs.
/// Simulates a predicate that selects rows from every Nth page.
fn build_cross_page_skip(n: usize, selectivity: f64) -> Vec<RowSelector> {
    // Select `select_rows` rows every `period` rows, where period ≈ page size.
    let period = APPROX_PAGE_ROWS;
    let select_per_period = (period as f64 * selectivity).round() as usize;
    let select_per_period = select_per_period.max(1);
    let skip_per_period = period - select_per_period;

    let mut selectors = Vec::new();
    let mut remaining = n;
    while remaining > 0 {
        let skip = remaining.min(skip_per_period);
        if skip > 0 {
            selectors.push(RowSelector::skip(skip));
            remaining -= skip;
        }
        if remaining == 0 {
            break;
        }
        let sel = remaining.min(select_per_period);
        if sel > 0 {
            selectors.push(RowSelector::select(sel));
            remaining -= sel;
        }
    }
    selectors
}

// ---------------------------------------------------------------------------
// File generation
// ---------------------------------------------------------------------------

fn bench_data_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("daft_bench_rowsel_v2_{NUM_ROWS}"));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_int64_file() -> PathBuf {
    let path = bench_data_dir().join("int64.parquet");
    if path.exists() { return path; }

    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int64, false)]));
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(NUM_ROWS)
        .build();
    let file = fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    let mut rng = fastrand::Rng::with_seed(42);
    for offset in (0..NUM_ROWS).step_by(100_000) {
        let n = 100_000.min(NUM_ROWS - offset);
        let data: Vec<i64> = (0..n).map(|_| rng.i64(..)).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(data)) as ArrayRef],
        ).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    path
}

fn write_string_file() -> PathBuf {
    let path = bench_data_dir().join("string.parquet");
    if path.exists() { return path; }

    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(NUM_ROWS)
        .build();
    let file = fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    let mut rng = fastrand::Rng::with_seed(99);
    for offset in (0..NUM_ROWS).step_by(100_000) {
        let n = 100_000.min(NUM_ROWS - offset);
        let mut builder = StringBuilder::with_capacity(n, n * 16);
        for _ in 0..n {
            let len = rng.usize(5..30);
            let s: String = (0..len).map(|_| rng.alphanumeric()).collect();
            builder.append_value(&s);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(builder.finish()) as ArrayRef],
        ).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    path
}

fn assert_single_row_group(path: &PathBuf) {
    let file = fs::File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    assert_eq!(metadata.num_row_groups(), 1,
        "Expected 1 RG, got {} ({})", metadata.num_row_groups(), path.display());
    assert_eq!(metadata.row_group(0).num_rows() as usize, NUM_ROWS,
        "Expected {} rows, got {} ({})", NUM_ROWS, metadata.row_group(0).num_rows(), path.display());
}

// ---------------------------------------------------------------------------
// Read functions
// ---------------------------------------------------------------------------

fn read_with_selection(path: &PathBuf, selectors: Vec<RowSelector>) -> usize {
    let file = fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_row_selection(RowSelection::from(selectors))
        .with_batch_size(8192)
        .build()
        .unwrap();
    reader.into_iter().map(|b| b.unwrap().num_rows()).sum()
}

fn read_all(path: &PathBuf) -> usize {
    let file = fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(8192)
        .build()
        .unwrap();
    reader.into_iter().map(|b| b.unwrap().num_rows()).sum()
}

// ---------------------------------------------------------------------------
// Benchmark registration
// ---------------------------------------------------------------------------

fn bench_sel(name: String, selectors: Vec<RowSelector>, path: PathBuf) -> Benchmark {
    let selectors = Arc::new(selectors);
    benchmark_fn(name, move |b| {
        let path = path.clone();
        let selectors = selectors.clone();
        b.iter(move || read_with_selection(&path, (*selectors).clone()))
    })
}

fn bench_baseline(name: String, path: PathBuf) -> Benchmark {
    benchmark_fn(name, move |b| {
        let path = path.clone();
        b.iter(move || read_all(&path))
    })
}

// ---------------------------------------------------------------------------
// Diagnostic: print selector pattern stats
// ---------------------------------------------------------------------------

fn print_selector_stats(name: &str, selectors: &[RowSelector]) {
    let num_selectors = selectors.len();
    let total_rows: usize = selectors.iter().map(|s| s.row_count).sum();
    let selected_rows: usize = selectors.iter().filter(|s| !s.skip).map(|s| s.row_count).sum();
    let skipped_rows: usize = selectors.iter().filter(|s| s.skip).map(|s| s.row_count).sum();
    let num_select_runs: usize = selectors.iter().filter(|s| !s.skip).count();
    let num_skip_runs: usize = selectors.iter().filter(|s| s.skip).count();
    let avg_select_run = if num_select_runs > 0 { selected_rows / num_select_runs } else { 0 };
    let avg_skip_run = if num_skip_runs > 0 { skipped_rows / num_skip_runs } else { 0 };
    let selectivity = if total_rows > 0 { (selected_rows as f64 / total_rows as f64) * 100.0 } else { 0.0 };

    // Print first 10 selectors as sample
    let sample: Vec<String> = selectors.iter().take(10).map(|s| {
        if s.skip { format!("skip({})", s.row_count) }
        else { format!("sel({})", s.row_count) }
    }).collect();
    let suffix = if selectors.len() > 10 { ", ..." } else { "" };

    eprintln!(
        "[pattern] {name}: selectors={num_selectors} sel_rows={selected_rows} skip_rows={skipped_rows} \
         selectivity={selectivity:.1}% avg_sel_run={avg_select_run} avg_skip_run={avg_skip_run} \
         sample=[{sample}]{suffix}",
        sample = sample.join(", "),
    );
}

// ---------------------------------------------------------------------------
// All benchmarks
// ---------------------------------------------------------------------------

fn all_benchmarks() -> impl IntoBenchmarks {
    let int64_path = write_int64_file();
    assert_single_row_group(&int64_path);
    let string_path = write_string_file();
    assert_single_row_group(&string_path);

    let selectivities: &[(&str, f64)] = &[
        ("01pct", 0.01),
        ("05pct", 0.05),
        ("10pct", 0.10),
        ("20pct", 0.20),
        ("30pct", 0.30),
        ("50pct", 0.50),
        ("70pct", 0.70),
        ("90pct", 0.90),
    ];

    let mut benchmarks: Vec<Benchmark> = Vec::new();

    for (dtype, path) in [("int64", &int64_path), ("str", &string_path)] {
        // Baseline: read all
        benchmarks.push(bench_baseline(
            format!("{dtype}/baseline_read_all"),
            path.clone(),
        ));

        for &(sel_name, sel) in selectivities {
            // --- Clustered: one big skip + one big select ---
            let sels = build_clustered(NUM_ROWS, sel);
            print_selector_stats(&format!("{dtype}/{sel_name}/clustered"), &sels);
            benchmarks.push(bench_sel(
                format!("{dtype}/{sel_name}/clustered"),
                sels,
                path.clone(),
            ));

            // --- Periodic with tiny runs (select 1-3 rows at a time) ---
            let sels = build_periodic(NUM_ROWS, sel, 2);
            print_selector_stats(&format!("{dtype}/{sel_name}/periodic_run2"), &sels);
            benchmarks.push(bench_sel(
                format!("{dtype}/{sel_name}/periodic_run2"),
                sels,
                path.clone(),
            ));

            // --- Periodic with small runs (select ~10 rows at a time) ---
            let sels = build_periodic(NUM_ROWS, sel, 10);
            print_selector_stats(&format!("{dtype}/{sel_name}/periodic_run10"), &sels);
            benchmarks.push(bench_sel(
                format!("{dtype}/{sel_name}/periodic_run10"),
                sels,
                path.clone(),
            ));

            // --- Periodic with medium runs (select ~100 rows at a time) ---
            let sels = build_periodic(NUM_ROWS, sel, 100);
            print_selector_stats(&format!("{dtype}/{sel_name}/periodic_run100"), &sels);
            benchmarks.push(bench_sel(
                format!("{dtype}/{sel_name}/periodic_run100"),
                sels,
                path.clone(),
            ));

            // --- Periodic with large runs (select ~1000 rows at a time) ---
            let sels = build_periodic(NUM_ROWS, sel, 1000);
            print_selector_stats(&format!("{dtype}/{sel_name}/periodic_run1000"), &sels);
            benchmarks.push(bench_sel(
                format!("{dtype}/{sel_name}/periodic_run1000"),
                sels,
                path.clone(),
            ));

            // --- Cross-page: skip spans entire pages ---
            let sels = build_cross_page_skip(NUM_ROWS, sel);
            print_selector_stats(&format!("{dtype}/{sel_name}/cross_page"), &sels);
            benchmarks.push(bench_sel(
                format!("{dtype}/{sel_name}/cross_page"),
                sels,
                path.clone(),
            ));

            // --- Random tiny [1..5] ---
            let sels = build_random(NUM_ROWS, sel, 1, 5, 111);
            print_selector_stats(&format!("{dtype}/{sel_name}/random_1_5"), &sels);
            benchmarks.push(bench_sel(
                format!("{dtype}/{sel_name}/random_1_5"),
                sels,
                path.clone(),
            ));

            // --- Random small [1..20] ---
            let sels = build_random(NUM_ROWS, sel, 1, 20, 222);
            print_selector_stats(&format!("{dtype}/{sel_name}/random_1_20"), &sels);
            benchmarks.push(bench_sel(
                format!("{dtype}/{sel_name}/random_1_20"),
                sels,
                path.clone(),
            ));

            // --- Random medium [50..150] ---
            let sels = build_random(NUM_ROWS, sel, 50, 150, 333);
            print_selector_stats(&format!("{dtype}/{sel_name}/random_50_150"), &sels);
            benchmarks.push(bench_sel(
                format!("{dtype}/{sel_name}/random_50_150"),
                sels,
                path.clone(),
            ));
        }
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
