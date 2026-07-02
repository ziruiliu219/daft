//! Standalone binary for perf profiling of RowSelection patterns.
//!
//! Usage:
//!   cargo build --release -p daft-parquet --bin row_selection_profile
//!   ./target/release/row_selection_profile <case>
//!
//! Then profile with perf:
//!   perf stat -r 5 -e cycles,instructions,branch-misses,cache-references,cache-misses,\
//!     L1-dcache-loads,L1-dcache-load-misses,LLC-loads,LLC-load-misses \
//!     ./target/release/row_selection_profile case11
//!
//! Available cases:
//!   read_all, skip_all, case7 (rand 1..20000), case8 (rand 1..2000),
//!   case9 (rand 1..200), case10 (rand 1..20), case11 (rand 1..5)

use std::{env, fs, path::PathBuf, sync::Arc};

use arrow::{
    array::{ArrayRef, Int64Array, RecordBatch},
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

const NUM_ROWS: usize = 100_000_000;
const BATCH_SIZE: usize = 8192;

fn bench_data_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("daft_profile_rowsel_{NUM_ROWS}"));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_int64_file() -> PathBuf {
    let path = bench_data_dir().join("single_rg_int64.parquet");

    if path.exists() {
        return path;
    }

    eprintln!("Generating parquet file ({NUM_ROWS} rows)...");
    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int64, false)]));

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
    eprintln!("Done: {}", path.display());
    path
}

fn build_random_selectors(n: usize, max_run: usize, seed: u64) -> Vec<RowSelector> {
    let mut selectors = Vec::new();
    let mut remaining = n;
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut is_select = rng.bool();

    while remaining > 0 {
        let run = rng.usize(1..=max_run).min(remaining);

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

fn build_case(case_name: &str) -> Vec<RowSelector> {
    match case_name {
        "read_all" => vec![RowSelector::select(NUM_ROWS)],
        "skip_all" => vec![RowSelector::skip(NUM_ROWS)],
        "case7" => build_random_selectors(NUM_ROWS, 20_000, 123),
        "case8" => build_random_selectors(NUM_ROWS, 2_000, 123),
        "case9" => build_random_selectors(NUM_ROWS, 200, 456),
        "case10" => build_random_selectors(NUM_ROWS, 20, 456),
        "case11" => build_random_selectors(NUM_ROWS, 5, 789),
        _ => panic!(
            "unknown case: {case_name}\navailable: read_all, skip_all, case7, case8, case9, case10, case11"
        ),
    }
}

fn read_with_selection(path: &PathBuf, selectors: Vec<RowSelector>) -> usize {
    let file = fs::File::open(path).unwrap();
    let row_selection = RowSelection::from(selectors);

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_row_selection(row_selection)
        .with_batch_size(BATCH_SIZE)
        .build()
        .unwrap();

    let mut total = 0usize;

    for batch in reader {
        total += batch.unwrap().num_rows();
    }

    total
}

fn main() {
    let case_name = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: row_selection_profile <case>");
        eprintln!("Cases: read_all, skip_all, case7, case8, case9, case10, case11");
        std::process::exit(1);
    });

    let path = write_int64_file();

    eprintln!("Running case: {case_name}");
    let selectors = build_case(&case_name);

    let num_selectors = selectors.len();
    let selected: usize = selectors.iter().filter(|s| !s.skip).map(|s| s.row_count).sum();
    eprintln!(
        "  selectors={num_selectors}, selected_rows={selected}, selectivity={:.1}%",
        (selected as f64 / NUM_ROWS as f64) * 100.0
    );

    let start = std::time::Instant::now();
    let rows = read_with_selection(&path, selectors);
    let elapsed = start.elapsed();

    println!("case={case_name}, rows={rows}, elapsed={elapsed:?}");
}
