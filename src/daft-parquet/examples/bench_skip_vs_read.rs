//! Standalone benchmark binary with timing + system metrics (macOS).
//!
//! Simpler than tango-bench, prints a table with wall-clock + CPU time.
//! Good for profiling with samply/Instruments.
//!
//! Run:
//!   cargo build --example bench_skip_vs_read -p daft-parquet --release
//!   ./target/release/examples/bench_skip_vs_read
//!
//! Profile:
//!   samply record ./target/release/examples/bench_skip_vs_read
//!   xcrun xctrace record --template "Time Profiler" --launch ./target/release/examples/bench_skip_vs_read

use std::{fs, path::PathBuf, sync::Arc, time::Instant};

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
    file::{properties::WriterProperties, reader::{FileReader, SerializedFileReader}},
};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const NUM_ROWS: usize = 1_000_000;
const WARMUP_ITERS: usize = 2;
const BENCH_ITERS: usize = 10;

// ---------------------------------------------------------------------------
// System metrics (macOS rusage)
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
fn get_rusage() -> libc::rusage {
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        usage
    }
}

#[cfg(target_os = "macos")]
fn user_cpu_us(start: &libc::rusage, end: &libc::rusage) -> u64 {
    ((end.ru_utime.tv_sec - start.ru_utime.tv_sec) as u64) * 1_000_000
        + (end.ru_utime.tv_usec - start.ru_utime.tv_usec) as u64
}

#[cfg(target_os = "macos")]
fn ctx_switches(start: &libc::rusage, end: &libc::rusage) -> (i64, i64) {
    (
        end.ru_nvcsw - start.ru_nvcsw,
        end.ru_nivcsw - start.ru_nivcsw,
    )
}

#[cfg(not(target_os = "macos"))]
struct FakeRusage;
#[cfg(not(target_os = "macos"))]
fn get_rusage() -> FakeRusage { FakeRusage }
#[cfg(not(target_os = "macos"))]
fn user_cpu_us(_: &FakeRusage, _: &FakeRusage) -> u64 { 0 }
#[cfg(not(target_os = "macos"))]
fn ctx_switches(_: &FakeRusage, _: &FakeRusage) -> (i64, i64) { (0, 0) }

// ---------------------------------------------------------------------------
// RowSelector patterns
// ---------------------------------------------------------------------------

fn build_selectors(case_id: usize, n: usize) -> Vec<RowSelector> {
    match case_id {
        1 => vec![RowSelector::select(n)],
        2 => vec![RowSelector::skip(n)],
        3 => {
            let mut s = Vec::new();
            let mut rem = n;
            while rem > 0 {
                let take = rem.min(100);
                s.push(RowSelector::select(take));
                rem -= take;
                if rem > 0 { s.push(RowSelector::skip(1)); rem -= 1; }
            }
            s
        }
        4 => {
            let mut s = Vec::new();
            let mut rem = n;
            while rem > 0 {
                let skip = rem.min(100);
                s.push(RowSelector::skip(skip));
                rem -= skip;
                if rem > 0 { s.push(RowSelector::select(1)); rem -= 1; }
            }
            s
        }
        5 => {
            if n <= 1 { vec![RowSelector::select(n)] }
            else { vec![RowSelector::skip(n - 1), RowSelector::select(1)] }
        }
        6 => {
            if n <= 1 { vec![RowSelector::skip(n)] }
            else { vec![RowSelector::select(n - 1), RowSelector::skip(1)] }
        }
        _ => panic!("unknown case"),
    }
}

// ---------------------------------------------------------------------------
// File generation — single RG
// ---------------------------------------------------------------------------

fn write_bench_file() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("daft_bench_skip_read_{NUM_ROWS}"));
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("data.parquet");
    if path.exists() { return path; }

    let schema = Arc::new(Schema::new(vec![
        Field::new("col", DataType::Int64, false),
    ]));
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

fn assert_single_row_group(path: &PathBuf) {
    let file = fs::File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    assert_eq!(metadata.num_row_groups(), 1,
        "Expected 1 RG, got {}", metadata.num_row_groups());
    assert_eq!(metadata.row_group(0).num_rows() as usize, NUM_ROWS,
        "Expected {} rows, got {}", NUM_ROWS, metadata.row_group(0).num_rows());
}

// ---------------------------------------------------------------------------
// Read functions
// ---------------------------------------------------------------------------

fn read_with_selection(path: &PathBuf, selectors: &[RowSelector]) -> usize {
    let file = fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_row_selection(RowSelection::from(selectors.to_vec()))
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
// Runner
// ---------------------------------------------------------------------------

fn run_bench(path: &PathBuf, selectors: Option<&[RowSelector]>, iters: usize) -> (u64, u64, i64, i64, usize) {
    // Warmup
    for _ in 0..WARMUP_ITERS {
        match selectors {
            Some(s) => { read_with_selection(path, s); }
            None => { read_all(path); }
        }
    }

    let mut total_wall = 0u64;
    let mut total_cpu = 0u64;
    let mut total_vol = 0i64;
    let mut total_invol = 0i64;
    let mut rows = 0usize;

    for _ in 0..iters {
        let ru_start = get_rusage();
        let t0 = Instant::now();

        rows = match selectors {
            Some(s) => read_with_selection(path, s),
            None => read_all(path),
        };

        let elapsed = t0.elapsed();
        let ru_end = get_rusage();

        total_wall += elapsed.as_micros() as u64;
        total_cpu += user_cpu_us(&ru_start, &ru_end);
        let (v, iv) = ctx_switches(&ru_start, &ru_end);
        total_vol += v;
        total_invol += iv;
    }

    (
        total_wall / iters as u64,
        total_cpu / iters as u64,
        total_vol / iters as i64,
        total_invol / iters as i64,
        rows,
    )
}

fn main() {
    let path = write_bench_file();
    assert_single_row_group(&path);

    println!("=== RowSelection skip/read Microbenchmark ===");
    println!("NUM_ROWS={NUM_ROWS}, WARMUP={WARMUP_ITERS}, ITERS={BENCH_ITERS}");
    println!("File: {}\n", path.display());

    println!(
        "{:<35} {:>10} {:>10} {:>8} {:>8} {:>10}",
        "Case", "Wall(µs)", "CPU(µs)", "VolCSW", "InvCSW", "Rows"
    );
    println!("{}", "-".repeat(85));

    // Baseline: read all (no RowSelection)
    let (w, c, v, iv, r) = run_bench(&path, None, BENCH_ITERS);
    println!(
        "{:<35} {:>10} {:>10} {:>8} {:>8} {:>10}",
        "read_all_baseline", w, c, v, iv, r
    );
    println!();

    let cases: &[(&str, usize)] = &[
        ("case1_select_all", 1),
        ("case2_skip_all", 2),
        ("case3_sel100_skip1 (99%)", 3),
        ("case4_skip100_sel1 (1%)", 4),
        ("case5_skipN1_sel1", 5),
        ("case6_selN1_skip1", 6),
    ];

    for &(name, case_id) in cases {
        let selectors = build_selectors(case_id, NUM_ROWS);
        let (w, c, v, iv, r) = run_bench(&path, Some(&selectors), BENCH_ITERS);
        println!(
            "{:<35} {:>10} {:>10} {:>8} {:>8} {:>10}",
            name, w, c, v, iv, r
        );
    }

    println!("\n=== Interpretation ===");
    println!("If case3 (99% sel, fragmented) is SLOWER than read_all_baseline,");
    println!("it proves skip/read interleaving overhead > full decode cost.");
    println!("If case5 (skip N-1) is FASTER than read_all_baseline,");
    println!("it proves large contiguous skips are effective.");
}
