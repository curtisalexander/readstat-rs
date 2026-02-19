//! Micro-benchmark isolating string clone vs move in the cols_to_batch pattern.
//!
//! Run with:
//!   cargo test -p readstat-tests --release string_micro -- --nocapture --ignored

use std::sync::Arc;
use std::time::Instant;
use arrow_array::{ArrayRef, StringArray};

/// Test clone vs move at various string lengths to find the crossover.
#[test]
#[ignore]
fn micro_bench_clone_vs_move() {
    let n_rows: usize = 10_000;
    let n_cols: usize = 500; // fewer cols for faster iteration
    let rounds = 3;

    println!("\n=== Micro-benchmark: clone vs move at different string lengths ===");
    println!("{} rows × {} columns × {} rounds\n", n_rows, n_cols, rounds);
    println!("{:>10} {:>10} {:>10} {:>8}", "str_len", "clone", "move", "speedup");
    println!("{:>10} {:>10} {:>10} {:>8}", "-------", "-----", "----", "-------");

    for str_len in [4, 8, 16, 32, 64, 128, 256] {
        let make_data = || -> Vec<Vec<Option<String>>> {
            (0..n_cols)
                .map(|_| {
                    (0..n_rows)
                        .map(|i| {
                            if i % 20 == 0 {
                                None
                            } else {
                                Some("A".repeat(str_len))
                            }
                        })
                        .collect()
                })
                .collect()
        };

        let mut total_clone = std::time::Duration::ZERO;
        let mut total_move = std::time::Duration::ZERO;

        for _ in 0..rounds {
            let data = make_data();
            let t0 = Instant::now();
            let _arrays: Vec<ArrayRef> = data
                .iter()
                .map(|col| {
                    let vec: Vec<Option<String>> = col.iter().map(|s| s.clone()).collect();
                    Arc::new(StringArray::from(vec)) as ArrayRef
                })
                .collect();
            total_clone += t0.elapsed();

            let data = make_data();
            let t0 = Instant::now();
            let _arrays: Vec<ArrayRef> = data
                .into_iter()
                .map(|col| {
                    let vec: Vec<Option<String>> = col.into_iter().collect();
                    Arc::new(StringArray::from(vec)) as ArrayRef
                })
                .collect();
            total_move += t0.elapsed();
        }

        let avg_clone = total_clone / rounds as u32;
        let avg_move = total_move / rounds as u32;
        let speedup = avg_clone.as_secs_f64() / avg_move.as_secs_f64();
        println!("{:>10} {:>10.2?} {:>10.2?} {:>8.2}x", str_len, avg_clone, avg_move, speedup);
    }
}

/// Benchmark using ReadStatVar enum (the actual type) to account for enum overhead.
#[test]
#[ignore]
fn micro_bench_readstatvar_clone_vs_drain() {
    use readstat::ReadStatVar;

    let n_rows: usize = 10_000;
    let n_cols: usize = 500;
    let str_len: usize = 8;
    let rounds = 3;

    println!("\n=== Micro-benchmark: ReadStatVar clone vs drain (str_len={}) ===", str_len);
    println!("{} rows × {} columns × {} rounds\n", n_rows, n_cols, rounds);

    let make_data = || -> Vec<Vec<ReadStatVar>> {
        (0..n_cols)
            .map(|_| {
                (0..n_rows)
                    .map(|i| {
                        if i % 20 == 0 {
                            ReadStatVar::ReadStat_String(None)
                        } else {
                            ReadStatVar::ReadStat_String(Some("A".repeat(str_len)))
                        }
                    })
                    .collect()
            })
            .collect()
    };

    // Clone path (original code)
    let mut total_clone = std::time::Duration::ZERO;
    for _ in 0..rounds {
        let data = make_data();
        let t0 = Instant::now();
        let _arrays: Vec<ArrayRef> = data
            .iter()
            .map(|col: &Vec<ReadStatVar>| {
                let vec: Vec<Option<String>> = col
                    .iter()
                    .map(|s| {
                        if let ReadStatVar::ReadStat_String(v) = s {
                            v.clone()
                        } else {
                            unreachable!()
                        }
                    })
                    .collect();
                Arc::new(StringArray::from(vec)) as ArrayRef
            })
            .collect();
        total_clone += t0.elapsed();
    }

    // Drain path (new code)
    let mut total_drain = std::time::Duration::ZERO;
    for _ in 0..rounds {
        let mut data = make_data();
        let t0 = Instant::now();
        let _arrays: Vec<ArrayRef> = data
            .drain(..)
            .map(|col: Vec<ReadStatVar>| {
                let vec: Vec<Option<String>> = col
                    .into_iter()
                    .map(|s| {
                        if let ReadStatVar::ReadStat_String(v) = s {
                            v
                        } else {
                            unreachable!()
                        }
                    })
                    .collect();
                Arc::new(StringArray::from(vec)) as ArrayRef
            })
            .collect();
        total_drain += t0.elapsed();
    }

    // StringArray::from_iter path — skip intermediate Vec entirely
    let mut total_from_iter = std::time::Duration::ZERO;
    for _ in 0..rounds {
        let mut data = make_data();
        let t0 = Instant::now();
        let _arrays: Vec<ArrayRef> = data
            .drain(..)
            .map(|col: Vec<ReadStatVar>| {
                let arr = StringArray::from_iter(col.into_iter().map(|s| {
                    if let ReadStatVar::ReadStat_String(v) = s {
                        v
                    } else {
                        unreachable!()
                    }
                }));
                Arc::new(arr) as ArrayRef
            })
            .collect();
        total_from_iter += t0.elapsed();
    }

    // Borrow path — avoid intermediate Vec by borrowing &str
    let mut total_borrow = std::time::Duration::ZERO;
    for _ in 0..rounds {
        let data = make_data();
        let t0 = Instant::now();
        let _arrays: Vec<ArrayRef> = data
            .iter()
            .map(|col: &Vec<ReadStatVar>| {
                let arr = StringArray::from_iter(col.iter().map(|s| {
                    if let ReadStatVar::ReadStat_String(v) = s {
                        v.as_deref()
                    } else {
                        unreachable!()
                    }
                }));
                Arc::new(arr) as ArrayRef
            })
            .collect();
        total_borrow += t0.elapsed();
    }

    let avg_clone = total_clone / rounds as u32;
    let avg_drain = total_drain / rounds as u32;
    let avg_from_iter = total_from_iter / rounds as u32;
    let avg_borrow = total_borrow / rounds as u32;

    println!("{:>20} {:>10} {:>8}", "Approach", "Time", "vs clone");
    println!("{:>20} {:>10} {:>8}", "--------", "----", "--------");
    println!("{:>20} {:>10.2?} {:>8}", "iter+clone+collect", avg_clone, "1.00x");
    println!("{:>20} {:>10.2?} {:>7.2}x", "drain+move+collect", avg_drain,
        avg_clone.as_secs_f64() / avg_drain.as_secs_f64());
    println!("{:>20} {:>10.2?} {:>7.2}x", "drain+from_iter", avg_from_iter,
        avg_clone.as_secs_f64() / avg_from_iter.as_secs_f64());
    println!("{:>20} {:>10.2?} {:>7.2}x", "borrow+from_iter", avg_borrow,
        avg_clone.as_secs_f64() / avg_borrow.as_secs_f64());
}
