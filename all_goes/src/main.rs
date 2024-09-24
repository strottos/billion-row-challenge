use std::{env, fs::File, sync::mpsc, thread, time::SystemTime};

use bytes::Bytes;
use memmap2::MmapOptions;
use rustc_hash::FxHashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start = SystemTime::now();
    eprintln!(
        "Starting: {}",
        SystemTime::now().duration_since(start)?.as_millis()
    );

    let args: Vec<String> = env::args().collect();
    let file = File::open(&args[1])?;
    let mmap = unsafe { MmapOptions::new().map(&file)? };

    let chunk_size: usize = match args.get(2) {
        Some(chunk_size) => chunk_size.parse().unwrap_or(2_usize.pow(20)),
        None => 2_usize.pow(20),
    };

    let (results_tx, results_rx) = mpsc::channel();

    let mut results: FxHashMap<Bytes, (f64, f64, f64, u32)> = FxHashMap::default();

    let thread = thread::spawn(move || {
        while let Ok(new_results) = results_rx.recv() {
            for (key, (new_min, new_max, new_sum, new_total)) in new_results {
                let (min, max, sum, total) =
                    results.entry(key).or_insert((f64::MAX, f64::MIN, 0.0, 0));
                *sum += new_sum;
                if new_min < *min {
                    *min = new_min;
                }
                if new_max > *max {
                    *max = new_max;
                }
                *total += new_total;
            }
        }

        println!("Done");
        for (key, (min, max, sum, total)) in results {
            println!(
                "Name: {}, Min: {}, Max: {}, Avg: {:.1}",
                std::str::from_utf8(&key).unwrap(),
                min,
                max,
                (sum / (total as f64)),
            );
        }
    });

    rayon::scope(|s| {
        let mut chunk_from = 0;
        loop {
            let chunk_to = if chunk_from + chunk_size < mmap.len() {
                chunk_from
                    + chunk_size
                    + mmap[chunk_from + chunk_size..]
                        .iter()
                        .position(|&x| x == b'\n')
                        .unwrap()
            } else {
                mmap.len()
            };
            let chunk = &mmap[chunk_from..chunk_to];

            let results_tx_clone = results_tx.clone();
            s.spawn(move |_| {
                spawned_working(chunk, results_tx_clone);
            });

            chunk_from = chunk_to + 1;

            if chunk_from >= mmap.len() {
                break;
            }
        }
    });

    eprintln!(
        "Processed all: {}",
        SystemTime::now().duration_since(start)?.as_millis()
    );

    drop(results_tx);

    thread.join().unwrap();

    Ok(())
}

fn spawned_working(chunk: &[u8], results_tx: mpsc::Sender<FxHashMap<Bytes, (f64, f64, f64, u32)>>) {
    let mut results: FxHashMap<Bytes, (f64, f64, f64, u32)> = FxHashMap::default();

    let mut name_idx_from = 0;
    let mut name_idx_to = 0;
    let mut float_idx_from = 0;

    for (idx, ch) in chunk.iter().enumerate() {
        if *ch == b';' {
            name_idx_to = idx;
            float_idx_from = idx + 1;
        } else if *ch == b'\n' {
            let name = Bytes::copy_from_slice(&chunk[name_idx_from..name_idx_to]);
            let number: f64 = fast_float::parse(&chunk[float_idx_from..idx]).unwrap();

            let (min, max, sum, total) =
                results.entry(name).or_insert((f64::MAX, f64::MIN, 0.0, 0));

            *sum += number;
            if number < *min {
                *min = number;
            }
            if number > *max {
                *max = number;
            }
            *total += 1;

            name_idx_from = idx + 1;
        }
    }

    results_tx.send(results).unwrap();
}
