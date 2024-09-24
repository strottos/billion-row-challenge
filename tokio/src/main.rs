use std::env;

use rustc_hash::FxHashMap;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let mut file = File::open(&args[1]).await?;

    let buf_size: usize = args
        .get(2)
        .unwrap_or(&"2097152".to_string())
        .parse()
        .unwrap();

    let mut chunk_idx = 0;
    let mut chunk1 = vec![0; buf_size];
    let mut chunk2 = vec![0; buf_size];
    let mut spawns = vec![];

    let mut results: FxHashMap<String, (f64, f64, f64, u32)> = FxHashMap::default();
    let (tx, mut rx) = mpsc::channel(1000);

    tokio::spawn(async move {
        while let Some(new_results) = rx.recv().await {
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

        for (key, (min, max, sum, total)) in results {
            println!(
                "Name: {}, Min: {}, Max: {}, Avg: {:.1}",
                key,
                min,
                max,
                (sum / (total as f64)),
            );
        }
    });

    loop {
        let chunk = if chunk_idx % 2 == 0 {
            //eprintln!("Chunk1: {:?}", chunk1.last());
            &mut chunk1
        } else {
            //eprintln!("Chunk2: {:?}", chunk2.last());
            &mut chunk2
        };
        let chunk_size = chunk.len();
        if chunk_size < buf_size / 2 {
            chunk.resize(buf_size, 0);
        }
        let len = file.read(chunk).await?;
        //eprintln!("Len: {}", len);
        if len == 0 {
            // Length of zero means end of file. Do final chunk
            let chunk = if chunk_idx % 2 == 0 { chunk2 } else { chunk1 };
            spawns.push(tokio::spawn(spawned_working(chunk, tx.clone())));
            break;
        } else if len < chunk_size {
            // eprintln!("Smaller len");
            chunk[len] = 0;
        }

        if chunk_idx == 0 {
            chunk_idx += 1;
            continue;
        }

        let chunk = if chunk_idx % 2 == 1 {
            let mut chunk = chunk1.clone();
            if chunk.last().unwrap() != &b'\n' {
                for (idx, ch) in chunk2.iter().enumerate() {
                    chunk.push(*ch);
                    if ch == &b'\n' {
                        chunk2 = chunk2.split_off(idx + 1);
                        break;
                    }
                }
            }
            chunk
        } else {
            let mut chunk = chunk2.clone();
            if chunk.last().unwrap() != &b'\n' {
                for (idx, ch) in chunk1.iter().enumerate() {
                    chunk.push(*ch);
                    if ch == &b'\n' {
                        chunk1 = chunk1.split_off(idx + 1);
                        break;
                    }
                }
            }
            chunk
        };

        spawns.push(tokio::spawn(spawned_working(chunk, tx.clone())));

        chunk_idx += 1;
    }

    for spawn in spawns {
        spawn.await?;
    }

    println!("Done");

    Ok(())
}

async fn spawned_working(
    chunk: Vec<u8>,
    tx: mpsc::Sender<FxHashMap<String, (f64, f64, f64, u32)>>,
) {
    let mut results: FxHashMap<String, (f64, f64, f64, u32)> = FxHashMap::default();
    let mut name = "";
    let mut idx = 0;
    for j in 0..chunk.len() {
        if chunk[j] == b';' {
            name = std::str::from_utf8(&chunk[idx..j]).unwrap();
            idx = j + 1;
        } else if chunk[j] == b'\n' {
            let number = std::str::from_utf8(&chunk[idx..j])
                .unwrap()
                .parse::<f64>()
                .unwrap();
            let (min, max, sum, total) =
                results
                    .entry(name.to_string())
                    .or_insert((f64::MAX, f64::MIN, 0.0, 0));
            *sum += number;
            if number < *min {
                *min = number;
            }
            if number > *max {
                *max = number;
            }
            *total += 1;
            idx = j + 1;
        }
    }

    tx.send(results).await.unwrap();
}
