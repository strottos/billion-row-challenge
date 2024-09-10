use std::sync::{Arc, Mutex};

use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::join;

const SIZE: usize = usize::pow(2, 20);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::open("../data/1_000_000_000_rows").await?;

    let mut chunk_idx = 0;
    let mut chunk1 = vec![0; SIZE];
    let mut chunk2 = vec![0; SIZE];
    let mut spawns = vec![];

    let min = Arc::new(Mutex::new(f64::MAX));
    let max = Arc::new(Mutex::new(0.0));
    let sum = Arc::new(Mutex::new(0.0));
    let total = Arc::new(Mutex::new(0));

    loop {
        let min_clone = min.clone();
        let max_clone = max.clone();
        let sum_clone = sum.clone();
        let total_clone = total.clone();

        let chunk = if chunk_idx % 2 == 0 {
            &mut chunk1
        } else {
            &mut chunk2
        };
        let len = file.read(chunk).await?;
        let chunk_size = chunk.len();
        if len == 0 {
            // Length of zero means end of file. Do final chunk
            eprintln!(
                "Chunk1: {:?} {:?}",
                chunk1.len(),
                &chunk1[chunk1.len() - 10..]
            );
            eprintln!(
                "Chunk2: {:?} {:?}",
                chunk2.len(),
                &chunk2[chunk2.len() - 10..]
            );
            let chunk = if chunk_idx % 2 == 0 { chunk2 } else { chunk1 };
            eprintln!("Final chunk: {:?}", chunk.len());
            spawns.push(tokio::spawn(spawned_thread(
                chunk,
                min_clone,
                max_clone,
                sum_clone,
                total_clone,
            )));
            break;
        } else if len < chunk_size {
            chunk.iter_mut().skip(len).for_each(|ch| *ch = 0);
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

        spawns.push(tokio::spawn(spawned_thread(
            chunk,
            min_clone,
            max_clone,
            sum_clone,
            total_clone,
        )));

        chunk_idx += 1;
    }

    for spawn in spawns {
        spawn.await?;
    }

    println!("Done");

    println!("Min: {}", *min.lock().unwrap());
    println!("Max: {}", *max.lock().unwrap());
    println!("Sum: {}", *sum.lock().unwrap());
    println!(
        "Avg: {}",
        *sum.lock().unwrap() / *total.lock().unwrap() as f64
    );
    println!("Total: {}", *total.lock().unwrap());

    Ok(())
}

async fn spawned_thread(
    chunk: Vec<u8>,
    min: Arc<Mutex<f64>>,
    max: Arc<Mutex<f64>>,
    sum: Arc<Mutex<f64>>,
    total: Arc<Mutex<u32>>,
) {
    let (chunk_min, chunk_sum, chunk_max, chunk_num) = process_chunk(chunk);

    let current_min = *min.lock().unwrap();
    if chunk_min < current_min {
        *min.lock().unwrap() = chunk_min
    };

    let current_max = *max.lock().unwrap();
    if chunk_max > current_max {
        *max.lock().unwrap() = chunk_max
    };

    let current_sum = *sum.lock().unwrap();
    *sum.lock().unwrap() = current_sum + chunk_sum;

    let current_total = *total.lock().unwrap();
    *total.lock().unwrap() = current_total + chunk_num;
}

fn process_chunk(chunk: Vec<u8>) -> (f64, f64, f64, u32) {
    let mut min = f64::MAX;
    let mut sum = 0.0;
    let mut max = 0.0;
    let mut total = 0;

    let lines = chunk.split(|&ch| ch == b'\n');
    for line in lines {
        if line == b"" || line.first() == Some(&0) {
            continue;
        }
        let line = std::str::from_utf8(line).unwrap();
        match line.parse::<f64>() {
            Ok(number) => {
                sum += number;
                if number < min {
                    min = number;
                }
                if number > max {
                    max = number;
                }
                total += 1;
            }
            Err(err) => {
                eprintln!("Line: {:?}", line);
                eprintln!("Error: {}", err);
            }
        }
    }

    (min, sum, max, total)
}
