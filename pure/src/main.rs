use std::{
    collections::HashMap,
    env,
    fs::File,
    io::Read,
    sync::mpsc,
    thread::{self, Builder},
    time::SystemTime,
};

const BUF_SIZE: usize = 2_usize.pow(18);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let builder = Builder::new().stack_size(8 * BUF_SIZE);

    let handler = builder
        .spawn(|| {
            do_main().unwrap();
        })
        .unwrap();

    handler.join().unwrap();

    Ok(())
}

fn do_main() -> Result<(), Box<dyn std::error::Error>> {
    let start = SystemTime::now();
    eprintln!(
        "Starting: {}",
        SystemTime::now().duration_since(start)?.as_millis()
    );

    let args: Vec<String> = env::args().collect();
    let mut file = File::open(&args[1])?;

    let mut chunk_idx = 0;
    let mut chunk1: [u8; BUF_SIZE] = [0; BUF_SIZE];
    let mut chunk2: [u8; BUF_SIZE] = [0; BUF_SIZE];

    let (results_tx, results_rx) = mpsc::channel();

    let num_workers = match args.get(2) {
        Some(num) => num.parse::<u32>().unwrap(),
        None => 64,
    };

    let workers = (0..num_workers)
        .map(|_| {
            let results_tx_clone = results_tx.clone();
            let (worker_tx, worker_rx) = mpsc::channel();
            let spawn = thread::spawn(move || {
                while let Ok(chunk) = worker_rx.recv() {
                    spawned_working(chunk, results_tx_clone.clone());
                }
            });
            (worker_tx, spawn)
        })
        .collect::<Vec<_>>();

    let mut results: HashMap<String, (f64, f64, f64, u32)> = HashMap::default();

    thread::spawn(move || {
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
                key,
                min,
                max,
                (sum / (total as f64)),
            );
        }
    });
    eprintln!(
        "Spawned workers: {}",
        SystemTime::now().duration_since(start)?.as_millis()
    );

    let mut prev_len = 0;
    loop {
        let len = if chunk_idx % 2 == 0 {
            file.read(&mut chunk1)?
        } else {
            file.read(&mut chunk2)?
        };

        if len == 0 {
            // Length of zero means end of file, finish.
            let chunk = if chunk_idx % 2 == 0 { chunk2 } else { chunk1 };
            if let Some(idx) = chunk.iter().position(|&x| x == b'\n') {
                workers
                    .get(chunk_idx as usize % num_workers as usize)
                    .unwrap()
                    .0
                    .send(chunk[idx..prev_len].to_vec())
                    .unwrap();
            }
            break;
        }
        prev_len = len;

        if chunk_idx == 0 {
            chunk_idx += 1;
            continue;
        }

        let chunk = if chunk_idx % 2 == 1 {
            let idx = chunk1.iter().position(|&x| x == b'\n');
            let mut chunk = if let Some(idx) = idx {
                if chunk_idx != 1 {
                    chunk1[idx + 1..].to_vec()
                } else {
                    chunk1.to_vec()
                }
            } else {
                chunk1.to_vec()
            };
            for ch in chunk2.iter() {
                chunk.push(*ch);
                if ch == &b'\n' {
                    break;
                }
            }
            chunk
        } else {
            let idx = chunk2.iter().position(|&x| x == b'\n');
            let mut chunk = if let Some(idx) = idx {
                chunk2[idx + 1..].to_vec()
            } else {
                chunk2.to_vec()
            };
            for ch in chunk1.iter() {
                chunk.push(*ch);
                if ch == &b'\n' {
                    break;
                }
            }
            chunk
        };

        workers
            .get(chunk_idx as usize % num_workers as usize)
            .unwrap()
            .0
            .send(chunk)
            .unwrap();

        chunk_idx += 1;
    }

    eprintln!(
        "Read all files: {}",
        SystemTime::now().duration_since(start)?.as_millis()
    );

    let mut spawned_workers = vec![];
    for worker in workers {
        drop(worker.0);
        spawned_workers.push(worker.1);
    }
    for worker in spawned_workers {
        worker.join().unwrap();
    }

    eprintln!(
        "Processed all workers: {}",
        SystemTime::now().duration_since(start)?.as_millis()
    );

    Ok(())
}

fn spawned_working(
    chunk: Vec<u8>,
    results_tx: mpsc::Sender<HashMap<String, (f64, f64, f64, u32)>>,
) {
    let mut results: HashMap<String, (f64, f64, f64, u32)> = HashMap::default();
    let mut name = "".to_string();
    let mut number = "".to_string();
    let mut doing_name = true;

    for ch in chunk.iter().skip_while(|ch| **ch == b'\n') {
        if *ch == b';' {
            doing_name = false;
            continue;
        } else if *ch == b'\n' {
            match number.parse::<f64>() {
                Ok(number_parsed) => {
                    let (min, max, sum, total) =
                        results.entry(name).or_insert((f64::MAX, f64::MIN, 0.0, 0));
                    *sum += number_parsed;
                    if number_parsed < *min {
                        *min = number_parsed;
                    }
                    if number_parsed > *max {
                        *max = number_parsed;
                    }
                    *total += 1;
                }
                Err(e) => {
                    eprintln!("Chunk failure: {:?}", &chunk[0..20]);
                    eprintln!("Error parsing number: {}, name: {}", number, name);
                    panic!("{}", e);
                }
            }
            doing_name = true;
            name = "".to_string();
            number = "".to_string();
            continue;
        } else if *ch == b'\0' {
            break;
        }

        if doing_name {
            name.push(*ch as char);
        } else {
            number.push(*ch as char);
        }
    }

    results_tx.send(results).unwrap();
}
