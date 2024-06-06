use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::thread::{JoinHandle, sleep};
use std::time::{Duration, Instant};
use memmap2::{Mmap, MmapOptions};

fn main() {
    let start = Instant::now();
    let now = Instant::now();

    let name = "./measurements.txt";
    let file = File::open(name).unwrap();
    let file_size_bytes = file.metadata().unwrap().len();
    let thread_count = std::thread::available_parallelism().unwrap().get() as u64;
    let mmap = unsafe {
        MmapOptions::new().map(&file).unwrap()
    };
    let mmap_buffer = Arc::new(mmap);

    // let workloads = read_file_into_chunks("./measurements.txt");
    let workloads = read_file_into_chunks(mmap_buffer, thread_count, file_size_bytes);
    println!("Workload Finished: {}ms", now.elapsed().as_millis());

    let now = Instant::now();
    let mut output = HashMap::new();
    for workload in workloads {
        let data = workload.join().unwrap();
        for (k, (min, max)) in data {
            output.entry(k).and_modify(|(o_min, o_max, mean)| {
                let mut updated = false;
                if min < *o_min {
                    *o_min = min;
                    updated = true;
                }
                if max > *o_max {
                    *o_max = max;
                    updated = true;
                }
                if updated {
                    *mean = (*o_min + *o_max) / 2;
                }
            }).or_insert((min, max, (min + max) / 2));
        }
    }
    println!("Workload Merging: {}ms", now.elapsed().as_millis());

    let now = Instant::now();
    let output = output.iter().map(|(station, (min, max, mean))| {
        format!("{station};{:.1};{:.1};{:.1}", *min as f32 * 0.1, *mean as f32 * 0.1, *max as f32 * 0.1)
    }).collect::<Vec<String>>().join("\n");
    println!("Output Parsing: {}ms", now.elapsed().as_millis());

    let mut file =  File::create("./results.txt").unwrap();
    let _ = file.write(output.as_bytes());

    println!("All Finished in: {}ms", start.elapsed().as_millis());
    sleep(Duration::from_secs(30))
}

fn read_file_into_chunks(mmap: Arc<Mmap>, thread_count: u64, file_size_bytes: u64,) -> Vec<JoinHandle<HashMap<String,(i16, i16)>>>{
    let chunk_divisor = 4;
    let chunk_size = file_size_bytes / thread_count;
    let small_chunk_size = chunk_size / chunk_divisor;
    println!("File Size: {} bytes\nSmall Chunk Size: {}\nThread Count: {}", file_size_bytes, small_chunk_size, thread_count);

    let mut thread_pool = Vec::new();
    let mut start_byte = 0;
    for _ in 0..(thread_count * chunk_divisor) {
        // let start = Instant::now();
        let end_byte = (small_chunk_size + start_byte) as usize;
        let buffer = &mmap[start_byte as usize..end_byte];
        if buffer.len() == 0 {
            break;
        }
        let end_byte = buffer.iter().enumerate().rfind(|(_, byte)| {
            **byte == b'\n'
        }).unwrap().0 as u64;

        let buffer = Arc::clone(&mmap);
        let thread = std::thread::spawn(move || {
            parse_chunk_bytes(buffer, start_byte, start_byte + end_byte)
        });
        thread_pool.push(thread);
        let end_byte = if end_byte < file_size_bytes {
            end_byte + 1
        } else {
            file_size_bytes
        };
        start_byte = (start_byte + end_byte).min(file_size_bytes);
        // println!("Total time: {}ms", start.elapsed().as_millis());
    }

    thread_pool
}

fn parse_chunk_bytes(buffer: Arc<Mmap>, start_byte: u64, end_byte: u64) -> HashMap<String, (i16, i16)> {
    let mut results = HashMap::new();
    let mut station_full = false;
    let mut station = String::new();
    let mut sign: i16 = 1;
    let mut value: i16 = 0;
    let buffer = &buffer[start_byte as usize..end_byte as usize];

    let mut buffer = buffer.iter();
    while let Some(byte) = buffer.next() {
        match byte {
            b';' => {
                station_full = true;
            },
            b'-' => {
                sign = -1;
            },
            b'.' => {
                continue;
            },
            b'\n' => {
                value = sign * value;
                results.entry(station).and_modify(|(min, max)| {
                    if value < *min {
                        *min = value;
                    } else if value > *max {
                        *max = value;
                    }
                }).or_insert((value, value));

                station_full = false;
                station = String::new();
                sign = 1;
                value = 0;
            }
            _ => {
                let char = *byte as char;
                if !station_full {
                    station.push(char);
                } else if char.is_digit(10) {
                    value = value * 10 + (char.to_digit(10).unwrap() as i16);
                }
            }
        }
    }

    results
}
