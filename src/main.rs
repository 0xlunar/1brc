use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::thread::JoinHandle;
use std::time::Instant;

fn main() {
    let now = Instant::now();

    let workloads = read_file_into_chunks("./measurements.txt");

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

    let output = output.iter().map(|(station, (min, max, mean))| {
        format!("{station};{:.1};{:.1};{:.1}", *min as f32 * 0.1, *mean as f32 * 0.1, *max as f32 * 0.1)
    }).collect::<Vec<String>>().join("\n");

    let mut file =  File::create("./results.txt").unwrap();
    let _ = file.write(output.as_bytes());

    println!("Finished in: {}ms", now.elapsed().as_millis());
}

// https://stackoverflow.com/a/67128168
fn seek_read<R>(reader: &mut R, offset: u64, buf: &mut [u8]) -> std::io::Result<()> where R: Read + Seek {
    reader.seek(SeekFrom::Start(offset))?;
    reader.read_exact(buf)?;
    Ok(())
}

fn read_file_into_chunks(name: &str) -> Vec<JoinHandle<HashMap<String,(i16, i16)>>>{
    let file = File::open(name).unwrap();
    let file_size_bytes = file.metadata().unwrap().len();
    let thread_count = std::thread::available_parallelism().unwrap().get();
    let mut reader = BufReader::new(file);

    let chunk_divisor = 4;
    let chunk_size = file_size_bytes / thread_count as u64;
    let small_chunk_size = chunk_size / chunk_divisor;
    let mut thread_pool = Vec::new();

    let mut start_byte = 0;
    for _ in 0..(thread_count as u64 * chunk_divisor) {
        let mut buffer = vec![0; small_chunk_size as usize];
        seek_read(&mut reader, start_byte, &mut buffer).unwrap();
        if buffer.len() == 0 {
            break;
        }

        let end_byte = buffer.iter().enumerate().rfind(|(pos, byte)| {
            **byte == b'\n'
        }).unwrap().0 as u64;

        buffer.truncate(end_byte as usize); // Shrink buffer to last new line.
        let thread = std::thread::spawn(move || {
            parse_chunk_bytes(buffer)
        });
        thread_pool.push(thread);
        let end_byte = if end_byte < file_size_bytes {
            end_byte + 1
        } else {
            file_size_bytes
        };
        start_byte = (start_byte + end_byte).min(file_size_bytes);
    }

    thread_pool
}

fn parse_chunk_bytes(buffer: Vec<u8>) -> HashMap<String, (i16, i16)> {
    let mut results = HashMap::new();
    let mut station_full = false;
    let mut station = String::new();
    let mut sign: i16 = 1;
    let mut value: i16 = 0;

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