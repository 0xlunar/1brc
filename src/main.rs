use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Error, Read, Seek, SeekFrom, Write};
use std::slice::Chunks;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

fn main() -> Result<(), Error> {
    let now = Instant::now();
    // // let (buffer, chunks) = read_file_into_chunks("./measurements.txt");
    // let chunks = read_file_into_chunks_seek_read("./measurements.txt");
    // println!("Reading File Time: {}ms", now.elapsed().as_millis());
    // let now = Instant::now();
    //
    //
    // // let workloads = chunks.into_iter().map(|(start, end)| {
    // //     let buffer = buffer.get(start as usize..=end as usize).unwrap().to_vec();
    // //     std::thread::spawn(move || {
    // //         parse_chunk_bytes(buffer)
    // //     })
    // // }).collect::<Vec<JoinHandle<_>>>();
    //
    // let workloads = chunks.into_iter().map(|buffer| {
    //     std::thread::spawn(move || {
    //         parse_chunk_bytes(buffer)
    //     })
    // }).collect::<Vec<JoinHandle<_>>>();
    //

    let workloads = read_file_into_chunks_seek_read_parallel("./measurements.txt");

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

    println!("Finished in: {}ms", now.elapsed().as_millis());

    let mut file =  File::create("./results.txt").unwrap();
    let _ = file.write(output.as_bytes());

    Ok(())
}

// https://stackoverflow.com/a/67128168
fn seek_read<R>(reader: &mut R, offset: u64, buf: &mut [u8]) -> std::io::Result<()> where R: Read + Seek {
    reader.seek(SeekFrom::Start(offset))?;
    reader.read_exact(buf)?;
    Ok(())
}

fn read_file_into_chunks_seek_read(name: &str) -> Vec<Vec<u8>>{
    let file = File::open(name).unwrap();
    let file_size_bytes = file.metadata().unwrap().len();
    println!("File Size: {:?}", file_size_bytes);
    let thread_count = std::thread::available_parallelism().unwrap().get();
    let mut reader = BufReader::new(file);

    let mut chunks = Vec::new();
    let chunk_size = file_size_bytes / thread_count as u64;
    println!("Chunk Size: {}", chunk_size);

    let mut start_byte = 0;
    for _ in 0..thread_count {
        let mut buffer = vec![0; chunk_size as usize];
        seek_read(&mut reader, start_byte, &mut buffer).unwrap();
        if buffer.len() == 0 {
            break;
        }
        let end_byte = buffer.iter().enumerate().rfind(|(pos, byte)| {
            **byte == b'\n'
        }).unwrap().0 as u64;
        buffer.truncate(end_byte as usize); // Shrink buffer to last new line.
        let end_byte = if end_byte < file_size_bytes {
            end_byte + 1
        } else {
            file_size_bytes
        };
        start_byte = (start_byte + end_byte).min(file_size_bytes);
        chunks.push(buffer);
    }

    chunks
}

fn read_file_into_chunks_seek_read_parallel(name: &str) -> Vec<JoinHandle<HashMap<String,(i16,i16)>>>{
    let file = File::open(name).unwrap();
    let file_size_bytes = file.metadata().unwrap().len();
    println!("File Size: {:?}", file_size_bytes);
    let thread_count = std::thread::available_parallelism().unwrap().get();
    let mut reader = BufReader::new(file);

    let chunk_size = file_size_bytes / thread_count as u64;
    println!("Chunk Size: {}", chunk_size);
    let mut thread_pool = Vec::new();

    let mut start_byte = 0;
    for _ in 0..thread_count {
        let mut buffer = vec![0; chunk_size as usize];
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
        // chunks.push(buffer);
    }

    thread_pool
}

fn read_file_into_chunks(name: &str) -> (Vec<u8>, Vec<(u64, u64)>){
    let file = File::open(name).unwrap();
    let file_size_bytes = file.metadata().unwrap().len();

    println!("File Size: {:?}", file_size_bytes);
    let mut reader = BufReader::new(file);
    let thread_count = std::thread::available_parallelism().unwrap().get();

    let mut chunks = Vec::new();
    let chunk_size = file_size_bytes / thread_count as u64;
    let mut buffer: Vec<u8> = Vec::with_capacity(chunk_size as usize);
    reader.read_to_end(&mut buffer).unwrap();

    println!("Buffer Size: {:?}", buffer.len());
    let mut start_byte = 0;
    let mut i_buffer = buffer.iter().enumerate();
    for _ in 0..thread_count {
        let end_byte = (start_byte + chunk_size).min(file_size_bytes);
        let mut buffer = i_buffer.clone().skip(end_byte as usize);
        if buffer.len() == 0 {
            break;
        }
        let end_byte = buffer.find(|(pos, byte)| {
            **byte == b'\n'
        }).unwrap().0 as u64;
        let end_byte = if end_byte < file_size_bytes {
            end_byte + 1
        } else {
            file_size_bytes
        };
        chunks.push((start_byte, end_byte));
        start_byte = end_byte;
    }

    (buffer, chunks)
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