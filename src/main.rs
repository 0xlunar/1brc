use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Error, Write};
use std::slice::Chunks;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

fn main() -> Result<(), std::io::Error> {
    let now = Instant::now();
    let lines = read_file_into_lines("./measurements.txt");
    let chunk_size = get_chunk_size(lines.len());
    let chunks = lines.chunks(chunk_size);
    println!("Total Lines: {}, Chunk Size: {}, Total Chunks: {}, Reading File Time: {}ms", lines.len(), chunk_size, chunks.len(), now.elapsed().as_millis());


    let now = Instant::now();
    let workloads = chunks.map(|chunk| {
        let chunk = chunk.to_vec();
        std::thread::spawn(move || {
            let parsed = parse_chunk(chunk);
            let output = process_dataset(parsed);
            output
        })
    }).collect::<Vec<JoinHandle<_>>>();

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

fn read_file_into_lines(name: &str) -> Vec<String>{
    let file = File::open(name).unwrap();
    let reader = BufReader::new(file);

    let lines = reader.lines().filter_map(|r| match r {
        Ok(r) => Some(r),
        Err(_) => None,
    }).collect::<Vec<String>>();

    lines
}

fn read_file_into_chunks(name: &str) {
    let file = File::open(name).unwrap();
    let file_size_bytes = file.metadata().unwrap().len();
    let reader = BufReader::new(file);
    let thread_count = std::thread::available_parallelism().unwrap().get();

    let mut chunks = Vec::new();
    let chunk_size = file_size_bytes / thread_count as u64;
    let mut start_byte = 0;
    let mut buffer = reader.buffer().iter().enumerate();
    for i in 0..thread_count {
        let end_byte = (start_byte + chunk_size).min(file_size_bytes);
        let end_byte = buffer.find(|(pos, byte)| *pos as u64 <= end_byte && **byte == b'\n').unwrap().0 as u64;
        let end_byte = if end_byte != -1 {
            end_byte + 1
        } else {
            file_size_bytes
        };
    }
}

fn get_chunk_size(lines: usize) -> usize {
    let thread_count = std::thread::available_parallelism().unwrap().get();
    lines / thread_count
}

fn parse_chunk(chunk: Vec<String>) -> Vec<(String, i16)> {
    let mut output: Vec<(String, i16)> = Vec::new();
    for data in chunk {
        let (station, mut temperature) = data.split_once(";").unwrap();
        let mut sign: i16 = 1;
        let mut value: i16 = 0;

        let mut chars = temperature.chars();
        while let Some(char) = chars.next() {
            if char.is_digit(10) {
                value = value * 10 + (char.to_digit(10).unwrap() as i16);
            } else if char == '-' {
                sign = -1;
            }
        }
        output.push((station.to_string(), value));
    }

    output
}

fn process_dataset(dataset: Vec<(String, i16)>) -> HashMap<String, (i16, i16)> {
    let mut output: HashMap<String, (i16, i16)> = HashMap::new();

    for (station, temperature) in dataset {
        output.entry(station).and_modify(|(min, max)| {
            if temperature < *min {
                *min = temperature;
            } else if temperature > *max {
                *max = temperature;
            }
        }).or_insert((temperature, temperature));
    }

    output
}

