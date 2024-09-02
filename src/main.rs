use crossbeam_utils::CachePadded;
use std::{
    collections::HashMap, sync::{Arc, Mutex, RwLock}, time::Instant
};
use threadpool::ThreadPool;

const MODE_PADDED: &str = "padded";
const MODE_RAW: &str = "raw";
const MODE_PARTITIONED: &str = "partitioned";

fn do_with_buffer(buffer: &mut HashMap<u64, ()>, data_size: usize, task_idx: usize) {
    for i in 0..data_size {
        let inserted = ((task_idx as u64) << 32) | i as u64;
        buffer.insert(inserted, ());
    }
}

struct PaddedPartitionedBuffer {
    buffers: Vec<CachePadded<Mutex<HashMap<u64, ()>>>>,
}

fn insert_into_padded_partitioned_buffer(
    num_threads: usize,
    num_tasks: usize,
    data_size: usize,
) -> usize {
    println!("running in padded mode");
    let thread_pool = ThreadPool::new(num_threads);
    let rate = 4;
    let buffer = {
        let num_partitions = num_threads * rate;
        let mut buffers = Vec::with_capacity(num_partitions);
        let partition_buffer_cap = (data_size * num_tasks) / num_partitions;
        for _ in 0..num_partitions {
            let buffer = HashMap::with_capacity(partition_buffer_cap);
            buffers.push(CachePadded::new(Mutex::new(buffer)));
        }

        Arc::new(PaddedPartitionedBuffer { buffers })
    };

    for i in 0..num_tasks {
        let buffer = buffer.clone();
        thread_pool.execute(move || {
            let idx = i % buffer.buffers.len();
            let mut buffer = buffer.buffers[idx].lock().unwrap();
            do_with_buffer(&mut buffer, data_size, i);
        });
    }

    thread_pool.join();

    buffer.buffers.iter().map(|v| v.lock().unwrap().len()).sum()
}

struct PartitionedBuffer {
    buffers: Vec<Mutex<HashMap<u64, ()>>>,
}

fn insert_into_partitioned_buffer(num_threads: usize, num_tasks: usize, data_size: usize) -> usize {
    println!("running in partitioned mode");
    let thread_pool = ThreadPool::new(num_threads);
    let rate = 4;
    let buffer = {
        let num_partitions = num_threads * rate;
        let mut buffers = Vec::with_capacity(num_partitions);
        let partition_buffer_cap = (data_size * num_tasks) / num_partitions;
        for _ in 0..num_partitions {
            let buffer = HashMap::with_capacity(partition_buffer_cap);
            buffers.push(Mutex::new(buffer));
        }

        Arc::new(PartitionedBuffer { buffers })
    };

    for i in 0..num_tasks {
        let buffer = buffer.clone();
        thread_pool.execute(move || {
            let idx = i % buffer.buffers.len();
            let mut buffer = buffer.buffers[idx].lock().unwrap();
            do_with_buffer(&mut buffer, data_size, i);
        });
    }

    thread_pool.join();
    buffer.buffers.iter().map(|v| v.lock().unwrap().len()).sum()
}

struct Buffer {
    buffer: Mutex<HashMap<u64, ()>>,
}

fn insert_into_raw_buffer(num_threads: usize, num_tasks: usize, data_size: usize) -> usize {
    println!("running in raw mode");
    let thread_pool = ThreadPool::new(num_threads);
    let buffer = Arc::new(Buffer {
        buffer: Mutex::new(HashMap::with_capacity(data_size * num_tasks)),
    });
    for i in 0..num_tasks {
        let buffer = buffer.clone();
        thread_pool.execute(move || {
            let mut buffer = buffer.buffer.lock().unwrap();
            do_with_buffer(&mut buffer, data_size, i);
        });
    }

    thread_pool.join();
    let n = buffer.buffer.lock().unwrap().len();
    n
}

fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).cloned().unwrap_or(MODE_RAW.to_string());
    let instant = Instant::now();
    let num_threads = 4;
    let num_tasks = 1024 * 1024;
    let data_size: usize = 64;
    let num_inserted_bytes = match mode.as_str() {
        MODE_PADDED => insert_into_padded_partitioned_buffer(num_threads, num_tasks, data_size),
        MODE_PARTITIONED => insert_into_partitioned_buffer(num_threads, num_tasks, data_size),
        _ => insert_into_raw_buffer(num_threads, num_tasks, data_size),
    };

    println!(
        "{mode} cost:{:?}, inserted_bytes:{num_inserted_bytes}",
        instant.elapsed()
    );
    Ok(())
}