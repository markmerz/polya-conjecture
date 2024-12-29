
use threadpool::ThreadPool;
use std::thread::available_parallelism;
use std::thread::sleep;
use std::time::Duration;
use std::collections::HashMap;
use thousands::Separable;
use std::sync::{Arc, RwLock};
use num_prime::nt_funcs::factorize64;

const PER_WORKER_BATCH_SIZE: usize = 1_000_000;
const HARD_STOP: usize = 1E9 as usize;

fn main() {
    
    let num_cpus = available_parallelism().unwrap().get();
    let pool = ThreadPool::new(num_cpus);

    let shared_results_map: Arc<RwLock<HashMap<usize, (usize, Vec<(u64, i64)>)>>> = Arc::new(RwLock::new(HashMap::new()));

    let mut batch_counter: usize = 0;
    for batch_start in (1..HARD_STOP).step_by(PER_WORKER_BATCH_SIZE) {
        let batch_end = batch_start + PER_WORKER_BATCH_SIZE;
        let batch_range = batch_start as u64..batch_end as u64;


        let shared_results_map = Arc::clone(&shared_results_map);
        pool.execute(move || {
            let results = worker(batch_counter, batch_range);
            let cloned_shared_results_map = Arc::clone(&shared_results_map);
            cloned_shared_results_map.write().unwrap().insert(batch_counter, results);
        });

        batch_counter += 1;
    }

    let mut sum: i64 = 0;
    let mut expected_batch: usize = 0;
    loop {
        if shared_results_map.read().unwrap().contains_key(&expected_batch) {
            let (_batch_nr, result) = shared_results_map.write().unwrap().remove(&expected_batch).unwrap();
            let mut last_n:u64 = 0;
            for (n, ret) in result {
                sum += ret;
                last_n = n;
                if sum > 0 && n > 2 {
                    println!("Positive! n: {}, sum: {}", last_n.separate_with_underscores(), sum);
                    return;
                }
            }
            println!("n: {}, current sum: {}", last_n.separate_with_underscores(), sum);
            expected_batch += 1;
        } else {
            sleep(Duration::from_millis(10));
        }
    }

}

fn worker(batch_counter: usize, batch_range: std::ops::Range<u64>) -> (usize, Vec<(u64, i64)>) {
    let mut results = Vec::new();
    for n in batch_range {
        results.push((n, liouville(n)));
    }
    (batch_counter, results)
}

fn liouville(n: u64) -> i64 {
    let factor_count:usize = factorize64(n).into_iter().map(|(_, e)| e).sum();
    if factor_count % 2 == 0 {
        1
    } else {
        -1
    }
}
