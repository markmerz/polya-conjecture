#!/usr/bin/python3

import multiprocessing
import primefac
import os

import setproctitle

per_worker_batch_size = 1_000_000
hard_stop = int(1E9)

def main():
    with multiprocessing.Pool(processes=os.cpu_count(), maxtasksperchild=1) as pool:
        
        sum = 0
        async_batch_results = list()

        for batch_start in range(1, hard_stop, per_worker_batch_size):
            batch_end = batch_start + per_worker_batch_size
            async_result = pool.apply_async(per_thread_batch_processor, args=(range(batch_start, batch_end),))
            async_batch_results.append(async_result)
        
        for ret in async_batch_results:
            for n, lambda_n in ret.get():
                sum += lambda_n
                if sum > 0 and n > 2:
                    print(f"Positive! n: {n:_}, sum: {sum}")
                    return
            print(f"n: {n:_}, current sum: {sum}")


def per_thread_batch_processor(batch_range: range) -> list:
    setproctitle.setproctitle(f"polya:{(batch_range.start / per_worker_batch_size):_}")
    ret = list()
    for n in batch_range:
        lambda_n = liouville_function(n)
        ret.append((n, lambda_n))
    return ret

def liouville_function(n):
    if len(list(primefac.primefac(n))) % 2 == 0:
        return 1
    else: 
        return -1
    
if __name__ == '__main__':
    main()
