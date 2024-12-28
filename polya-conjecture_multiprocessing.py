#!/usr/bin/python3

import multiprocessing
import primefac
import os
import collections
import setproctitle

# this one works best

# per_worker_batch_size = 10_000_000 # 28 min, uses ~70GB of memory
per_worker_batch_size = 1_000_000 # 25 min with few GB of memory
hard_stop = int(1E9)

processes_count = os.cpu_count() # 25 min on intel i9-14000kf
# processes_count = 24 # also 25 min

def main():
    with multiprocessing.get_context("spawn").Pool(processes=processes_count, maxtasksperchild=1) as pool:
        
        sum = 0
        async_batch_results = collections.deque()

        for batch_start in range(1, hard_stop, per_worker_batch_size):
            batch_end = batch_start + per_worker_batch_size
            async_result = pool.apply_async(per_thread_batch_processor, args=(range(batch_start, batch_end),))
            async_batch_results.append(async_result)
        
        try:
            while True:
                for n, lambda_n in async_batch_results.popleft().get():
                    sum += lambda_n
                    if sum > 0 and n > 2:
                        print(f"Positive! n: {n:_}, sum: {sum}")
                        return
                print(f"n: {n:_}, current sum: {sum}")
        except IndexError:
            pass


def per_thread_batch_processor(batch_range: range) -> list:
    setproctitle.setproctitle(f"polya:{int(batch_range.start / per_worker_batch_size):_}")
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
