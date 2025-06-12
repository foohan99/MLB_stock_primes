import time
import math
import json
import os
from multiprocessing import Pool, cpu_count

LOG_FILE = "primes5m.log"
COUNT_FILE = "primes5m_count.json"

def is_prime(n):
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    sqrt_n = int(math.sqrt(n)) + 1
    for i in range(3, sqrt_n, 2):
        if n % i == 0:
            return False
    return True

def count_primes_in_range(args):
    start, end = args
    count = 0
    for n in range(start, end):
        if is_prime(n):
            count += 1
    return count

def load_run_count():
    if os.path.exists(COUNT_FILE):
        with open(COUNT_FILE, "r") as f:
            data = json.load(f)
            return data.get("total_runs", 0)
    return 0

def save_run_count(total_runs):
    with open(COUNT_FILE, "w") as f:
        json.dump({"total_runs": total_runs}, f)

def log_run(total_runs, elapsed, total_primes):
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"[{timestamp}] Run #{total_runs}: {total_primes} primes found in {elapsed:.2f} seconds\n")

if __name__ == "__main__":
    RANGE_START = 1
    RANGE_END = 50_000_000
    num_workers = cpu_count()
    print(f"Using {num_workers} CPU cores.")

    # Split the range into chunks for each worker
    chunk_size = (RANGE_END - RANGE_START) // num_workers
    ranges = []
    for i in range(num_workers):
        chunk_start = RANGE_START + i * chunk_size
        chunk_end = chunk_start + chunk_size
        if i == num_workers - 1:
            chunk_end = RANGE_END
        ranges.append((chunk_start, chunk_end))

    total_runs = load_run_count() + 1

    start_time = time.time()
    with Pool(processes=num_workers) as pool:
        results = pool.map(count_primes_in_range, ranges)
    total_primes = sum(results)
    elapsed = time.time() - start_time

    print(f"Total primes found: {total_primes}")
    print(f"Elapsed time: {elapsed:.2f} seconds")
    print(f"Total runs: {total_runs}")

    save_run_count(total_runs)
    log_run(total_runs, elapsed, total_primes)
