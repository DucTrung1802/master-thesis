from concurrent.futures import ThreadPoolExecutor
import random
import time


def process_data(data):
    time.sleep(random.uniform(0.1, 0.5))
    return sum(data)


data_chunks = [list(range(1000)) for _ in range(10)]

# 1 WORKER
print("1 WORKER")
start_time = time.time()

# Create a ThreadPoolExecutor with 1 worker
with ThreadPoolExecutor(max_workers=1) as executor:
    results = list(executor.map(process_data, data_chunks))

end_time = time.time()

print(results)
print(f"Execution time if : {end_time - start_time:.2f} seconds")


# 4 WORKERS
print("4 WORKERS")
start_time = time.time()

# Create a ThreadPoolExecutor with 4 worker
with ThreadPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(process_data, data_chunks))

end_time = time.time()

print(results)
print(f"Execution time if : {end_time - start_time:.2f} seconds")


# 10 WORKERS
print("10 WORKERS")
start_time = time.time()

# Create a ThreadPoolExecutor with 10 worker
with ThreadPoolExecutor(max_workers=10) as executor:
    results = list(executor.map(process_data, data_chunks))

end_time = time.time()

print(results)
print(f"Execution time if : {end_time - start_time:.2f} seconds")
