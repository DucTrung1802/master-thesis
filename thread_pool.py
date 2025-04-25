from concurrent.futures import ThreadPoolExecutor, as_completed
import time


class Task:
    def __init__(self, name, func, *args, **kwargs):
        self.name = name
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        return self.func(*self.args, **self.kwargs)


def slow_multiply(a, b, delay=1):
    time.sleep(delay)
    return a * b


# Create a list of Task objects
tasks = [Task(f"Task-{i}", slow_multiply, i, i + 1, delay=2) for i in range(5)]

# Use ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=2) as executor:
    future_to_task = {}  # Map future -> Task

    for task in tasks:
        future = executor.submit(task.run)
        future_to_task[future] = task

    last_future = list(future_to_task.keys())[-1]
    if last_future.cancel():
        print(f"{future_to_task[last_future].name} was cancelled before execution.")
    else:
        print(f"{future_to_task[last_future].name} could not be cancelled.")

    for future in as_completed(future_to_task):
        task = future_to_task[future]
        if future.cancelled():
            print(f"{task.name} was cancelled.")
        elif future.done():
            print(f"{task.name} is done. Running: {future.running()}")
            print(f"{task.name} result: {future.result()}")
