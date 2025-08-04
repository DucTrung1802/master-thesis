from logger.logger import Logger
from thread_manager.thread_manager import ThreadManager
from models.thread_manager_models.task import Task

import time
import random

logger = Logger(file_name="hello.log")
tm = ThreadManager(logger)

random.seed(42)  # For reproducibility

def task_func(name):
    wait_time = random.randint(1, 3)
    print(f"Task {name} will run after waiting for {wait_time} seconds.")
    time.sleep(random.randint(1, 3))  # Simulate some work
    print(f"Running {name}")
    return name


# Independent tasks
task1 = Task("task_1", task_func, "task_1")
task2 = Task("task_2", task_func, "task_2")
task3 = Task("task_3", task_func, "task_3")

# Dependent task
final_task = Task(
    "final_task", task_func, "final_task", dependencies=["task_1", "task_2", "task_3"]
)

tm.add_task(final_task)
tm.add_task(task1)
tm.add_task(task2)
tm.add_task(task3)

tm.execute()
