import os
from concurrent.futures import ThreadPoolExecutor, wait
from typing import List, Set
from logger.logger import Logger

from utils.constants import THREAD_MANAGER_POWER
from dtos.thread_manager_dtos.task import *


class ThreadManager:
    def __init__(self, logger: Logger, power: int = THREAD_MANAGER_POWER):
        self._logger = logger

        try:
            power = int(power)
            if 0 < power <= 100:
                self._power = power
                self._logger.log_info(
                    f"Power of ThreadManager is set at {self._power} %."
                )
            else:
                self._logger.log_warning(
                    f'Power of ThreadManager is invalid: "{power}". Power is set as default: 50%.'
                )
        except:
            self._logger.log_warning(
                f'Power of ThreadManager is invalid: "{power}". Power is set as default: 50%.'
            )

        self._max_workers = int(os.process_cpu_count() * self._power / 100) * 0.4
        self._logger.log_info(f"Total logical processors: {os.process_cpu_count()}.")
        self._logger.log_info(f"The number of max workers is {self._max_workers}.")

        self._task_name_set: Set[str] = set()
        self._task_list: List[Task] = list()

    def _validate_task(self, task: Task) -> bool:
        if not task.name:
            self._logger.log_error(f"The name provided for task must not be empty.")
            raise ValueError(f"The name provided for task must not be empty.")

        task_name = str.lower(task.name)
        if task_name in self._task_name_set:
            self._logger.log_error(
                f'The name "{task_name}" (ignore_case) must be unique for each function.'
            )
            raise ValueError(
                f'The name "{task_name}" (ignore_case) must be unique for each function.'
            )

        if not callable(task.func):
            self._logger.log_error(
                f"The function provided for task '{task.name}' is not callable."
            )
            raise ValueError(
                f"The function provided for task '{task.name}' is not callable."
            )
        try:
            from inspect import signature

            sig = signature(task.func)
            sig.bind(*task.args, **task.kwargs)
        except TypeError as e:
            self._logger.log_error(f"Invalid arguments for task '{task.name}': {e}")
            raise ValueError(f"Invalid arguments for task '{task.name}': {e}")

        self._logger.log_info(
            f'Task "{task.name}" with function "{task.func.__name__}" and arguments {task.args} and keyword arguments {task.kwargs} is valid.'
        )
        return True

    def add_task(self, task: Task):
        if not self._validate_task(task):
            return

        self._task_name_set.add(task.name)
        self._task_list.append(task)

    def remove_task(self, task_name: str):
        if task_name in self._task_name_set:
            try:
                self._task_name_set.remove()
                self._task_list = [
                    task for task in self._task_list if task.name != task_name
                ]
                self._logger.log_info(
                    f'Removed task with name "{task_name}" from task list.'
                )
            except KeyError as e:
                self._logger.log_warning(
                    f'Task with name "{task_name}" does not exist in the task list.'
                )
                return

    def remove_all_tasks(self):
        self._task_name_set = set()
        self._task_list = list()

    def get_current_number_of_task(self):
        return len(self._task_name_set)

    def generate_callbacks(self, func: callable, num: int):
        def make_callback(i):
            def callback(sublist):
                return func(sublist, i)

            return callback

        return [make_callback(i) for i in range(num)]

    def execute(self, final_callback: callable = None):
        successful_tasks = {}
        failed_tasks = {}

        total_round = 0
        while self._task_list:
            total_round += 1
            ready_tasks = []

            # Only run tasks whose dependencies are all completed
            for task in self._task_list:
                if all(dep in successful_tasks for dep in task.dependencies):
                    ready_tasks.append(task)

            # If no task is ready but tasks remain, it means dependencies failed or circular dependency
            if not ready_tasks:
                self._logger.log_error("No ready tasks found. Possible dependency issue.")
                break

            # Remove ready tasks from task list
            for task in ready_tasks:
                self._task_list.remove(task)

            with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
                future_to_task = {executor.submit(task.run): task for task in ready_tasks}

                futures = list(future_to_task.keys())
                wait(futures)

                for future in future_to_task:
                    task = future_to_task[future]
                    try:
                        result = future.result()
                        successful_tasks[task.name] = result
                        self._logger.log_info(f"Task '{task.name}' completed successfully.")
                    except Exception as e:
                        failed_tasks[task.name] = str(e)
                        self._logger.log_error(f"Task '{task.name}' failed: {e}")

        # Final callback runs once after all tasks
        if final_callback:
            try:
                final_callback()
                self._logger.log_info("Final callback executed after all tasks.")
            except Exception as e:
                self._logger.log_error(f"Final callback failed: {e}")

        self._logger.log_info(
            f"Successful Tasks ({len(successful_tasks)}): {list(successful_tasks.keys())}"
        )
        self._logger.log_info(
            f"Failed Tasks ({len(failed_tasks)}): {list(failed_tasks.keys())}"
        )

        return list(successful_tasks.items()), list(failed_tasks.items())
