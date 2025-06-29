import os
from concurrent.futures import ThreadPoolExecutor, wait
from typing import List, Set
from logger.logger import Logger

from utils.constants import THREAD_MANAGER_POWER
from models.thread_manager_models.task import *


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
        successful_tasks = []
        failed_tasks = []

        total_round = 0
        while self._task_list:
            total_round += 1
            current_batch = self._task_list[:]
            self._task_list.clear()
            self._logger.log_info(
                f"Executing batch #{total_round} with {len(current_batch)} tasks..."
            )

            with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
                future_to_task = {
                    executor.submit(task.run): task for task in current_batch
                }

                futures = list(future_to_task.keys())
                wait(futures)

                for future in future_to_task:
                    task = future_to_task[future]
                    try:
                        result = future.result()
                        successful_tasks.append((task.name, result))
                        self._logger.log_info(
                            f"Task '{task.name}' completed successfully."
                        )
                    except Exception as e:
                        failed_tasks.append((task.name, str(e)))
                        self._logger.log_error(
                            f"Task '{task.name}' failed with exception: {e}"
                        )

                if final_callback:
                    try:
                        final_callback()
                        self._logger.log_info(
                            "Final callback executed after all tasks."
                        )
                    except Exception as e:
                        self._logger.log_error(f"Final callback failed: {e}")

            # This log shows progress across rounds
            self._logger.log_info(f"Finished executing batch #{total_round}.")

        self._logger.log_info(
            f"All tasks have been executed across {total_round} batches."
        )

        successful_names = [name for name, _ in successful_tasks]
        failed_names = [name for name, _ in failed_tasks]

        self._logger.log_info(
            f"Successful Tasks ({len(successful_names)} total): {successful_names}"
        )
        self._logger.log_info(
            f"Failed Tasks ({len(failed_names)} total): {failed_names}"
        )

        return successful_tasks, failed_tasks
