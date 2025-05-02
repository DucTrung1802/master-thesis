from concurrent.futures import ThreadPoolExecutor
from typing import List, Set
from logger.logger import Logger

from models.thread_manager_models.task import *


class ThreadManager:
    def __init__(self, logger: Logger):
        self._logger = logger
        self._task_name_set: Set[str] = Set()
        self._task_list: List[Task] = []

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
        self._task_name_set = Set()
        self._task_list = List()

    def get_current_number_of_task(self):
        return len(self._task_name_set)

    def execute(self):
        if not self._task_list:
            self._logger.log_info("No tasks to execute.")
            return

        with ThreadPoolExecutor() as executor:
            self._logger.log_info(f"Executing {len(self._task_list)} tasks...")

            future_to_task = {
                executor.submit(task.run): task for task in self._task_list
            }

            for future in future_to_task:
                task = future_to_task[future]
                try:
                    result = future.result()
                    self._logger.log_info(
                        f"Task '{task.name}' completed with result: {result}"
                    )
                except Exception as e:
                    self._logger.log_error(
                        f"Task '{task.name}' failed with exception: {e}"
                    )

            # Log after all tasks are done
            self._logger.log_info("All tasks have been completed.")
