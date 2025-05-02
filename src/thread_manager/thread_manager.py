from concurrent.futures import ThreadPoolExecutor
from typing import List
from logger.logger import Logger

from models.thread_manager_models.task import *


class ThreadManager:
    def __init__(self, logger: Logger):
        self._logger = logger
        self._task_list: List[Task] = []

    def _validate_task(self, task: Task) -> bool:
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

        self._task_list.append(task)

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
