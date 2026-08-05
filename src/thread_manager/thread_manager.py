# src\thread_manager\thread_manager.py

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Set
from logger.logger import Logger

from utils.constants import THREAD_MANAGER_POWER
from dtos.thread_manager_dtos.task import *


class ThreadManager:
    def __init__(
        self,
        logger: Logger,
        power: int = THREAD_MANAGER_POWER,
        max_workers: int = None,
    ):
        self._logger = logger
        self._power = 50  # default if power validation below falls through

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

        # An explicit max_workers pins the pool to exactly that many threads
        # (used for I/O-bound work like web scraping, where the count is not tied
        # to CPU cores). Otherwise fall back to the CPU-proportional power formula.
        if max_workers is not None:
            self._max_workers = max(1, int(max_workers))
        else:
            self._max_workers = max(1, int(os.cpu_count() * self._power / 100 * 0.4))
        self._logger.log_info(f"Total logical processors: {os.cpu_count()}.")
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

    @staticmethod
    def _format_duration(seconds: float) -> str:
        """`5h22m` / `47m10s` / `38s` — whichever unit pair a reader actually needs."""
        seconds = int(max(0, seconds))
        h, rem = divmod(seconds, 3600)
        m, s = divmod(rem, 60)
        if h:
            return f"{h}h{m:02d}m"
        if m:
            return f"{m}m{s:02d}s"
        return f"{s}s"

    def _progress_line(
        self,
        done: int,
        total: int,
        failed: int,
        started_at: float,
        last_task: str,
    ) -> str:
        """One line per finished task: how far, how fast, how much longer.

        A scrape runs for hours inside one Dagster step, and Dagster reports nothing
        until the step ends — so `logs/app.log` is the only place progress can show up.
        Both forms are here on purpose: the PERCENTAGE answers "are we nearly there",
        the COUNTS answer "how many series did we actually get", and the two disagree in
        the way that matters when tasks fail.
        """
        elapsed = time.monotonic() - started_at
        pct = done / total * 100 if total else 100.0
        rate = done / elapsed * 60 if elapsed > 0 else 0.0  # tasks per minute
        eta = (total - done) / (done / elapsed) if done and done < total else 0

        filled = int(round(pct / 5))  # 20-cell bar
        bar = "#" * filled + "-" * (20 - filled)

        return (
            f"Progress [{bar}] {pct:5.1f}%  {done}/{total}"
            f" | ok {done - failed} fail {failed}"
            f" | {rate:.1f}/min"
            f" | elapsed {self._format_duration(elapsed)}"
            f" | ETA {self._format_duration(eta)}"
            f" | last: {last_task}"
        )

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
                self._logger.log_error(
                    "No ready tasks found. Possible dependency issue."
                )
                break

            # Remove ready tasks from task list
            for task in ready_tasks:
                self._task_list.remove(task)

            # ⚠️ PROGRESS IS LOGGED AS TASKS LAND, NOT COLLECTED AT THE END. A scrape is
            # thousands of tasks over hours inside ONE Dagster step, and Dagster reports
            # nothing until that step finishes — so without this the only signal that a
            # 7-hour run is alive was the file count in `raw_data/`.
            #
            # `as_completed` replaced `wait(futures, timeout=120)` + a second pass. That
            # timeout never did anything: the subsequent `future.result()` blocks with no
            # timeout of its own, and the `with` block joins the pool anyway, so a run
            # longer than 120 s was not cut short — it just stopped reporting.
            round_total = len(ready_tasks)
            round_started = time.monotonic()
            round_done = 0
            round_failed = 0

            with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
                future_to_task = {
                    executor.submit(task.run): task for task in ready_tasks
                }

                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        successful_tasks[task.name] = future.result()
                    except Exception as e:
                        failed_tasks[task.name] = str(e)
                        round_failed += 1
                        self._logger.log_error(f"Task '{task.name}' failed: {e}")

                    round_done += 1
                    self._logger.log_info(
                        self._progress_line(
                            round_done,
                            round_total,
                            round_failed,
                            round_started,
                            task.name,
                        )
                    )

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
