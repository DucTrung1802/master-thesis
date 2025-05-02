import time

from logger.logger import Logger, LogType
from utils.constants import LOG_FILE_BASE
from models.thread_manager_models.task import Task
from thread_manager.thread_manager import ThreadManager


def task_download(url):
    time.sleep(2)
    return f"Downloaded from {url}"


def task_log_message(message, repeat=1):
    for _ in range(repeat):
        time.sleep(1)
    return f"Logged message: {message}"


def task_simulate_file_read(filename):
    time.sleep(1.5)
    return f"Contents of {filename}"


def task_wait_and_return(wait_time):
    time.sleep(wait_time)
    return f"Waited {wait_time} seconds"


def main():
    my_logger = Logger(file_name=LOG_FILE_BASE)
    my_thread_manager = ThreadManager(logger=my_logger)

    # Add tasks
    my_thread_manager.add_task(
        Task("DownloadTask", task_download, "http://example.com")
    )
    my_thread_manager.add_task(Task("LogTask", task_log_message, "hello"))
    my_thread_manager.add_task(
        Task("ReadFileTask", task_simulate_file_read, "dummy.txt")
    )
    my_thread_manager.add_task(Task("WaitTask", task_wait_and_return, 3))

    # Execute all tasks in thread pool
    my_thread_manager.execute()


if __name__ == "__main__":
    main()
