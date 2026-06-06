# src\dtos\thread_manager_dtos\task.py

from concurrent.futures import ThreadPoolExecutor


class Task:
    def __init__(self, name, func, *args, callbacks=None, dependencies=None, **kwargs):
        self.name = name
        self.func = func
        self.args = args
        self.kwargs = kwargs
        if callbacks and not isinstance(callbacks, list):
            callbacks = [callbacks]
        self.callbacks = callbacks if callbacks else []

        # New: dependencies (list of task names)
        self.dependencies = dependencies if dependencies else []

    def run(self):
        print(f"Executing task: {self.name}")
        result = self.func(*self.args, **self.kwargs)

        def submit_callback(cb_executor, cb, value, extra_args, extra_kwargs):
            return cb_executor.submit(cb, value, *extra_args, **extra_kwargs)

        with ThreadPoolExecutor() as cb_executor:
            futures = []

            if isinstance(result, list) and len(result) == len(self.callbacks):
                for (cb, extra_args, extra_kwargs), item in zip(self.callbacks, result):
                    futures.append(
                        submit_callback(cb_executor, cb, item, extra_args, extra_kwargs)
                    )
            else:
                for cb, extra_args, extra_kwargs in self.callbacks:
                    futures.append(
                        submit_callback(
                            cb_executor, cb, result, extra_args, extra_kwargs
                        )
                    )

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"Callback failed: {e}")

        return result


# ---------------- TESTING ----------------
if __name__ == "__main__":

    # Example task function
    def sample_task(x, y):
        print(f"Running sample_task with {x}, {y}")
        return x + y

    # ✅ Fixed callbacks
    def cb1(result, a, b, flag=False):
        print(f"Callback 1 got: {result}, a={a}, b={b}, flag={flag}")

    def cb2(result, x, msg=None):
        print(f"Callback 2 got: {result}, x={x}, msg={msg}")

    # Test 1
    task1 = Task(
        "Task1",
        sample_task,
        3,
        4,
        callbacks=[
            (cb1, (5, 6,), {"flag": True}),
            (cb2, (99,), {"msg": "hello"}),
        ],
    )

    print("---- Running Task1 ----")
    task1.run()

    # Test 2
    def list_task():
        return [10, {"Duc": "Trung"}]

    task2 = Task(
        name="Task2",
        func=list_task,
        callbacks=[
            (cb1, (1, 2), {"flag": False}),
            (cb2, (42,), {"msg": "world"}),
        ],
    )

    print("\n---- Running Task2 ----")
    task2.run()
