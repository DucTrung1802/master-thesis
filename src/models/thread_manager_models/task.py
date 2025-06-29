from concurrent.futures import ThreadPoolExecutor


class Task:
    def __init__(self, name, func, *args, callbacks=None, **kwargs):
        self.name = name
        self.func = func
        self.args = args
        self.kwargs = kwargs
        if callbacks and not isinstance(callbacks, list):
            callbacks = [callbacks]
        self.callbacks = callbacks if callbacks else []

    def run(self):
        print(f"Executing task: {self.name}")
        result = self.func(*self.args, **self.kwargs)

        # If result is a list and matches callbacks count, split it
        if isinstance(result, list) and len(result) == len(self.callbacks):
            with ThreadPoolExecutor() as cb_executor:
                futures = [
                    cb_executor.submit(cb, item)
                    for cb, item in zip(self.callbacks, result)
                ]
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Callback failed: {e}")
        else:
            # Fallback if result is not a list of same length
            with ThreadPoolExecutor() as cb_executor:
                futures = [cb_executor.submit(cb, result) for cb in self.callbacks]
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Callback failed: {e}")

        return result
