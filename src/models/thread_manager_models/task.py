from concurrent.futures import ThreadPoolExecutor


class Task:
    def __init__(self, name, func, *args, callbacks=None, **kwargs):
        self.name = name
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.callbacks = callbacks if callbacks else []

    def run(self):
        result = self.func(*self.args, **self.kwargs)

        with ThreadPoolExecutor() as cb_executor:
            futures = [cb_executor.submit(cb, result) for cb in self.callbacks]
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"Callback failed: {e}")

        return result
