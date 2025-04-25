class Task:
    def __init__(self, name, func, *args, **kwargs):
        self.name = name
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def validate(self):
        if not callable(self.func):
            raise ValueError(
                f"The function provided for task '{self.name}' is not callable."
            )
        try:
            from inspect import signature

            sig = signature(self.func)
            sig.bind(*self.args, **self.kwargs)
        except TypeError as e:
            raise ValueError(f"Invalid arguments for task '{self.name}': {e}")
        return True

    def run(self):
        self.validate()
        return self.func(*self.args, **self.kwargs)
