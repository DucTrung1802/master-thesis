import inspect
from .models import *


class Helper:

    def get_context(self):
        class_name = self.__class__.__name__
        method_name = (
            inspect.currentframe().f_back.f_code.co_name
        )  # f_back gets the caller's frame
        return Context(className=class_name, methodName=method_name)
