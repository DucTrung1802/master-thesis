import inspect
from pathlib import Path


class Helper:

    def get_current_folder(self):
        caller_frame = inspect.stack()[
            2
        ]  # [2] gives the caller's frame (1 is for this method, 0 is for the current method)
        caller_file = caller_frame.filename
        return Path(caller_file).resolve().parent

    def get_parent_folder(self):
        return self.get_current_folder().parent

    def get_grandparent_folder(self):
        return self.get_parent_folder().parent
