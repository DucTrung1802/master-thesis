from datetime import datetime
import random
import string


class Helper:

    @staticmethod
    def get_today():
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def get_current_timestamp():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def generate_lower_string(length: int = 10):
        if not length or length <= 0:
            length = 10

        return "".join(random.choices(string.ascii_lowercase, k=length))
