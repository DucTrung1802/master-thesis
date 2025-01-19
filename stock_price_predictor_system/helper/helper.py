from datetime import datetime


class Helper:

    @staticmethod
    def get_today():
        return datetime.now().strftime("%Y-%m-%d")
