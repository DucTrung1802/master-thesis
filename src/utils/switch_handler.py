import json
import os

from logger.logger import Logger


class SwitchHandler:
    def __init__(self, logger: Logger, config_path: str = "src/switch_config.json"):
        self._logger: Logger = logger
        self._config_path = config_path
        self._config = self._load_config()

    def _load_config(self):
        if not os.path.exists(self._config_path):
            self._logger.log_error(f"Config file not found: {self._config_path}")
            return {}

        try:
            with open(self._config_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                self._logger.log_info("Switch config loaded successfully")
                return data
        except Exception as e:
            self._logger.log_error(f"Failed to load config: {e}")
            return {}

    def is_enabled(self, *keys) -> bool:
        if not keys:
            return False

        node = self._config

        try:
            # First key (root)
            node = node[keys[0]]

            # 🔴 check root first
            if not node.get("enable", False):
                self._logger.log_info(
                    f"Switch {'/'.join(keys)} = False (parent disabled)"
                )
                return False

            # Traverse children
            for key in keys[1:]:
                node = node.get("children", {})
                node = node[key]

                # 🔴 check each level
                if not node.get("enable", False):
                    self._logger.log_info(
                        f"Switch {'/'.join(keys)} = False (parent disabled)"
                    )
                    return False

            result = node.get("enable", False)
            self._logger.log_info(f"Switch {'/'.join(keys)} = {result}")
            return result

        except Exception:
            self._logger.log_warning(f"Switch path not found: {'/'.join(keys)}")
            return False
