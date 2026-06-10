# src\utils\switch_handler.py

import json
import os
from typing import List

from logger.logger import Logger

_SEP = "/"


class SwitchHandler:
    """
    Feature-flag handler backed by a flat JSON config.

    Config format
    ─────────────
    Each key is a slash-separated path; each value is a boolean.

        {
            "web_scraper/trading_view/links":                                          true,
            "web_scraper/trading_view/links/stocks/vietnam/common_stock/finance":      true,
            "web_scraper/data_preprocessor/data_quality_gold/enterprise":              true
        }

    Rules
    ─────
    • A path is enabled only when EVERY prefix in the chain is explicitly true.
      Example: "a/b/c" requires "a", "a/b", and "a/b/c" to all be true.
      → Disabling a parent ("a/b": false) automatically disables all its
        descendants without needing to touch them individually.

    • A missing key is treated as false — you never need to declare disabled paths.

    • Calling is_enabled("a", "b", "c") is identical to the old API — callers
      need no changes.
    """

    def __init__(self, logger: Logger, config_path: str = "src/switch_config.json"):
        self._logger = logger
        self._config_path = config_path
        self._switches: dict[str, bool] = self._load_config()

    # ──────────────────────────────────────────────────────────────────────────
    # Loading
    # ──────────────────────────────────────────────────────────────────────────

    def _load_config(self) -> dict[str, bool]:
        if not os.path.exists(self._config_path):
            self._logger.log_error(f"Config file not found: {self._config_path}")
            return {}

        try:
            with open(self._config_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if not isinstance(data, dict):
                self._logger.log_error(
                    f"switch_config.json must be a flat JSON object, got {type(data).__name__}"
                )
                return {}

            # Keys starting with "//" are treated as inline comments and ignored.
            bad = {
                k: v
                for k, v in data.items()
                if not isinstance(v, bool) and not k.startswith("//")
            }
            if bad:
                self._logger.log_error(
                    f"Non-boolean values in switch config: {list(bad.keys())}"
                )

            switches = {k: bool(v) for k, v in data.items() if isinstance(v, bool)}
            self._logger.log_info(
                f"Switch config loaded: {len(switches)} switches "
                f"({sum(switches.values())} enabled)"
            )
            return switches

        except json.JSONDecodeError as e:
            self._logger.log_error(f"switch_config.json is not valid JSON: {e}")
            return {}
        except Exception as e:
            self._logger.log_error(f"Failed to load switch config: {e}")
            return {}

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    def is_enabled(self, *keys: str) -> bool:
        """
        Return True only when every prefix of the given key path is
        explicitly set to true in the config.

        Usage (identical to the old API):
            switch_handler.is_enabled("web_scraper", "trading_view", "links")
        """
        if not keys:
            return False

        path_parts: list[str] = []
        for key in keys:
            path_parts.append(key)
            prefix = _SEP.join(path_parts)

            if not self._switches.get(prefix, False):
                self._logger.log_info(f"Switch {prefix!r} = False")
                return False

        full_path = _SEP.join(keys)
        self._logger.log_info(f"Switch {full_path!r} = True")
        return True

    def get_enabled_paths(self, *prefix_keys: str) -> List[str]:
        """
        Return all enabled full paths that start with the given prefix
        AND have every ancestor also enabled.

        Useful for iterating tasks without calling is_enabled in a tight loop:

            for path in switch_handler.get_enabled_paths("web_scraper", "trading_view", "links", "stocks"):
                # path e.g. "web_scraper/trading_view/links/stocks/vietnam/common_stock/finance"
                parts = path.split("/")
                country, stock_type, sector = parts[4], parts[5], parts[6]
                ...
        """
        prefix = _SEP.join(prefix_keys)

        # Collect all enabled paths that start with (or equal) the prefix
        candidates = [
            path
            for path, enabled in self._switches.items()
            if enabled and (path == prefix or path.startswith(prefix + _SEP))
        ]

        # Keep only leaf paths: those that have no deeper key in the config
        all_paths = set(self._switches.keys())
        leaves = [
            path
            for path in candidates
            if not any(
                other != path and other.startswith(path + _SEP) for other in all_paths
            )
        ]

        # Filter to only those whose every ancestor prefix is also true
        result = []
        for path in sorted(leaves):
            if self._all_ancestors_enabled(path):
                result.append(path)

        return result

    def reload(self) -> None:
        """Re-read the config file from disk (useful for long-running processes)."""
        self._switches = self._load_config()

    # ──────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _all_ancestors_enabled(self, full_path: str) -> bool:
        """Check that every prefix of full_path is true in the config."""
        parts = full_path.split(_SEP)
        for i in range(1, len(parts) + 1):
            prefix = _SEP.join(parts[:i])
            if not self._switches.get(prefix, False):
                return False
        return True
