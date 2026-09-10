import os
import math
import warnings
from pathlib import Path
from typing import Any, Dict, Optional

class LogConfig:
    """Configuration for the logging system."""
    _VALID_FORMATS = {"color", "plain", "json", "csv", "logfmt"}
    _VALID_LOG_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"}

    def __init__(self):
        env_base_format = os.environ.get("EZTRACE_LOG_FORMAT")
        env_console_format = os.environ.get("EZTRACE_CONSOLE_LOG_FORMAT")
        env_file_format = os.environ.get("EZTRACE_FILE_LOG_FORMAT")

        env_base_format = self._validated_env_format("LOG_FORMAT", env_base_format, None)
        env_console_format = self._validated_env_format("CONSOLE_LOG_FORMAT", env_console_format, None)
        env_file_format = self._validated_env_format("FILE_LOG_FORMAT", env_file_format, None)

        self._explicit: Dict[str, bool] = {
            "format": env_base_format is not None,
            "console_format": env_console_format is not None,
            "file_format": env_file_format is not None,
        }

        self._config: Dict[str, Any] = {
            # Legacy "set both" format. When unset, per-sink defaults apply.
            'format': env_base_format,
            'console_format': env_console_format or 'color',
            'file_format': env_file_format or 'json',
            'log_file': self._get_env('LOG_FILE', 'app.log'),
            'max_size': self._get_env_int('MAX_SIZE', 10 * 1024 * 1024, minimum=0),
            'backup_count': self._get_env_int('BACKUP_COUNT', 5, minimum=0),
            'log_dir': self._get_env('LOG_DIR', 'logs'),
            'log_level': self._get_env_log_level('LOG_LEVEL', 'DEBUG'),
            'buffer_enabled': self._get_env_bool('BUFFER_ENABLED', False),
            'buffer_flush_interval': self._get_env_float('BUFFER_FLUSH_INTERVAL', 1.0, minimum=0.001),
            'disable_file_logging': self._get_env('DISABLE_FILE_LOGGING', '1').lower() in {'1', 'true', 'yes', 'on'},
            'disable_resource_metrics': self._get_env_bool('DISABLE_RESOURCE_METRICS', False),
        }

    def _get_env(self, key: str, default: str) -> str:
        """Get environment variable with EZTRACE_ prefix."""
        return os.environ.get(f'EZTRACE_{key}', default)

    def _get_env_bool(self, key: str, default: bool) -> bool:
        value = os.environ.get(f'EZTRACE_{key}')
        if value is None:
            return default
        return value.lower() in {"1", "true", "yes", "on"}

    def _warn_invalid_env(self, key: str, value: Any, default: Any) -> None:
        warnings.warn(
            f"Ignoring invalid EZTRACE_{key}={value!r}; using {default!r}.",
            RuntimeWarning,
            stacklevel=3,
        )

    def _get_env_int(self, key: str, default: int, *, minimum: Optional[int] = None) -> int:
        raw = os.environ.get(f"EZTRACE_{key}")
        if raw is None:
            return default
        try:
            parsed = int(raw)
        except (TypeError, ValueError):
            self._warn_invalid_env(key, raw, default)
            return default
        if not math.isfinite(parsed) or (minimum is not None and parsed < minimum):
            self._warn_invalid_env(key, raw, default)
            return default
        return parsed

    def _get_env_float(self, key: str, default: float, *, minimum: Optional[float] = None) -> float:
        raw = os.environ.get(f"EZTRACE_{key}")
        if raw is None:
            return default
        try:
            parsed = float(raw)
        except (TypeError, ValueError):
            self._warn_invalid_env(key, raw, default)
            return default
        if not math.isfinite(parsed) or (minimum is not None and parsed < minimum):
            self._warn_invalid_env(key, raw, default)
            return default
        return parsed

    def _get_env_log_level(self, key: str, default: str) -> str:
        raw = os.environ.get(f"EZTRACE_{key}")
        if raw is None:
            return default
        normalized = raw.strip().upper()
        if normalized not in self._VALID_LOG_LEVELS:
            self._warn_invalid_env(key, raw, default)
            return default
        return normalized

    def _validated_env_format(
        self,
        key: str,
        value: Optional[str],
        default: Optional[str],
    ) -> Optional[str]:
        if value is None:
            return default
        normalized = value.strip().lower()
        if normalized not in self._VALID_FORMATS:
            self._warn_invalid_env(key, value, default)
            return default
        return normalized

    @property
    def format(self) -> Optional[str]:
        return self._config['format']

    @format.setter
    def format(self, value: Optional[str]) -> None:
        normalized = str(value).strip().lower() if value is not None else None
        if normalized is not None and normalized not in self._VALID_FORMATS:
            raise ValueError(f"Unsupported log format: {value!r}")
        self._config['format'] = normalized
        self._explicit["format"] = True

    @property
    def console_format(self) -> str:
        return self._config["console_format"]

    @console_format.setter
    def console_format(self, value: str) -> None:
        normalized = str(value).strip().lower()
        if normalized not in self._VALID_FORMATS:
            raise ValueError(f"Unsupported console log format: {value!r}")
        self._config["console_format"] = normalized
        self._explicit["console_format"] = True

    @property
    def file_format(self) -> str:
        return self._config["file_format"]

    @file_format.setter
    def file_format(self, value: str) -> None:
        normalized = str(value).strip().lower()
        if normalized not in self._VALID_FORMATS:
            raise ValueError(f"Unsupported file log format: {value!r}")
        self._config["file_format"] = normalized
        self._explicit["file_format"] = True

    @property
    def format_explicit(self) -> bool:
        return self._explicit.get("format", False)

    @property
    def console_format_explicit(self) -> bool:
        return self._explicit.get("console_format", False)

    @property
    def file_format_explicit(self) -> bool:
        return self._explicit.get("file_format", False)

    @property
    def log_file(self) -> str:
        return self._config['log_file']

    @log_file.setter
    def log_file(self, value: str) -> None:
        self._config['log_file'] = value

    @property
    def max_size(self) -> int:
        return self._config['max_size']

    @max_size.setter
    def max_size(self, value: int) -> None:
        parsed = int(value)
        if parsed < 0:
            raise ValueError("max_size must be >= 0")
        self._config['max_size'] = parsed

    @property
    def backup_count(self) -> int:
        return self._config['backup_count']

    @backup_count.setter
    def backup_count(self, value: int) -> None:
        parsed = int(value)
        if parsed < 0:
            raise ValueError("backup_count must be >= 0")
        self._config['backup_count'] = parsed

    @property
    def log_dir(self) -> str:
        return self._config['log_dir']

    @log_dir.setter
    def log_dir(self, value: str) -> None:
        self._config['log_dir'] = value

    @property
    def log_level(self) -> str:
        return self._config['log_level']

    @log_level.setter
    def log_level(self, value: str) -> None:
        normalized = str(value).strip().upper()
        if normalized not in self._VALID_LOG_LEVELS:
            raise ValueError(f"Unsupported log level: {value!r}")
        self._config['log_level'] = normalized

    @property
    def buffer_enabled(self) -> bool:
        return self._config['buffer_enabled']

    @buffer_enabled.setter
    def buffer_enabled(self, value: bool) -> None:
        self._config['buffer_enabled'] = value

    @property
    def buffer_flush_interval(self) -> float:
        return self._config['buffer_flush_interval']

    @buffer_flush_interval.setter
    def buffer_flush_interval(self, value: float) -> None:
        parsed = float(value)
        if parsed <= 0:
            raise ValueError("buffer_flush_interval must be > 0")
        self._config['buffer_flush_interval'] = parsed

    def get_log_path(self) -> Path:
        """Get the full path to the log file."""
        if os.path.isabs(self.log_file):
            return Path(self.log_file)
        return Path(self.log_dir) / self.log_file

    @property
    def disable_file_logging(self) -> bool:
        return self._config['disable_file_logging']

    @disable_file_logging.setter
    def disable_file_logging(self, value: bool) -> None:
        self._config['disable_file_logging'] = value

    @property
    def disable_resource_metrics(self) -> bool:
        return self._config['disable_resource_metrics']

    @disable_resource_metrics.setter
    def disable_resource_metrics(self, value: bool) -> None:
        self._config['disable_resource_metrics'] = value

config = LogConfig()
