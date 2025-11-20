import os
from pathlib import Path
from typing import Any, Dict

class LogConfig:
    """Configuration for the logging system."""
    def __init__(self):
        self._config: Dict[str, Any] = {
            'format': self._get_env('LOG_FORMAT', 'color'),
            'log_file': self._get_env('LOG_FILE', 'app.log'),
            'max_size': int(self._get_env('MAX_SIZE', str(10 * 1024 * 1024))),  # 10MB
            'backup_count': int(self._get_env('BACKUP_COUNT', '5')),
            'log_dir': self._get_env('LOG_DIR', 'logs'),
            'log_level': self._get_env('LOG_LEVEL', 'DEBUG'),
            'buffer_enabled': self._get_env_bool('BUFFER_ENABLED', False),
            'buffer_flush_interval': float(self._get_env('BUFFER_FLUSH_INTERVAL', '1.0')),
            'disable_file_logging': self._get_env('DISABLE_FILE_LOGGING', '0').lower() in {'1', 'true', 'yes', 'on'},
        }

    def _get_env(self, key: str, default: str) -> str:
        """Get environment variable with EZTRACE_ prefix."""
        return os.environ.get(f'EZTRACE_{key}', default)

    def _get_env_bool(self, key: str, default: bool) -> bool:
        value = os.environ.get(f'EZTRACE_{key}')
        if value is None:
            return default
        return value.lower() in {"1", "true", "yes", "on"}

    @property
    def format(self) -> str:
        return self._config['format']

    @format.setter
    def format(self, value: str) -> None:
        self._config['format'] = value

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
        self._config['max_size'] = value

    @property
    def backup_count(self) -> int:
        return self._config['backup_count']

    @backup_count.setter
    def backup_count(self, value: int) -> None:
        self._config['backup_count'] = value

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
        self._config['log_level'] = value

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
        self._config['buffer_flush_interval'] = value

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

config = LogConfig()
