from pydantic_settings import BaseSettings
from pydantic import ConfigDict
from typing import Optional
import os
from pathlib import Path

class LogConfig(BaseSettings):
    """Configuration for the logging system."""
    format: str = os.environ.get("EZTRACE_LOG_FORMAT", "color")
    log_file: str = "app.log"
    max_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5
    log_dir: str = "logs"
    log_level: str = "DEBUG"
    
    model_config = ConfigDict(env_prefix="EZTRACE_")

    def get_log_path(self) -> Path:
        """Get the full path to the log file."""
        if os.path.isabs(self.log_file):
            return Path(self.log_file)
        return Path(self.log_dir) / self.log_file

config = LogConfig()
