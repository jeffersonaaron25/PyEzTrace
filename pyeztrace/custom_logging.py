import logging
import sys
import time
import os
import json
import csv
import io
import traceback
from logging.handlers import RotatingFileHandler
import threading
import queue
from pyeztrace.setup import Setup
from pyeztrace.config import config

from typing import Any, Callable, Optional, Union, Dict
from contextlib import contextmanager

class LogContext:
    """Thread-safe context management for logging."""
    _context_data = threading.local()

    @classmethod
    def get_current_context(cls) -> Dict:
        if not hasattr(cls._context_data, 'stack'):
            cls._context_data.stack = [{}]
        return cls._context_data.stack[-1]

    def __init__(self, **kwargs):
        self.context = kwargs

    def __enter__(self):
        if not hasattr(self.__class__._context_data, 'stack'):
            self.__class__._context_data.stack = [{}]
        self.__class__._context_data.stack.append({**self.__class__.get_current_context(), **self.context})
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__class__._context_data.stack.pop()


class BufferedHandler(logging.Handler):
    """Buffered logging handler for improved performance."""
    def __init__(self, target_handler, buffer_size=1000, flush_interval=1.0):
        super().__init__()
        self.target_handler = target_handler
        self.buffer = queue.Queue(maxsize=buffer_size)
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        self.last_flush = time.time()
        self._lock = threading.Lock()

    def shouldFlush(self):
        return (self.buffer.qsize() >= self.buffer_size or
                time.time() - self.last_flush >= self.flush_interval)

    def emit(self, record):
        try:
            self.buffer.put_nowait(record)
        except queue.Full:
            self.flush()
            self.buffer.put_nowait(record)
        
        if self.shouldFlush():
            self.flush()

    def flush(self):
        with self._lock:
            while not self.buffer.empty():
                try:
                    record = self.buffer.get_nowait()
                    self.target_handler.emit(record)
                except queue.Empty:
                    break
            self.last_flush = time.time()

class Logging:
    """
    A class to handle logging and exception handling, supporting multiple formats.
    """

    _configured = False
    _format = os.environ.get("EZTRACE_LOG_FORMAT", "color")  # color, plain, json, csv, logfmt
    _metrics_lock = threading.Lock()
    _metrics: Dict[str, Dict[str, Any]] = {}
    
    COLOR_CODES = {
        'DEBUG': '\033[36m',  # Cyan
        'INFO': '\033[32m',   # Green
        'WARNING': '\033[33m',# Yellow
        'ERROR': '\033[31m',  # Red
        'RESET': '\033[0m',
    }

    def __init__(self, log_format: Optional[Union[str, Callable[..., str]]] = None) -> None:
        """
        Initialize the Logging class and set up the logger (only once).
        log_format: 'color', 'plain', 'json', 'csv', 'logfmt', or a callable
        """
        if not Setup.is_setup_done():
            raise Exception("Setup is not done. Cannot initialize logging.")
        if log_format:
            Logging._format = log_format
        if not Logging._configured:
            root_logger = logging.getLogger()
            root_logger.setLevel(getattr(logging, config.log_level))
            formatter = logging.Formatter('%(message)s')

            # Remove all handlers associated with the root logger object (avoid duplicate logs)
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)

            # Set up console handler
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setFormatter(formatter)
            root_logger.addHandler(stream_handler)

            # Set up rotating file handler
            log_path = config.get_log_path()
            os.makedirs(log_path.parent, exist_ok=True)
            
            # Close and remove any existing handlers for this file
            for handler in root_logger.handlers:
                if isinstance(handler, logging.FileHandler) and handler.baseFilename == str(log_path):
                    handler.close()
                    root_logger.removeHandler(handler)
            
            file_handler = RotatingFileHandler(
                filename=str(log_path),
                maxBytes=config.max_size,
                backupCount=config.backup_count,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            
            Logging._configured = True

    @staticmethod
    def with_context(**kwargs):
        """Context manager for adding context to log messages."""
        return LogContext(**kwargs)
        
    @classmethod
    def _get_context(cls) -> Dict:
        """Get the current logging context."""
        return LogContext.get_current_context()

    @staticmethod
    def _format_message(
        level: str,
        message: str,
        type: Optional[str] = None,
        function: Optional[str] = None,
        duration: Optional[float] = None,
        **kwargs: Any
    ) -> str:
        # Merge context with kwargs
        context = LogContext.get_current_context()
        merged_kwargs = {**context, **kwargs}
        
        project = Setup.get_project() if Setup.is_setup_done() else "?"
        level_str = level.upper()
        log_type = type or ""
        func = function or context.get('function', '')
        log_format = Logging._format
        data = f" Data: {merged_kwargs}" if merged_kwargs else ""
        timestamp = time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime())
        try:
            level_indent = int(Setup.get_level())
        except Exception:
            level_indent = 0
        if level_indent == 0:
            tree = ""
        elif level_indent == 1:
            tree = "├──"
        else:
            tree = "│    " * (level_indent - 1) + "├───"  
        color = Logging.COLOR_CODES.get(level_str, '')
        reset = Logging.COLOR_CODES['RESET']
        if log_format == "color":
            msg = f"{color}{timestamp} - {level_str} - [{project}] {tree} {func} {message}{reset}{data}"
            if duration is not None:
                msg += f" (took {duration:.5f} seconds)"
            if level_indent == 0 and (log_type == 'parent' or log_type == ''):
                msg = "\n" + msg
            return msg
        elif log_format == "plain":
            msg = f"{timestamp} - {level_str} - [{project}] {tree}{log_type} {func} {message}{data}"
            if duration is not None:
                msg += f" (took {duration:.5f} seconds)"
            if level_indent == 0 and (log_type == 'parent' or log_type == ''):
                msg = "\n" + msg
            return msg
        # JSON
        elif log_format == "json":
            data = {
                "timestamp": timestamp,
                "level": level_str,
                "project": project,
                "type": log_type,
                "function": func,
                "message": message,
                "data": merged_kwargs,
            }
            if duration is not None:
                data["duration"] = duration
            return json.dumps(data)
        # CSV
        elif log_format == "csv":
            output = io.StringIO()
            writer = csv.writer(output)
            row = [timestamp, level_str, project, log_type, func, message, kwargs]
            if duration is not None:
                row.append(f"{duration:.5f}")
            writer.writerow(row)
            return output.getvalue().strip()
        # logfmt
        elif log_format == "logfmt":
            msg = f"time={timestamp} level={level_str} project={project} type={log_type} function={func} message=\"{message}\" data={kwargs}"
            if duration is not None:
                msg += f" duration={duration:.5f}"
            return msg
        # Custom callable
        elif callable(log_format):
            return log_format(level, message, type, function, duration, **kwargs)
        # Fallback
        else:
            msg = f"{timestamp} - {level_str} - [{project}]|{log_type} {func} {message} {data}"
            if duration is not None:
                msg += f" (took {duration:.5f} seconds)"
            return msg

    @staticmethod
    def log_info(
        message: str,
        type: Optional[str] = None,
        function: Optional[str] = None,
        duration: Optional[float] = None,
        **kwargs: Any
    ) -> None:
        if Setup.is_setup_done():
            msg = Logging._format_message("INFO", message, type, function, duration, **kwargs)
            logging.info(msg)
        else:
            raise Exception("Setup is not done. Cannot log info.")
        
    @staticmethod
    def log_error(
        message: str,
        type: Optional[str] = None,
        function: Optional[str] = None,
        duration: Optional[float] = None,
        **kwargs: Any
    ) -> None:
        if Setup.is_setup_done():
            msg = Logging._format_message("ERROR", message, type, function, duration, **kwargs)
            logging.error(msg)
        else:
            raise Exception("Setup is not done. Cannot log error.")
        
    @staticmethod
    def log_warning(
        message: str,
        type: Optional[str] = None,
        function: Optional[str] = None,
        duration: Optional[float] = None,
        **kwargs: Any
    ) -> None:
        if Setup.is_setup_done():
            msg = Logging._format_message("WARNING", message, type, function, duration, **kwargs)
            logging.warning(msg)
        else:
            raise Exception("Setup is not done. Cannot log warning.")
        
    @staticmethod
    def log_debug(
        message: str,
        type: Optional[str] = None,
        function: Optional[str] = None,
        duration: Optional[float] = None,
        **kwargs: Any
    ) -> None:
        if Setup.is_setup_done():
            msg = Logging._format_message("DEBUG", message, type, function, duration, **kwargs)
            logging.debug(msg)
        else:
            raise Exception("Setup is not done. Cannot log debug.")
        
    @staticmethod
    def raise_exception_to_log(
        exception: Exception,
        message: Optional[str] = None,
        stack: bool = False
    ) -> None:
        if Setup.is_setup_done():
            msg = message if message else str(exception)
            Logging.log_error(msg)
            if stack:
                logging.error(traceback.format_exc())
            raise exception
        else:
            raise Exception("Setup is not done. Cannot raise exception.")
        
    @staticmethod
    def show_full_traceback() -> None:
        if Setup.is_setup_done():
            Logging.log_error("Full traceback:")
            logging.error(traceback.format_exc())
        else:
            raise Exception("Setup is not done. Cannot show full traceback.")

    @staticmethod
    def record_metric(func_name: str, duration: float) -> None:
        with Logging._metrics_lock:
            m = Logging._metrics.setdefault(func_name, {"count": 0, "total": 0.0})
            m["count"] += 1
            m["total"] += duration

    @staticmethod
    def log_metrics_summary() -> None:
        if not Logging._metrics:
            Logging.log_warning("No performance metrics collected.")
            return
        Logging.log_info("\n=== Tracing Performance Metrics Summary ===")
        Logging.log_info(f"{'Function':40} {'Calls':>8} {'Total(s)':>12} {'Avg(s)':>12}")
        Logging.log_info("-" * 76)
        for func, m in sorted(Logging._metrics.items()):
            count = m["count"]
            total = m["total"]
            avg = total / count if count else 0.0
            Logging.log_info(f"{func:40} {count:8d} {total:12.5f} {avg:12.5f}")
        Logging.log_info("=" * 76)
