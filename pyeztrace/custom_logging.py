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
    _buffer_enabled = False  # Disable buffering by default
    _show_data_in_cli = os.environ.get("EZTRACE_SHOW_DATA_IN_CLI", "0").lower() in {"1", "true", "yes", "on"}
    
    COLOR_CODES = {
        'DEBUG': '\033[36m',  # Cyan
        'INFO': '\033[32m',   # Green
        'WARNING': '\033[33m',# Yellow
        'ERROR': '\033[31m',  # Red
        'RESET': '\033[0m',
    }

    def __init__(self, log_format: Optional[Union[str, Callable[..., str]]] = config.format) -> None:
        """
        Initialize the Logging class and set up the logger (only once).
        log_format: 'color', 'plain', 'json', 'csv', 'logfmt', or a callable
        """
        if not Setup.is_setup_done():
            raise Exception("Setup is not done. Cannot initialize logging.")
        if log_format:
            Logging._format = log_format
        if not Logging._configured:
            logger = logging.getLogger("pyeztrace")
            logger.setLevel(getattr(logging, config.log_level))
            logger.propagate = False  # Prevent logs from propagating to root logger
            formatter = logging.Formatter('%(message)s')

            # Remove all handlers associated with the named logger (avoid duplicate logs)
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)

            # Set up console handler
            stream_handler = logging.StreamHandler(sys.__stdout__)
            stream_handler.setFormatter(formatter)
            
            # Set up rotating file handler
            log_path = config.get_log_path()
            os.makedirs(log_path.parent, exist_ok=True)

            # Close and remove any existing handlers for this file
            for handler in logger.handlers[:]:
                if isinstance(handler, logging.FileHandler) and handler.baseFilename == str(log_path):
                    handler.close()
                    logger.removeHandler(handler)

            file_handler = RotatingFileHandler(
                filename=str(log_path),
                maxBytes=config.max_size,
                backupCount=config.backup_count,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            
            # Use buffered handlers for better performance
            if Logging._buffer_enabled:
                buffered_stream = BufferedHandler(stream_handler)
                buffered_file = BufferedHandler(file_handler)
                logger.addHandler(buffered_stream)
                logger.addHandler(buffered_file)
            else:
                logger.addHandler(stream_handler)
                logger.addHandler(file_handler)

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
        fn_type: Optional[str] = None,
        function: Optional[str] = None,
        duration: Optional[float] = None,
        **kwargs: Any
    ) -> str:
        # Merge context with kwargs
        context = LogContext.get_current_context()
        merged_kwargs = {**context, **kwargs}
        include_data_in_output = Logging._show_data_in_cli
        
        if Logging._format == "json":
            include_data_in_output = True  # JSON logs must include data for structured consumers
        
        project = Setup.get_project() if Setup.is_setup_done() else "?"
        level_str = level.upper()
        log_type = fn_type or ""
        func = function or context.get('function', '')
        log_format = Logging._format
        data_str = f" Data: {merged_kwargs}" if include_data_in_output and merged_kwargs else ""
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
            msg = f"{color}{timestamp} - {level_str} - [{project}] {tree} {func} {message}{reset}{data_str}"
            if duration is not None:
                msg += f" (took {duration:.5f} seconds)"
            if level_indent == 0 and (log_type == 'parent' or log_type == ''):
                msg = "\n" + msg
            return msg
        elif log_format == "plain":
            msg = f"{timestamp} - {level_str} - [{project}] {tree}{log_type} {func} {message}{data_str}"
            if duration is not None:
                msg += f" (took {duration:.5f} seconds)"
            if level_indent == 0 and (log_type == 'parent' or log_type == ''):
                msg = "\n" + msg
            return msg
        # JSON
        elif log_format == "json":
            payload = {
                "timestamp": timestamp,
                "level": level_str,
                "project": project,
                "fn_type": log_type,
                "function": func,
                "message": message,
                "data": merged_kwargs,
            }
            if duration is not None:
                payload["duration"] = duration
            return json.dumps(payload)
        # CSV
        elif log_format == "csv":
            output = io.StringIO()
            writer = csv.writer(output)
            row = [timestamp, level_str, project, log_type, func, message]
            if include_data_in_output and merged_kwargs:
                row.append(merged_kwargs)
            if duration is not None:
                row.append(f"{duration:.5f}")
            writer.writerow(row)
            return output.getvalue().strip()
        # logfmt
        elif log_format == "logfmt":
            msg = f"time={timestamp} level={level_str} project={project} fn_type={log_type} function={func} message=\"{message}\""
            if include_data_in_output and merged_kwargs:
                msg += f" data={json.dumps(merged_kwargs)}"
            if duration is not None:
                msg += f" duration={duration:.5f}"
            return msg
        # Custom callable
        elif callable(log_format):
            return log_format(level, message, fn_type, function, duration, **kwargs)
        # Fallback
        else:
            msg = f"{timestamp} - {level_str} - [{project}]|{log_type} {func} {message}"
            if duration is not None:
                msg += f" (took {duration:.5f} seconds)"
            return msg

    @staticmethod
    def log_info(
        message: str,
        fn_type: Optional[str] = None,
        function: Optional[str] = None,
        duration: Optional[float] = None,
        **kwargs: Any
    ) -> None:
        if Setup.is_setup_done():
            # Get the current context and merge with kwargs
            context = LogContext.get_current_context()
            merged_kwargs = {**context, **kwargs}
            
            msg = Logging._format_message("INFO", message, fn_type, function, duration, **merged_kwargs)
            # Capture logs in testing mode
            if Setup.is_testing_mode():
                Setup.capture_log({
                    "level": "INFO",
                    "message": message,
                    "fn_type": fn_type,
                    "function": function,
                    "duration": duration,
                    "formatted": msg,
                    "kwargs": merged_kwargs  # Use merged kwargs here
                })
                return
                
            logger = logging.getLogger("pyeztrace")
            logger.info(msg)
        else:
            raise Exception("Setup is not done. Cannot log info.")
        
    @staticmethod
    def log_error(
        message: str,
        fn_type: Optional[str] = None,
        function: Optional[str] = None,
        duration: Optional[float] = None,
        **kwargs: Any
    ) -> None:
        if Setup.is_setup_done():
            # Get the current context and merge with kwargs
            context = LogContext.get_current_context()
            merged_kwargs = {**context, **kwargs}
            
            msg = Logging._format_message("ERROR", message, fn_type, function, duration, **merged_kwargs)
            
            # Capture logs in testing mode
            if Setup.is_testing_mode():
                Setup.capture_log({
                    "level": "ERROR",
                    "message": message,
                    "fn_type": fn_type,
                    "function": function,
                    "duration": duration,
                    "formatted": msg,
                    "kwargs": merged_kwargs  # Use merged kwargs here
                })
                return
                
            logger = logging.getLogger("pyeztrace")
            logger.error(msg)
        else:
            raise Exception("Setup is not done. Cannot log error.")
        
    @staticmethod
    def log_warning(
        message: str,
        fn_type: Optional[str] = None,
        function: Optional[str] = None,
        duration: Optional[float] = None,
        **kwargs: Any
    ) -> None:
        if Setup.is_setup_done():
            # Get the current context and merge with kwargs
            context = LogContext.get_current_context()
            merged_kwargs = {**context, **kwargs}
            
            msg = Logging._format_message("WARNING", message, fn_type, function, duration, **merged_kwargs)
            
            # Capture logs in testing mode
            if Setup.is_testing_mode():
                Setup.capture_log({
                    "level": "WARNING",
                    "message": message,
                    "fn_type": fn_type,
                    "function": function,
                    "duration": duration,
                    "formatted": msg,
                    "kwargs": merged_kwargs  # Use merged kwargs here
                })
                return
                
            logger = logging.getLogger("pyeztrace")
            logger.warning(msg)
        else:
            raise Exception("Setup is not done. Cannot log warning.")
        
    @staticmethod
    def log_debug(
        message: str,
        fn_type: Optional[str] = None,
        function: Optional[str] = None,
        duration: Optional[float] = None,
        **kwargs: Any
    ) -> None:
        if Setup.is_setup_done():
            # Get the current context and merge with kwargs
            context = LogContext.get_current_context()
            merged_kwargs = {**context, **kwargs}
            
            msg = Logging._format_message("DEBUG", message, fn_type, function, duration, **merged_kwargs)
            
            # Capture logs in testing mode
            if Setup.is_testing_mode():
                Setup.capture_log({
                    "level": "DEBUG",
                    "message": message,
                    "fn_type": fn_type,
                    "function": function,
                    "duration": duration,
                    "formatted": msg,
                    "kwargs": merged_kwargs  # Use merged kwargs here
                })
                return
                
            logger = logging.getLogger("pyeztrace")
            logger.debug(msg)
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
                stack_trace = traceback.format_exc()
                
                # In testing mode, capture the stack trace too
                if Setup.is_testing_mode():
                    Setup.capture_log({
                        "level": "ERROR",
                        "message": "Stack trace",
                        "fn_type": "",
                        "function": "raise_exception_to_log",
                        "formatted": stack_trace,
                        "stack_trace": stack_trace,
                        "exception": str(exception)
                    })
                else:
                    logger = logging.getLogger("pyeztrace")
                    logger.error(stack_trace)
                    
            raise exception
        else:
            raise Exception("Setup is not done. Cannot raise exception.")
        
    @staticmethod
    def show_full_traceback() -> None:
        if Setup.is_setup_done():
            Logging.log_error("Full traceback:")
            logger = logging.getLogger("pyeztrace")
            logger.error(traceback.format_exc())
        else:
            raise Exception("Setup is not done. Cannot show full traceback.")

    @staticmethod
    def record_metric(func_name: str, duration: float) -> None:
        # Use thread-local storage for temporary metrics to reduce lock contention
        thread_id = threading.get_ident()
        if not hasattr(Logging, '_thread_metrics'):
            Logging._thread_metrics = {}
        
        if thread_id not in Logging._thread_metrics:
            Logging._thread_metrics[thread_id] = {}
            
        thread_metrics = Logging._thread_metrics[thread_id]
        
        if func_name not in thread_metrics:
            thread_metrics[func_name] = {"count": 0, "total": 0.0}
            
        thread_metrics[func_name]["count"] += 1
        thread_metrics[func_name]["total"] += duration
        
        # Periodically flush to global metrics (every 10 records)
        if thread_metrics[func_name]["count"] % 10 == 0:
            Logging._flush_thread_metrics(thread_id)
    
    @staticmethod
    def _flush_thread_metrics(thread_id):
        """Flush thread-local metrics to global metrics."""
        if not hasattr(Logging, '_thread_metrics') or thread_id not in Logging._thread_metrics:
            return
            
        with Logging._metrics_lock:
            for func_name, metrics in Logging._thread_metrics[thread_id].items():
                if func_name not in Logging._metrics:
                    Logging._metrics[func_name] = {"count": 0, "total": 0.0}
                Logging._metrics[func_name]["count"] += metrics["count"]
                Logging._metrics[func_name]["total"] += metrics["total"]
            
            # Clear thread metrics after flushing
            Logging._thread_metrics[thread_id] = {}

    @staticmethod
    def log_metrics_summary() -> None:
        # Flush any remaining thread-local metrics
        if hasattr(Logging, '_thread_metrics'):
            for thread_id in list(Logging._thread_metrics.keys()):
                Logging._flush_thread_metrics(thread_id)
                
        if not Logging._metrics:
            return
        metrics_payload = []
        total_calls = 0
        for func, m in sorted(Logging._metrics.items()):
            count = m["count"]
            total = m["total"]
            avg = total / count if count else 0.0
            total_calls += count
            metrics_payload.append({
                "function": func,
                "calls": count,
                "total_seconds": round(total, 6),
                "avg_seconds": round(avg, 6)
            })

        Logging.log_info(
            "Performance metrics summary",
            fn_type="metrics",
            function="metrics_summary",
            event="metrics_summary",
            status="success",
            metrics=metrics_payload,
            total_functions=len(metrics_payload),
            total_calls=total_calls,
            generated_at=time.time()
        )


    @staticmethod
    def disable_buffering():
        """Disable log buffering for immediate writes."""
        Logging._buffer_enabled = False
        
    @staticmethod
    def enable_buffering():
        """Enable log buffering for better performance."""
        Logging._buffer_enabled = True
        
    @staticmethod
    def flush_logs():
        """Force flush all buffered logs."""
        logger = logging.getLogger("pyeztrace")
        for handler in logger.handlers:
            if isinstance(handler, BufferedHandler):
                handler.flush()
            else:
                if hasattr(handler, 'flush'):
                    handler.flush()
