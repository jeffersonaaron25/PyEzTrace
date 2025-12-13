import atexit
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
import hashlib
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
    _base_format = os.environ.get("EZTRACE_LOG_FORMAT")  # Legacy: sets both console and file
    _console_format: Optional[Union[str, Callable[..., str]]] = None
    _file_format: Optional[Union[str, Callable[..., str]]] = None
    _file_logging_enabled = False
    _metrics_lock = threading.Lock()
    _metrics: Dict[str, Dict[str, Any]] = {}
    _metrics_thread = None
    _metrics_stop_event = threading.Event()
    _metrics_scheduler_started = False
    _metrics_flush_interval = float(os.environ.get("EZTRACE_METRICS_INTERVAL", "5.0"))
    _metrics_sidecar_lock = threading.Lock()
    _last_metrics_sidecar_fingerprint: Optional[str] = None
    _buffer_enabled = False  # Disable buffering by default (configurable via env)
    _buffer_flush_interval = 1.0
    _show_data_in_cli = os.environ.get("EZTRACE_SHOW_DATA_IN_CLI", "0").lower() in {"1", "true", "yes", "on"}
    
    COLOR_CODES = {
        'DEBUG': '\033[36m',  # Cyan
        'INFO': '\033[32m',   # Green
        'WARNING': '\033[33m',# Yellow
        'ERROR': '\033[31m',  # Red
        'RESET': '\033[0m',
    }

    def __init__(self, log_format: Optional[Union[str, Callable[..., str]]] = None, disable_file_logging: Optional[bool] = None) -> None:
        """
        Initialize the Logging class and set up the logger (only once).
        log_format: 'color', 'plain', 'json', 'csv', 'logfmt', or a callable
        """
        if not Setup.is_setup_done():
            raise Exception("Setup is not done. Cannot initialize logging.")

        env_buffer_enabled = os.environ.get("EZTRACE_BUFFER_ENABLED")
        env_flush_interval = os.environ.get("EZTRACE_BUFFER_FLUSH_INTERVAL")
        if env_buffer_enabled is not None:
            Logging._buffer_enabled = env_buffer_enabled.lower() in {"1", "true", "yes", "on"}
        else:
            Logging._buffer_enabled = config.buffer_enabled

        if env_flush_interval is not None:
            try:
                Logging._buffer_flush_interval = float(env_flush_interval)
            except ValueError:
                Logging._buffer_flush_interval = config.buffer_flush_interval
        else:
            Logging._buffer_flush_interval = config.buffer_flush_interval

        # Determine per-sink formats.
        # Precedence:
        # 1) Explicit log_format arg: force both sinks (backward compatible).
        # 2) Explicit config.format (env EZTRACE_LOG_FORMAT or config.format setter): sets both sinks.
        # 3) Per-sink formats (defaults: console=color, file=json).
        if log_format is not None:
            Logging._base_format = log_format
            Logging._console_format = log_format
            Logging._file_format = log_format
        else:
            if getattr(config, "format_explicit", False) and config.format is not None:
                Logging._base_format = config.format
                Logging._console_format = config.format
                Logging._file_format = config.format
            else:
                Logging._base_format = None
                Logging._console_format = config.console_format
                Logging._file_format = config.file_format

            # Allow explicit per-sink overrides even when a base format is set via env/config.
            if getattr(config, "console_format_explicit", False):
                Logging._console_format = config.console_format
            if getattr(config, "file_format_explicit", False):
                Logging._file_format = config.file_format
        if not Logging._configured:
            disable_files = Setup.get_disable_file_logging()
            if disable_file_logging is not None:
                disable_files = disable_file_logging
            if Setup.is_testing_mode():
                disable_files = True
            Logging._file_logging_enabled = not disable_files
            logger = logging.getLogger("pyeztrace")
            logger.setLevel(getattr(logging, config.log_level))
            logger.propagate = False  # Prevent logs from propagating to root logger
            formatter = logging.Formatter('%(message)s')

            class _SinkFilter(logging.Filter):
                def __init__(self, sink: str) -> None:
                    super().__init__()
                    self._sink = sink

                def filter(self, record: logging.LogRecord) -> bool:
                    if not getattr(record, "eztrace_managed", False):
                        return True
                    return getattr(record, "eztrace_sink", None) == self._sink

            # Remove all handlers associated with the named logger (avoid duplicate logs)
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)

            # Set up console handler
            stream_handler = logging.StreamHandler(sys.__stdout__)
            stream_handler.setFormatter(formatter)

            if not disable_files:
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
                    buffered_stream = BufferedHandler(stream_handler, flush_interval=Logging._buffer_flush_interval)
                    buffered_file = BufferedHandler(file_handler, flush_interval=Logging._buffer_flush_interval)
                    buffered_stream.addFilter(_SinkFilter("console"))
                    buffered_file.addFilter(_SinkFilter("file"))
                    logger.addHandler(buffered_stream)
                    logger.addHandler(buffered_file)
                else:
                    stream_handler.addFilter(_SinkFilter("console"))
                    file_handler.addFilter(_SinkFilter("file"))
                    logger.addHandler(stream_handler)
                    logger.addHandler(file_handler)
            else:
                # File logging disabled, only add stream handler
                if Logging._buffer_enabled:
                    buffered_stream = BufferedHandler(stream_handler, flush_interval=Logging._buffer_flush_interval)
                    buffered_stream.addFilter(_SinkFilter("console"))
                    logger.addHandler(buffered_stream)
                else:
                    stream_handler.addFilter(_SinkFilter("console"))
                    logger.addHandler(stream_handler)

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
        _log_format: Optional[Union[str, Callable[..., str]]] = None,
        **kwargs: Any
    ) -> str:
        # Merge context with kwargs
        context = LogContext.get_current_context()
        merged_kwargs = {**context, **kwargs}
        include_data_in_output = Logging._show_data_in_cli

        log_format = _log_format if _log_format is not None else Logging._base_format or "color"

        if log_format == "json":
            include_data_in_output = True  # JSON logs must include data for structured consumers
        
        project = Setup.get_project() if Setup.is_setup_done() else "?"
        level_str = level.upper()
        log_type = fn_type or ""
        func = function or context.get('function', '')
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

            # In testing mode, capture logs with minimal overhead (no formatting).
            if Setup.is_testing_mode():
                Setup.capture_log({
                    "level": "INFO",
                    "message": message,
                    "fn_type": fn_type,
                    "function": function,
                    "duration": duration,
                    "formatted": None,
                    "kwargs": merged_kwargs,
                })
                return
            
            console_format = Logging._console_format or "color"
            file_format = Logging._file_format or "json"

            def _formats_equal(a: Any, b: Any) -> bool:
                if isinstance(a, str) and isinstance(b, str):
                    return a == b
                return a is b

            split = Logging._file_logging_enabled and not _formats_equal(console_format, file_format)

            msg = Logging._format_message("INFO", message, fn_type, function, duration, _log_format=console_format, **merged_kwargs)

            logger = logging.getLogger("pyeztrace")
            if split:
                logger.info(msg, extra={"eztrace_managed": True, "eztrace_sink": "console"})
                file_msg = Logging._format_message("INFO", message, fn_type, function, duration, _log_format=file_format, **merged_kwargs)
                logger.info(file_msg, extra={"eztrace_managed": True, "eztrace_sink": "file"})
            else:
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

            # In testing mode, capture logs with minimal overhead (no formatting).
            if Setup.is_testing_mode():
                Setup.capture_log({
                    "level": "ERROR",
                    "message": message,
                    "fn_type": fn_type,
                    "function": function,
                    "duration": duration,
                    "formatted": None,
                    "kwargs": merged_kwargs,
                })
                return
            
            console_format = Logging._console_format or "color"
            file_format = Logging._file_format or "json"

            def _formats_equal(a: Any, b: Any) -> bool:
                if isinstance(a, str) and isinstance(b, str):
                    return a == b
                return a is b

            split = Logging._file_logging_enabled and not _formats_equal(console_format, file_format)

            msg = Logging._format_message("ERROR", message, fn_type, function, duration, _log_format=console_format, **merged_kwargs)

            logger = logging.getLogger("pyeztrace")
            if split:
                logger.error(msg, extra={"eztrace_managed": True, "eztrace_sink": "console"})
                file_msg = Logging._format_message("ERROR", message, fn_type, function, duration, _log_format=file_format, **merged_kwargs)
                logger.error(file_msg, extra={"eztrace_managed": True, "eztrace_sink": "file"})
            else:
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

            # In testing mode, capture logs with minimal overhead (no formatting).
            if Setup.is_testing_mode():
                Setup.capture_log({
                    "level": "WARNING",
                    "message": message,
                    "fn_type": fn_type,
                    "function": function,
                    "duration": duration,
                    "formatted": None,
                    "kwargs": merged_kwargs,
                })
                return
            
            console_format = Logging._console_format or "color"
            file_format = Logging._file_format or "json"

            def _formats_equal(a: Any, b: Any) -> bool:
                if isinstance(a, str) and isinstance(b, str):
                    return a == b
                return a is b

            split = Logging._file_logging_enabled and not _formats_equal(console_format, file_format)

            msg = Logging._format_message("WARNING", message, fn_type, function, duration, _log_format=console_format, **merged_kwargs)

            logger = logging.getLogger("pyeztrace")
            if split:
                logger.warning(msg, extra={"eztrace_managed": True, "eztrace_sink": "console"})
                file_msg = Logging._format_message("WARNING", message, fn_type, function, duration, _log_format=file_format, **merged_kwargs)
                logger.warning(file_msg, extra={"eztrace_managed": True, "eztrace_sink": "file"})
            else:
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

            # In testing mode, capture logs with minimal overhead (no formatting).
            if Setup.is_testing_mode():
                Setup.capture_log({
                    "level": "DEBUG",
                    "message": message,
                    "fn_type": fn_type,
                    "function": function,
                    "duration": duration,
                    "formatted": None,
                    "kwargs": merged_kwargs,
                })
                return
            
            console_format = Logging._console_format or "color"
            file_format = Logging._file_format or "json"

            def _formats_equal(a: Any, b: Any) -> bool:
                if isinstance(a, str) and isinstance(b, str):
                    return a == b
                return a is b

            split = Logging._file_logging_enabled and not _formats_equal(console_format, file_format)

            msg = Logging._format_message("DEBUG", message, fn_type, function, duration, _log_format=console_format, **merged_kwargs)

            logger = logging.getLogger("pyeztrace")
            if split:
                logger.debug(msg, extra={"eztrace_managed": True, "eztrace_sink": "console"})
                file_msg = Logging._format_message("DEBUG", message, fn_type, function, duration, _log_format=file_format, **merged_kwargs)
                logger.debug(file_msg, extra={"eztrace_managed": True, "eztrace_sink": "file"})
            else:
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
        if not Setup.get_show_metrics():
            return

        # Only run the periodic background snapshot writer when file logging is enabled.
        # Console should only show the final summary at exit.
        if Logging._file_logging_active():
            Logging._ensure_metrics_scheduler()

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
    def _ensure_metrics_scheduler() -> None:
        if Logging._metrics_scheduler_started:
            return

        Logging._metrics_scheduler_started = True

        def _run_scheduler():
            while not Logging._metrics_stop_event.wait(Logging._metrics_flush_interval):
                try:
                    # Background snapshots are persisted to a sidecar file (when enabled),
                    # not emitted as log lines to avoid console noise and mixed log schemas.
                    Logging.log_metrics_summary()
                except Exception:
                    continue

        Logging._metrics_thread = threading.Thread(target=_run_scheduler, daemon=True)
        Logging._metrics_thread.start()
        atexit.register(Logging.stop_metrics_scheduler)
    
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
    def _build_metrics_summary_snapshot() -> Optional[Dict[str, Any]]:
        # Flush any remaining thread-local metrics
        if hasattr(Logging, '_thread_metrics'):
            for thread_id in list(Logging._thread_metrics.keys()):
                Logging._flush_thread_metrics(thread_id)

        with Logging._metrics_lock:
            metrics_snapshot = {k: v.copy() for k, v in Logging._metrics.items()}

        if not metrics_snapshot:
            return None

        metrics_payload = []
        total_calls = 0
        for func, m in sorted(metrics_snapshot.items()):
            count = int(m.get("count", 0) or 0)
            total = float(m.get("total", 0.0) or 0.0)
            avg = total / count if count else 0.0
            total_calls += count
            metrics_payload.append({
                "function": func,
                "calls": count,
                "total_seconds": round(total, 6),
                "avg_seconds": round(avg, 6)
            })

        return {
            "schema_version": 1,
            "event": "metrics_summary",
            "status": "success",
            "timestamp": time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime()),
            "generated_at": time.time(),
            "metrics": metrics_payload,
            "total_functions": len(metrics_payload),
            "total_calls": total_calls,
        }

    @staticmethod
    def _file_logging_active() -> bool:
        try:
            return Logging._file_logging_enabled and (not Setup.is_testing_mode())
        except Exception:
            return False

    @staticmethod
    def _metrics_sidecar_path() -> Optional["os.PathLike"]:
        try:
            if not Setup.is_setup_done():
                return None
            if not Logging._file_logging_active():
                return None
            log_path = config.get_log_path()
            return type(log_path)(str(log_path) + ".metrics")
        except Exception:
            return None

    @staticmethod
    def _persist_metrics_sidecar(snapshot: Dict[str, Any]) -> None:
        metrics_path = Logging._metrics_sidecar_path()
        if metrics_path is None:
            return

        # Dedupe based on content, not timestamps
        fingerprint_obj = {
            "metrics": snapshot.get("metrics", []),
            "total_functions": snapshot.get("total_functions"),
            "total_calls": snapshot.get("total_calls"),
        }
        fingerprint = hashlib.sha256(
            json.dumps(fingerprint_obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

        with Logging._metrics_sidecar_lock:
            if Logging._last_metrics_sidecar_fingerprint == fingerprint:
                return

            # Best-effort append; viewer ignores malformed/partial JSON lines
            try:
                parent_dir = os.path.dirname(os.fspath(metrics_path))
                if parent_dir:
                    os.makedirs(parent_dir, exist_ok=True)
                with open(metrics_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(snapshot) + "\n")
                Logging._last_metrics_sidecar_fingerprint = fingerprint
            except Exception:
                return

    @staticmethod
    def _format_metrics_table(snapshot: Dict[str, Any]) -> str:
        metrics_list = snapshot.get("metrics") or []
        total_functions = snapshot.get("total_functions") or 0
        total_calls = snapshot.get("total_calls") or 0
        ts = snapshot.get("timestamp") or ""

        lines = []
        lines.append(f"{ts} - METRICS SUMMARY")
        lines.append(f"Functions: {total_functions}, Total calls: {total_calls}")
        if not metrics_list:
            return "\n".join(lines)

        lines.append(f"  {'Function':<40} {'Calls':>8} {'Total Time':>12} {'Avg Time':>12} {'Time/Call':>12}")
        lines.append(f"  {'-' * 40} {'-' * 8} {'-' * 12} {'-' * 12} {'-' * 12}")
        for metric in metrics_list:
            func_name = metric.get("function", "")
            calls = int(metric.get("calls", 0) or 0)
            total_seconds = float(metric.get("total_seconds", 0.0) or 0.0)
            avg_seconds = float(metric.get("avg_seconds", 0.0) or 0.0)
            time_per_call_ms = (total_seconds / calls * 1000) if calls > 0 else 0.0

            if len(func_name) > 38:
                func_name = func_name[:35] + "..."
            lines.append(
                f"  {func_name:<40} {calls:>8} {total_seconds:>12.6f}s {avg_seconds:>12.6f}s {time_per_call_ms:>12.3f}ms"
            )
        return "\n".join(lines)

    @staticmethod
    def log_metrics_summary() -> None:
        """
        Persist a metrics snapshot for tooling (viewer/UI).
        This does not emit a log line (to avoid console spam and mixed schemas).
        """
        if not Setup.get_show_metrics():
            return

        snapshot = Logging._build_metrics_summary_snapshot()
        if snapshot is None:
            return

        Logging._persist_metrics_sidecar(snapshot)

    @staticmethod
    def log_final_metrics_summary() -> None:
        """
        Print the final metrics summary to console at exit (if enabled),
        and persist one last snapshot to the sidecar when file logging is enabled.
        """
        if not Setup.is_setup_done() or not Setup.get_show_metrics():
            return

        try:
            Logging.stop_metrics_scheduler()
        except Exception:
            pass

        snapshot = Logging._build_metrics_summary_snapshot()
        if snapshot is None:
            return

        Logging._persist_metrics_sidecar(snapshot)

        # Avoid noisy output in testing mode.
        if Setup.is_testing_mode():
            return

        try:
            print(Logging._format_metrics_table(snapshot), file=sys.__stdout__)
        except Exception:
            pass

    @staticmethod
    def stop_metrics_scheduler() -> None:
        Logging._metrics_stop_event.set()
        thread = Logging._metrics_thread
        if thread and thread.is_alive():
            thread.join(timeout=1.0)
        Logging._metrics_thread = None
        Logging._metrics_scheduler_started = False
        Logging._metrics_stop_event = threading.Event()


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
