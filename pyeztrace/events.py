"""Versioned record identities and explicit local run scopes."""
import contextvars
import os
import threading
import time
import uuid

SCHEMA_VERSION = 1
_run = contextvars.ContextVar('pyeztrace_run', default=None)
_process = os.getpid()
_process_run = uuid.uuid4().hex
_lock = threading.Lock()


def current_run_id():
    global _process, _process_run, _lock
    explicit = _run.get()
    pid = os.getpid()
    if explicit is not None and explicit[0] == pid:
        return explicit[1]
    # A PID check gives a forked child an independent implicit run.
    if _process != pid:
        # Do not acquire an inherited lock potentially held at fork.
        _process, _process_run = pid, uuid.uuid4().hex
        # Replace the inherited lock before acquiring it in the child.
        _lock = threading.Lock()
    with _lock:
        if _process_run is None:
            _process_run = uuid.uuid4().hex
        return _process_run


def envelope(kind='log'):
    return {'schema_version': SCHEMA_VERSION, 'event_id': uuid.uuid4().hex,
            'run_id': current_run_id(), 'kind': kind}


class Run:
    """Group work into an explicit run; supports ``with`` in sync/async code.

    A scope instance is single-use. Concurrent tasks inherit the run identity;
    finish them before leaving the scope. Ordinary tracing works without a Run.
    """

    def __init__(self, name='run'):
        self.name = str(name)
        self.id = uuid.uuid4().hex
        self._token = None
        self._used = False

    def __enter__(self):
        from pyeztrace.custom_logging import Logging
        if self._used:
            raise RuntimeError('Run scopes are single-use')
        self._used = True
        self._token = _run.set((os.getpid(), self.id))
        self._start = time.monotonic()
        try:
            Logging.log_info('Run started', function=self.name, event='run_start')
        except BaseException:
            _run.reset(self._token)
            raise
        return self

    def __exit__(self, exc_type, exc, tb):
        from pyeztrace.custom_logging import Logging
        current = _run.get()
        if current != (os.getpid(), self.id):
            raise RuntimeError('Run scopes must exit in reverse entry order')
        _run.reset(self._token)
        token = _run.set(current)
        try:
            Logging.log_info('Run ended', function=self.name,
                             event='run_error' if exc_type else 'run_end',
                             status='error' if exc_type else 'success',
                             duration=time.monotonic() - self._start)
        finally:
            _run.reset(token)
