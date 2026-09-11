"""Runtime contracts across initialization, context reuse and cancellation."""
import asyncio
import inspect
import logging
from logging.handlers import MemoryHandler

import pytest

from pyeztrace import exceptions
from pyeztrace.config import config
from pyeztrace.custom_logging import BufferedHandler, LogContext, Logging
from pyeztrace.setup import Setup
from pyeztrace.tracer import trace, tracing_active, _call_stack_ids, _currently_tracing


@pytest.fixture(autouse=True)
def runtime_state():
    original_config = config._config.copy(), config._explicit.copy()
    Setup.reset()
    Setup.enable_testing_mode()
    context_token = LogContext._context_stack.set(None)
    yield
    LogContext._context_stack.reset(context_token)
    Setup.disable_testing_mode()
    Setup.reset()
    config._config, config._explicit = original_config


@pytest.mark.parametrize('call', [Logging, lambda: Logging.log_info('message'),
    lambda: Logging.log_error('message'), lambda: Logging.log_debug('message'),
    lambda: Logging.log_warning('message'), lambda: Logging.log_critical('message'),
    lambda: Logging.raise_exception_to_log(ValueError('original')),
    Logging.show_full_traceback])
def test_uninitialized_logging_uses_specific_compatible_error(call):
    with pytest.raises(exceptions.SetupNotDoneError, match='Setup is not done'):
        call()


def test_async_signature_matches_sync():
    assert inspect.signature(Setup.async_initialize) == inspect.signature(Setup.initialize)


async def test_async_setup_options_and_transactional_retry(tmp_path):
    with pytest.raises(ValueError, match='Unsupported log level'):
        await Setup.async_initialize('broken', log_format='plain', log_level='invalid')
    assert not Setup.is_setup_done()
    await Setup.async_initialize(123, show_metrics=True, disable_file_logging=False,
        log_format='plain', console_format='color', file_format='json', log_level='DEBUG',
        log_dir=str(tmp_path), log_file='async.log', max_size=2048, backup_count=2,
        buffer_enabled=True, buffer_flush_interval=0.2)
    assert Setup.get_project() == '123'
    assert Setup.get_show_metrics() is True
    assert Setup.get_disable_file_logging() is False
    assert config.log_file == 'async.log' and config.log_dir == str(tmp_path)
    assert config.console_format == 'color' and config.file_format == 'json'
    assert config.log_level == 'DEBUG'
    assert config.max_size == 2048 and config.backup_count == 2
    assert config.buffer_enabled and config.buffer_flush_interval == 0.2
    with pytest.raises(exceptions.SetupAlreadyDoneError):
        Setup.initialize('again')


async def test_async_setup_has_one_winner():
    async def initialize(name):
        try:
            await Setup.async_initialize(name)
            return True
        except exceptions.SetupAlreadyDoneError:
            return False
    results = await asyncio.gather(*(initialize(str(i)) for i in range(20)))
    assert sum(results) == 1


def test_context_instance_can_be_nested_and_reused():
    context = LogContext(request='shared')
    with context:
        with context:
            assert LogContext.get_current_context() == {'request': 'shared'}
        assert LogContext.get_current_context() == {'request': 'shared'}
    assert LogContext.get_current_context() == {}
    with context:
        assert LogContext.get_current_context() == {'request': 'shared'}
    assert LogContext.get_current_context() == {}


async def test_context_instance_can_be_shared_across_tasks():
    shared = LogContext(operation='shared')
    ready = [asyncio.Event(), asyncio.Event()]
    async def worker(index):
        with LogContext(request=index):
            with shared:
                ready[index].set()
                await ready[1-index].wait()
                assert LogContext.get_current_context() == {'request': index, 'operation': 'shared'}
            assert LogContext.get_current_context() == {'request': index}
        assert LogContext.get_current_context() == {}
    await asyncio.gather(worker(0), worker(1))
    assert LogContext.get_current_context() == {}


async def test_cancellation_restores_context_in_the_cancelled_task():
    Setup.initialize('cancellation')
    entered = asyncio.Event()
    @trace()
    async def child():
        with LogContext(child=True):
            entered.set()
            await asyncio.Event().wait()
    @trace()
    async def parent():
        await child()
    async def worker():
        with LogContext(request='retained'):
            try:
                await parent()
            except asyncio.CancelledError:
                assert LogContext.get_current_context() == {'request': 'retained'}
                assert Setup.get_level() == 0
                assert not tracing_active.get()
                assert _call_stack_ids.get() == () and not _currently_tracing.get()
                raise
    task = asyncio.create_task(worker())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert LogContext.get_current_context() == {}


def test_exception_restores_nested_context():
    with LogContext(request='outer'):
        with pytest.raises(ValueError):
            with LogContext(request='inner'):
                raise ValueError('expected')
        assert LogContext.get_current_context() == {'request': 'outer'}
    assert LogContext.get_current_context() == {}


def test_buffer_flush_reaches_buffered_target():
    records = []
    class Sink(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())
    target = MemoryHandler(100, target=Sink())
    handler = BufferedHandler(target, flush_interval=60)
    try:
        handler.handle(logging.makeLogRecord({'msg': 'pending', 'levelno': logging.INFO}))
        handler.flush()
        assert records == ['pending']
    finally:
        handler.close()


def test_context_instance_can_be_shared_across_threads():
    import threading
    from concurrent.futures import ThreadPoolExecutor
    shared = LogContext(component='threads')
    barrier = threading.Barrier(4)
    def worker(index):
        with LogContext(request=index), shared:
            barrier.wait(timeout=5)
            assert LogContext.get_current_context() == {'request': index, 'component': 'threads'}
        assert LogContext.get_current_context() == {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(worker, range(4)))


def test_real_process_shutdown_smoke():
    import subprocess
    import sys
    from pathlib import Path
    script = Path(__file__).resolve().parents[1] / 'scripts' / 'smoke_runtime.py'
    result = subprocess.run([sys.executable, str(script)], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
