import sys
import threading
import traceback
from typing import Callable
from contextlib import contextmanager

from gi.repository import GLib

from lutris.util.log import logger

thread_namespace = threading.local()


class AsyncCall(threading.Thread):
    def __init__(self, func, callback, *args, **kwargs):
        """Execute `function` in a new thread then schedule `callback` for
        execution in the main loop.
        """
        self.callback_task = None
        self.stop_request = threading.Event()

        super().__init__(target=self.target, args=args, kwargs=kwargs)
        self.function = func
        self.callback = callback if callback else lambda r, e: None
        self.daemon = kwargs.pop("daemon", True)
        self.start()

    def target(self, *a, **kw):
        thread_namespace.stop_request = self.stop_request
        result = None
        error = None

        try:
            result = self.function(*a, **kw)
        except Exception as ex:  # pylint: disable=broad-except
            logger.error("Error while completing task %s: %s %s", self.function, type(ex), ex)
            error = ex
            _ex_type, _ex_value, trace = sys.exc_info()
            traceback.print_tb(trace)

        if not self.stop_request.is_set():
            self.callback_task = schedule_at_idle(self.callback, result, error)


@contextmanager
def check_stop(stop_event: threading.Event):
    """Context manager for active jobs to check for stop_event and raise StopRequested if set."""

    def check():
        if stop_event and stop_event.is_set():
            raise StopRequested()

    check()
    yield
    check()


class StopRequested(Exception):
    """Raised when a stop has been requested."""

    pass


class ProcessManager:
    """This class provides a basic featureset to manage a collection of threads (utilizing AsyncCall).
    It is intended to be used in cases were lots of subprocesses are being used within a pre-defined scope
    that might end before the subprocesses are done."""

    def __init__(self):
        # (name: AsyncCall Object) Dictionary of tasks currently running.
        self._active_processes = {}
        self._process_lock = threading.Lock()

    def add_job(self, func, callback=None, name: str = None, *args, **kwargs) -> str:
        """Adds and starts a job and returns its name. If no name is provided the ID of the called function is being used."""

        def _callback(r, e):
            if callback:
                callback(r, e)
            self.remove_job(name)

        if name is None:
            name = id(func)
        with self._process_lock:
            if name in self._active_processes:
                logger.error("Job with name '%s' already exists, dropping new job.", name)
                return None
            self._active_processes[name] = AsyncCall(func, _callback, *args, **kwargs)
        return str(name)

    def remove_job(self, name, send_stop_request=True, origin: str = "Undefined") -> AsyncCall:
        """Removes a job from the queue. Returns its AsyncCall object if successful.
        Define origin for better traceability in logs."""
        with self._process_lock:
            if name in self._active_processes:
                _ref = self._active_processes[name]
                del self._active_processes[name]
                if send_stop_request:
                    _ref.stop_request.set()
                return _ref
            return False

    def remove_jobs(self, origin: str = "Undefined"):
        """Sends stop requests to all running jobs and clears the queue.
        Define origin for better traceability in logs."""
        if len(self._active_processes) > 0:
            with self._process_lock:
                for name, thread in self._active_processes.items():
                    logger.debug("(%s) Sending stop request to job '%s'.", origin, name)
                    thread.stop_request.set()
                self._active_processes.clear()

    def __del__(self):
        if sys is None:
            # Interpreter is going down, we're done here
            return
        if len(self._active_processes) > 0:
            logger.critical(
                "ProcessManager abandoned with active processes still in the queue. This should never happen, stopping them gracefully..."
            )
            self.remove_jobs(origin=self.__class__.__name__)

    def __len__(self):
        """Returns the number of active processes in the queue."""
        with self._process_lock:
            return len(self._active_processes)

    def __bool__(self):
        """Returns True if there are active processes in the queue."""
        with self._process_lock:
            return len(self._active_processes) > 0

    def __str__(self):
        """Returns class name and list of names of active processes."""
        with self._process_lock:
            return f"{self.__class__.__name__}({list(self._active_processes.keys())})"

    def __contains__(self, name):
        """Returns True if a process with the given name is in the queue."""
        with self._process_lock:
            return name in self._active_processes

    def __iter__(self):
        """Returns iterator over names of active processes."""
        with self._process_lock:
            return iter(self._active_processes.keys())


class IdleTask:
    """This class provides a safe interface for cancelling idle tasks and timeouts;
    this will simply do nothing after being used once, and once the task completes,
    it will also do nothing.

    These objects are returned by the schedule methods below, which disconnect
    them when appropriate."""

    def __init__(self) -> None:
        """Initializes a task with no connection to a source, but also not completed; this can be
        connected to a source via the connect() method, unless it is completed first."""
        self.source_id = None
        self._is_completed = False

    def unschedule(self) -> None:
        """Call this to prevent the idle task from running, if it has not already run."""
        if self.is_connected():
            GLib.source_remove(self.source_id)
            self.disconnect()

    def is_connected(self) -> bool:
        """True if the idle task can still be unscheduled. If false, unschedule() will do nothing."""
        return self.source_id is not None

    def is_completed(self) -> bool:
        """True if the idle task has completed; that is, if mark_completed() was called on it."""
        return self._is_completed

    def connect(self, source_id) -> None:
        """Connects this task to a source to be unscheduled; but if the task is already
        completed, this does nothing."""
        if not self._is_completed:
            self.source_id = source_id

    def disconnect(self) -> None:
        """Break the link to the idle task, so it can't be unscheduled."""
        self.source_id = None

    def mark_completed(self) -> None:
        """Marks the task as completed, and also disconnect it."""
        self._is_completed = True
        self.disconnect()


# A task that is always completed and disconnected and does nothing.
COMPLETED_IDLE_TASK = IdleTask()
COMPLETED_IDLE_TASK.mark_completed()


def schedule_at_idle(func: Callable[..., None], *args, delay_seconds: float = 0.0) -> IdleTask:
    """Schedules a function to run at idle time, once. You can specify a delay in seconds
    before it runs.
    Returns an object to prevent it running."""

    task = IdleTask()

    def wrapper(*a, **kw) -> bool:
        try:
            func(*a, **kw)
            return False
        finally:
            task.disconnect()

    handler_object = func.__self__ if hasattr(func, "__self__") else None
    if handler_object:
        wrapper.__self__ = handler_object  # type: ignore[attr-defined]

    if delay_seconds >= 0.0:
        milliseconds = int(delay_seconds * 1000)
        source_id = GLib.timeout_add(milliseconds, wrapper, *args)
    else:
        source_id = GLib.idle_add(wrapper, *args)

    task.connect(source_id)
    return task


def schedule_repeating_at_idle(
    func: Callable[..., bool],
    *args,
    interval_seconds: float = 0.0,
) -> IdleTask:
    """Schedules a function to run at idle time, over and over until it returns False.
    It can be repeated at an interval in seconds, which will also delay it's first invocation.
    Returns an object to stop it running."""

    task = IdleTask()

    def wrapper(*a, **kw) -> bool:
        repeat = False
        try:
            repeat = func(*a, **kw)
            return repeat
        finally:
            if not repeat:
                task.disconnect()

    handler_object = func.__self__ if hasattr(func, "__self__") else None
    if handler_object:
        wrapper.__self__ = handler_object  # type: ignore[attr-defined]

    if interval_seconds >= 0.0:
        milliseconds = int(interval_seconds * 1000)
        source_id = GLib.timeout_add(milliseconds, wrapper, *args)
    else:
        source_id = GLib.idle_add(wrapper, *args)

    task.connect(source_id)
    return task
