## @package WorkQueuePythonAsync
# Python Work Queue bindings.
#
# This is a library on top of work_queue which allows for asynchronous wait.
# It should work as drop-in replacement for the classic work_queue module.
#
# EXPERIMENTAL CODE, IT MAY NOT WORK AT ALL
#
# BEHAVIOUR DIFFERENCE: queue sends/receives tasks even when wait() is not called.
#
# INTERFACE DIFFERENCE: q.wait(0) returns immediately if no task to return is
# available. Also, q.receive() is the same as q.wait(0)
# 
#
# - @ref work_queue_async::WorkQueue
# - @ref work_queue::Task

import work_queue
from work_queue import Task, set_debug_flag
import threading
import Queue as ThreadQueue

##
# Python Work Queue object
#
# Implements an asynchronous WorkQueue object.
# @ref work_queue_async::WorkQueue.
class WorkQueue(object):
    def __init__(self, *args, **kwargs):
        self._queue = work_queue.WorkQueue(*args, **kwargs)
        self._thread = threading.Thread(target = self._sync_loop)

        self._stop_queue_event = threading.Event()

        self._tasks_to_return = ThreadQueue.Queue()
        self._tasks_to_submit = ThreadQueue.Queue()

        # calls to synchronous WorkQueue are coordinated with _queue_lock
        self._queue_lock     = threading.Lock()

        self._thread.daemon  = True
        self._thread.start()

    # methods not explicitly defined we route to synchronous WorkQueue, using a lock.
    def __getattr__(self, name):
        attr = getattr(self._queue, name)

        if callable(attr):
            def method_wrapped(*args, **kwargs):
                result = None
                with self._queue_lock:
                    result = attr(*args, **kwargs)
                return result
            return method_wrapped
        else:
            return attr

    ##
    # Submit a task to the queue.
    #
    # @param self   Reference to the current work queue object.
    # @param task   A task description created from @ref work_queue::Task.
    def submit(self, task):
        if isinstance(task, Task):
            self._tasks_to_submit.put(task, False)
        else:
            raise TypeError("{} is not a WorkQueue.Task")

    ##
    # Waits for timeout for a task to complete. Return such task if it exists.
    # If timeout = 0 and no task is completed, return immediately.
    #
    # @param self       Reference to the current work queue object.
    # @param timeout    The number of seconds to wait for a completed task
    #                   before returning.  Use an integer to set the timeout or the constant @ref
    #                   WORK_QUEUE_WAITFORTASK to block until a task has completed.
    def wait(self, timeout=0):
        try:
            task = self._tasks_to_return.get(True, timeout)
            self._tasks_to_return.task_done()
            return task
        except ThreadQueue.Empty:
            return None

    ##
    # Returns. a completed task. If there are no completed tasks, returns None.
    #
    # @param self       Reference to the current work queue object.
    def receive(self):
        return self.wait(0)

    ##
    # Determine whether there are any known tasks queued, running, or waiting to be collected.
    #
    # Returns 0 if there are tasks remaining in the system, 1 if the system is "empty".
    #
    # @param self       Reference to the current work queue object.
    def empty(self):
        if self._tasks_to_submit.empty() and self._tasks_to_return.empty():
            return self._queue.empty()
        else:
            return 0

    def _sync_loop(self):
        while True:
            if self._stop_queue_event.is_set():
                return

            # do the submits, if any
            while not self._tasks_to_submit.empty():
                try:
                    task = self._tasks_to_submit.get(False)
                    with self._queue_lock:
                        self._queue.submit(task)
                    self._tasks_to_submit.task_done()
                except ThreadQueue.Empty:
                    pass

            # wait for any task
            task = None
            if not self._queue.empty():
                with self._queue_lock:
                    task = self._queue.wait(1)
                if task:
                    self._tasks_to_return.put(task, False)

    def _terminate(self):
        self._stop_queue_event.set()
        self._thread.join()

    def __exit__(self):
        self._terminate()

    def __del__(self):
        self._terminate()

