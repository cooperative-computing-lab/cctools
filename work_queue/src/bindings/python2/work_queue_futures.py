## @package work_queue_futures
# Python Work Queue bindings.
#
# This is a library on top of work_queue which replaces q.wait with the concept
# of futures.
#
# This is experimental.
#
# - @ref work_queue_futures::WorkQueue
# - @ref work_queue::Task

import work_queue
import multiprocessing
import os
import subprocess
import sys
import threading
import time
import traceback
import concurrent.futures as futures
import atexit

try:
    # from py3
    import queue as ThreadQueue
except ImportError:
    # from py2
    import Queue as ThreadQueue




##
# Python Work Queue object
#
# Implements an asynchronous WorkQueueFutures object.
# @ref work_queue_futures::WorkQueueFutures.
class WorkQueueFutures(object):
    def __init__(self, *args, **kwargs):

        local_worker_args = kwargs.get('local_worker', None)
        if local_worker_args:
            del kwargs['local_worker']
            if local_worker_args is True:
                # local_worker_args can be a dictionary of worker options, or
                # simply 'True' to get the defaults (1 core, 512MB memory,
                # 1000MB of disk)
                local_worker_args = {}

        # calls to synchronous WorkQueueFutures are coordinated with _queue_lock
        self._queue_lock       = threading.Lock()
        self._stop_queue_event = threading.Event()

        # set when queue is empty
        self._join_event = threading.Event()

        self._tasks_to_submit        = ThreadQueue.Queue()
        self._tasks_before_callbacks = ThreadQueue.Queue()

        self._sync_loop = threading.Thread(target = self._sync_loop)
        self._sync_loop.daemon  = True

        self._callback_loop = threading.Thread(target = self._callback_loop)
        self._callback_loop.daemon  = True

        self._local_worker = None

        self._queue = work_queue.WorkQueue(*args, **kwargs)

        if local_worker_args:
            self._local_worker = Worker(self.port, **local_worker_args)

        self._sync_loop.start()
        self._callback_loop.start()

        atexit.register(self._terminate)


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
    def submit(self, future_task):
        if isinstance(future_task, FutureTask):
            self._tasks_to_submit.put(future_task, False)
        else:
            raise TypeError("{} is not a WorkQueue.Task")

    ##
    # Disable wait when using the futures interface
    def wait(self, *args, **kwargs):
        raise AttributeError('wait cannot be used with the futures interface.')

    ##
    # Determine whether there are any known tasks queued, running, or waiting to be collected.
    #
    # Returns 0 if there are tasks remaining in the system, 1 if the system is "empty".
    #
    # @param self       Reference to the current work queue object.
    def empty(self):
        if self._tasks_to_submit.empty():
            return self._queue.empty()
        else:
            return 0

    def _callback_loop(self):
        while not self._stop_queue_event.is_set():

            task = None
            try:
                task = self._tasks_before_callbacks.get(True, 1)
                task.set_result_or_exception()
                self._tasks_before_callbacks.task_done()
            except ThreadQueue.Empty:
                pass
            except Exception as e:
                err = traceback.format_exc()
                if task:
                    task.set_exception(FutureTaskError(t, err))
                else:
                    print(err)

    def _sync_loop(self):
        # map from taskids to FutureTask objects
        active_tasks = {}

        while True:
            try:
                if self._stop_queue_event.is_set():
                    return

                # if the queue is empty, we wait for tasks to be declared for
                # submission, otherwise _queue.wait return immediately and we
                # busy-wait
                submit_timeout = 1
                if len(active_tasks.keys()) > 0:
                    submit_timeout = 0

                # do the submits, if any
                empty = False
                while not empty:
                    try:
                        task = self._tasks_to_submit.get(True, submit_timeout)
                        if not task.cancelled():
                            with self._queue_lock:
                                submit_timeout = 0
                                taskid = self._queue.submit(task)
                                task._set_queue(self)
                                active_tasks[task.id] = task
                        self._tasks_to_submit.task_done()
                    except ThreadQueue.Empty:
                        empty = True

                # wait for any task
                with self._queue_lock:
                    if not self._queue.empty():
                        task = self._queue.wait(1)
                        if task:
                            self._tasks_before_callbacks.put(task, False)
                            del active_tasks[task.id]

                if len(active_tasks) == 0 and self._tasks_to_submit.empty():
                    self._join_event.set()

                if self._local_worker:
                    self._local_worker.check_alive()

            except Exception as e:
                # on error, we set exception to all the known tasks so that .result() does not block
                err = traceback.format_exc()
                while not self._tasks_to_submit.empty():
                    try:
                        t = self._tasks_to_submit.get(False)
                        t.set_exception(FutureTaskError(t, err))
                        self._tasks_to_submit.task_done()
                    except ThreadQueue.Empty:
                        pass
                while not self._tasks_before_callbacks.empty():
                    try:
                        t = self._tasks_before_callbacks.get(False)
                        t.set_exception(FutureTaskError(t, err))
                        self._tasks_before_callbacks.task_done()
                    except ThreadQueue.Empty:
                        pass
                for t in active_tasks.values():
                    t.set_exception(FutureTaskError(t, err))
                active_tasks.clear()
                self._stop_queue_event.set()

    def join(self, timeout=None):
        now = time.time()
        self._join_event.clear()
        return self._join_event.wait(timeout)

    def _terminate(self):
        self._stop_queue_event.set()

        for thread in [self._sync_loop, self._callback_loop]:
            try:
                thread.join()
            except RuntimeError:
                pass

        if self._local_worker:
            try:
                self._local_worker.shutdown()
            except Exception as e:
                pass

    def __del__(self):
        self._terminate()

class FutureTask(work_queue.Task):
    valid_runtime_envs = ['conda', 'singularity']

    def __init__(self, command):
        super(FutureTask, self).__init__(command)

        self._queue     = None
        self._cancelled = False
        self._exception = None

        self._done_event = threading.Event()
        self._callbacks = []

        self._runtime_env_type = None

    @property
    def queue(self):
        return self._queue

    def _set_queue(self, queue):
        self._queue = queue
        self.set_running_or_notify_cancel()

    def cancel(self):
        if self.queue:
            self.queue.cancel_by_taskid(self.id)

        self._cancelled = True
        self._done_event.set()
        self._invoke_callbacks()

        return self.cancelled()

    def cancelled(self):
        return self._cancelled

    def done(self):
        return self._done_event.is_set()

    def running(self):
        return (self._queue is not None) and (not self.done())

    def result(self, timeout=None):
        if self.cancelled():
            raise futures.CancelledError

        # wait for task to be done event if not done already
        self._done_event.wait(timeout)

        if self.done():
            if self._exception is not None:
                raise self._exception
            else:
                return self._result
        else:
            # Raise exception if task not done by timeout
            raise futures.TimeoutError(timeout)

    def exception(self, timeout=None):
        if self.cancelled():
            raise futures.CancelledError

        self._done_event.wait(timeout)

        if self.done():
            return self._exception
        else:
            raise futures.TimeoutError(timeout)


    def add_done_callback(self, fn):
        """
        Attaches the callable fn to the future. fn will be called, with the
        future as its only argument, when the future is cancelled or finishes
        running.  Added callables are called in the order that they were added
        and are always called in a thread belonging to the process that added
        them.

        If the callable raises an Exception subclass, it will be logged and
        ignored. If the callable raises a BaseException subclass, the behavior
        is undefined.

        If the future has already completed or been cancelled, fn will be
        called immediately.
        """

        if self.done():
            fn(self)
        else:
            self._callbacks.append(fn)

    def _invoke_callbacks(self):
        self._done_event.set()
        for fn in self._callbacks:
            try:
                fn(self)
            except Exception as e:
                sys.stderr.write('Error when executing future object callback:\n')
                traceback.print_exc()

    def set_result_or_exception(self):
        result = self._task.result
        if result == work_queue.WORK_QUEUE_RESULT_SUCCESS and self.return_status == 0:
            self.set_result(True)
        else:
            self.set_exception(FutureTaskError(self))

    def set_running_or_notify_cancel(self):
        if self.cancelled():
            return False
        else:
            return True

    def set_result(self, result):
        self._result = result
        self._invoke_callbacks()

    def set_exception(self, exception):
        self._exception = exception
        self._invoke_callbacks()

    def specify_runtime_env(self, type, filename):
        import _work_queue
        if type not in FutureTask.valid_runtime_envs:
            raise FutureTaskError("Runtime '{}' type is not one of {}".format(type, FutureTask.valid_runtime_envs))

        self._runtime_env_type = type

        if type == 'conda':
            conda_env = 'conda_env.tar.gz'
            self.specify_input_file(filename, conda_env, cache = True)
            command = 'mkdir -p conda_env && tar xf {} -C conda_env && source conda_env/bin/activate && {}'.format(conda_env, self.command)
            _work_queue.work_queue_task_command_line_set(self._task, command)
        elif type == 'singularity':
            sin_env = 'sin_env.img'
            self.specify_input_file(filename, sin_env, cache = True)
            command = 'singularity exec -B $(pwd):/wq-sandbox --pwd /wq-sandbox {} -- {}'.format(sin_env, self.command)
            _work_queue.work_queue_task_command_line_set(self._task, command)




class Worker(object):
    def __init__(self, port, executable='work_queue_worker', cores=1, memory=512, disk=1000):
        self._proc = None
        self._port = port

        self._executable = executable
        self._cores      = cores
        self._memory     = memory
        self._disk       = disk

        self._permanent_error = None

        self.devnull    = open(os.devnull, 'w')

        self.check_alive()


    def check_alive(self):
        if self._permanent_error is not None:
            raise Exception(self._permanent_error)
            return False

        if self._proc and self._proc.is_alive():
            return True

        if self._proc:
            self._proc.join()
            if self._proc.exitcode != 0:
                self._permanent_error = self._proc.exitcode
                return False

        return self._launch_worker()

    def shutdown(self):
        if not self._proc:
            return

        if self._proc.is_alive():
            self._proc.terminate()

        self._proc.join()

    def _launch_worker(self):
        args = [self._executable,
                '--single-shot',
                '--cores',  self._cores,
                '--memory', self._memory,
                '--disk',   self._disk,
                '--timeout', 300,
                'localhost',
                self._port]

        args = [str(x) for x in args]

        self._proc = multiprocessing.Process(target=lambda: subprocess.check_call(args, stderr=self.devnull, stdout=self.devnull), daemon=True)
        self._proc.start()

        return self.check_alive()


class FutureTaskError(Exception):
    _state_to_msg = {
        work_queue.WORK_QUEUE_RESULT_SUCCESS:             'Success',
        work_queue.WORK_QUEUE_RESULT_INPUT_MISSING:       'Input file is missing',
        work_queue.WORK_QUEUE_RESULT_OUTPUT_MISSING:      'Output file is missing',
        work_queue.WORK_QUEUE_RESULT_STDOUT_MISSING:      'stdout is missing',
        work_queue.WORK_QUEUE_RESULT_SIGNAL:              'Signal received',
        work_queue.WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION: 'Resources exhausted',
        work_queue.WORK_QUEUE_RESULT_TASK_TIMEOUT:        'Task timed-out before completion',
        work_queue.WORK_QUEUE_RESULT_UNKNOWN:             'Unknown error',
        work_queue.WORK_QUEUE_RESULT_FORSAKEN:            'Internal error',
        work_queue.WORK_QUEUE_RESULT_MAX_RETRIES:         'Maximum number of retries reached',
        work_queue.WORK_QUEUE_RESULT_TASK_MAX_RUN_TIME:   'Task did not finish before deadline',
        work_queue.WORK_QUEUE_RESULT_DISK_ALLOC_FULL:     'Disk allocation for the task is full'
    }

    def __init__(self, task, exception = None):
        self.task  = task

        self.exit_status = None
        self.state       = None
        self.exception   = None

        if exception:
            self.exception = exception
        else:
            self.exit_status = task.return_status
            self.state       = task._task.result

    def __str__(self):
        if self.exception:
            return str(self.exception)

        msg = self._state_to_str()
        if not msg:
            return str(self.state)

        if self.state != work_queue.WORK_QUEUE_RESULT_SUCCESS or self.exit_status == 0:
            return msg
        else:
            return 'Execution completed with exit status {}'.format(self.exit_status)

    def _state_to_str(self):
        return FutureTaskError._state_to_msg.get(self.state, None)

# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
