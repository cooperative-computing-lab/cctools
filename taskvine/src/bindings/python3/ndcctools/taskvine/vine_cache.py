##
# @package ndcctools.taskvine.vine_cache
#
# Task result caching module for TaskVine.
#
# Provides opt-in memoization of task results: tasks whose fingerprint matches
# a previously completed task are returned from cache without re-execution.
# Cache state persists across manager restarts via a JSON transaction log.
#
# All fingerprinting logic is contained here. task.py only provides the
# minimal infrastructure this module reads (_tracked_inputs, _tracked_outputs,
# _fn_def, _core_hash, _event).
#
# Copyright (C) 2026- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import hashlib
import inspect
import json
import os
import re
import shutil
import time
from threading import Lock

import cloudpickle


_RESOURCE_FIELDS = [
    'wall_time', 'cpu_time', 'memory', 'disk', 'cores', 'gpus',
    'bandwidth', 'bytes_read', 'bytes_written', 'bytes_received', 'bytes_sent',
    'total_processes', 'max_concurrent_processes', 'start', 'end', 'swap_memory',
    'virtual_memory',
]

# Regex patterns for stripping UUID / hex hash suffixes from Dask keys
_UUID_SUFFIX = re.compile(r"(.*?)(-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})$")
_HASH_SUFFIX = re.compile(r"(.*?)(-[0-9a-fA-F]{12,})$")
# DaskVine generates uuid4().p filenames as remote names for inter-task VINE_TEMP inputs.
# These are run-specific and must be stripped so core_hash is stable across runs.
_UUID_FILENAME = re.compile(r"^[0-9a-fA-F-]{8,}\.p$")


# ---------------------------------------------------------------------------
# Fingerprinting helpers
# ---------------------------------------------------------------------------

def _collect_callables(obj):
    """Recursively collect callable objects from args/kwargs structures."""
    funcs = []
    if callable(obj):
        funcs.append(obj)
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            funcs.extend(_collect_callables(item))
    elif isinstance(obj, dict):
        for item in obj.values():
            funcs.extend(_collect_callables(item))
    return funcs


def _normalize_dask_label(label):
    """Strip UUID or hex-hash suffixes from a Dask task key string."""
    if not isinstance(label, str):
        return label
    m = _UUID_SUFFIX.match(label)
    if m:
        return m.group(1)
    m = _HASH_SUFFIX.match(label)
    if m:
        return m.group(1)
    return label


def _canonicalize_value(value):
    """
    Normalize a value for stable hashing across runs.

    Ported directly from taskvine_rewind.vine_rewind._canonicalize_value.

    - Callables: normalized label (UUID/hash suffix stripped from __name__)
    - list/tuple: recursively canonicalized tuple
    - dict: sorted tuple of (canonical_key, canonical_value) pairs; dict VALUES
      that are DaskVine uuid4().p inter-task remote names are replaced with
      FILE_ARG:{canonical_key} so the form is stable but key-unique.
    - str: UUID/hash suffix stripped via _normalize_dask_label
    """
    if callable(value):
        label = getattr(value, "__name__", repr(value))
        return _normalize_dask_label(label)
    if isinstance(value, tuple):
        return tuple(_canonicalize_value(v) for v in value)
    if isinstance(value, list):
        return tuple(_canonicalize_value(v) for v in value)
    if isinstance(value, dict):
        canonical_items = []
        for k, v in value.items():
            ck = _canonicalize_value(k)
            if isinstance(v, str) and _UUID_FILENAME.match(v):
                cv = f"FILE_ARG:{ck}"
            else:
                cv = _canonicalize_value(v)
            canonical_items.append((ck, cv))
        canonical_items.sort(key=lambda item: repr(item[0]))
        return tuple(canonical_items)
    if isinstance(value, str):
        return _normalize_dask_label(value)
    return value


# ---------------------------------------------------------------------------
# TransactionLog
# ---------------------------------------------------------------------------

##
# @class ndcctools.taskvine.vine_cache.TransactionLog
#
# Append-only, fsync-on-write JSON log. One JSON object per line.
# Only COMPLETED records are used to reconstruct the in-memory cache.
#
class TransactionLog:
    ##
    # @param log_file  Path to the log file. Created empty if it does not exist.
    def __init__(self, log_file="vine-cache.txlog"):
        self.log_file = log_file
        self._lock = Lock()
        if not os.path.exists(log_file):
            with open(log_file, 'w'):
                pass

    ##
    # Append a JSON record to the log.
    # @param event_type  String label, e.g. "COMPLETED".
    # @param task_hash   SHA-256 hex string identifying the task.
    # @param **data      Additional fields included in the JSON record.
    def append(self, event_type, task_hash, **data):
        record = {'timestamp': time.time(), 'event': event_type, 'task_id': task_hash}
        record.update(data)
        with self._lock:
            with open(self.log_file, 'a') as f:
                f.write(json.dumps(record) + '\n')
                f.flush()
                os.fsync(f.fileno())

    ##
    # Replay the log and reconstruct the in-memory cache dict.
    # @return  dict mapping task_hash -> metadata dict for all COMPLETED entries.
    def replay(self):
        tasks = {}
        if not os.path.exists(self.log_file):
            return tasks
        with open(self.log_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                event = record.get('event', '')
                task_hash = record.get('task_id')
                if not task_hash:
                    continue
                if event == 'COMPLETED':
                    tasks[task_hash] = {
                        'task_id': task_hash,
                        'status': 'COMPLETED',
                        'exit_code': record.get('exit_code', 0),
                        'result': record.get('result'),
                        'success': record.get('success', True),
                        'std_output': record.get('std_output', ''),
                        'python_output_file': record.get('python_output_file'),
                        'wall_time': record.get('wall_time', 0),
                        'resources_measured': record.get('resources_measured'),
                        'resources_requested': record.get('resources_requested'),
                        'resources_allocated': record.get('resources_allocated'),
                        'hostname': record.get('hostname', 'unknown'),
                        'timestamp': record.get('timestamp', 0),
                    }
                elif event == 'FAILED':
                    tasks.pop(task_hash, None)
        return tasks

    ##
    # Clear the log (for testing or explicit cache invalidation).
    def clear(self):
        with self._lock:
            with open(self.log_file, 'w'):
                pass


# ---------------------------------------------------------------------------
# CachedResourceView
# ---------------------------------------------------------------------------

class _CachedResourceView:
    """Read-only proxy over a resource measurement dict, mimicking rmsummary."""
    def __init__(self, data):
        self._data = data or {}

    def __getattr__(self, item):
        return self._data.get(item, 0)

    def __repr__(self):
        return f"CachedResourceView({self._data})"


# ---------------------------------------------------------------------------
# CachedTaskResult
# ---------------------------------------------------------------------------

##
# @class ndcctools.taskvine.vine_cache.CachedTaskResult
#
# Duck-typed task result returned from Manager.wait() on a cache hit.
# Mimics the vine.Task interface so all callers (including DaskVine) work
# identically to receiving a real completed task.
#
class CachedTaskResult:
    ##
    # @param task_id          UUID string (never clashes with integer C task IDs).
    # @param original_task    Original task object; preserves .key, .sexpr, .tag.
    # @param cached_metadata  dict from TransactionLog.replay().
    # @param output_file      vine.File pointing at the cached output (optional).
    def __init__(self, task_id, original_task, cached_metadata, output_file=None):
        self._id = task_id
        self._original_task = original_task
        self._cached = cached_metadata
        self._output_file = output_file

    @property
    def id(self):
        return self._id

    @property
    def command(self):
        return self._cached.get('command', '')

    @property
    def exit_code(self):
        return self._cached.get('exit_code', 0)

    @property
    def result(self):
        return self._cached.get('result', 0)

    @property
    def std_output(self):
        return self._cached.get('std_output', '')

    @property
    def output(self):
        """Load pickled Python result from cached output file, or return std_output."""
        python_output_file = self._cached.get('python_output_file')
        if python_output_file and os.path.exists(python_output_file):
            try:
                with open(python_output_file, 'rb') as f:
                    return cloudpickle.load(f)
            except Exception:
                pass
        return self.std_output

    @property
    def output_file(self):
        """vine.File for the cached output; needed by DaskVine as a task input."""
        if self._output_file is not None:
            return self._output_file
        if hasattr(self._original_task, 'output_file') and self._original_task.output_file:
            return self._original_task.output_file
        return None

    def successful(self):
        return self._cached.get('success', True)

    def completed(self):
        return True

    @property
    def hostname(self):
        return 'CACHED'

    @property
    def addrport(self):
        return 'CACHED'

    @property
    def tag(self):
        cached_tag = self._cached.get('tag', '')
        if cached_tag:
            return cached_tag
        return getattr(self._original_task, 'tag', '')

    @property
    def key(self):
        cached_key = self._cached.get('key')
        if cached_key:
            return cached_key
        return getattr(self._original_task, 'key', None)

    @property
    def sexpr(self):
        if hasattr(self._original_task, 'sexpr'):
            return self._original_task.sexpr
        sexpr_str = self._cached.get('sexpr')
        if sexpr_str:
            try:
                return eval(sexpr_str)
            except Exception:
                return sexpr_str
        return ()

    @property
    def category(self):
        return self._cached.get('category', 'default')

    @property
    def resources_measured(self):
        data = self._cached.get('resources_measured')
        if not data:
            data = {'wall_time': self._cached.get('wall_time', 0)}
        return _CachedResourceView(data)

    @property
    def resources_requested(self):
        return _CachedResourceView(self._cached.get('resources_requested'))

    @property
    def resources_allocated(self):
        return _CachedResourceView(self._cached.get('resources_allocated'))

    @property
    def limits_exceeded(self):
        return None

    def decrement_retry(self):
        """Cached tasks never need retrying."""
        return 0

    def load_wrapper_output(self, manager):
        """Delegate wrapper output loading to original task if available."""
        if hasattr(self._original_task, 'load_wrapper_output'):
            return self._original_task.load_wrapper_output(manager)
        return self._cached.get('wrapper_output', None)

    def __repr__(self):
        key = self.key or 'unknown'
        return f"CachedTaskResult(id={self._id[:8]}..., key={key})"


# ---------------------------------------------------------------------------
# TasksCache
# ---------------------------------------------------------------------------

##
# @class ndcctools.taskvine.vine_cache.TasksCache
#
# Core caching module. Manages in-memory cache dict, persists via TransactionLog,
# and contains all fingerprinting logic for Task, PythonTask, and FunctionCall.
#
# Attach to a Manager with Manager.enable_tasks_cache().
#
class TasksCache:
    ##
    # @param cache_dir  Directory where cached output files are stored.
    # @param log_file   Path to the JSON transaction log.
    def __init__(self, cache_dir="vine-cache-outputs", log_file="vine-cache.txlog"):
        self._cache_dir = cache_dir
        self._log = TransactionLog(log_file)
        self._cache = {}
        os.makedirs(cache_dir, exist_ok=True)
        self._load()

    def _load(self):
        self._cache = self._log.replay()

    # ------------------------------------------------------------------
    # Fingerprinting API
    # ------------------------------------------------------------------

    ##
    # Pre-compute stable hash data that requires access to _fn_def.
    #
    # Must be called in Manager.submit() BEFORE task.submit_finalize(),
    # because submit_finalize() clears _fn_def on PythonTask.
    #
    # Stores the computed core hash directly on the task as task._core_hash.
    # For non-PythonTask tasks this is a no-op.
    #
    # @param task  Any task object.
    def prepare(self, task):
        if (hasattr(task, '_fn_def')
                and task._fn_def is not None
                and getattr(task, '_core_hash', None) is None):
            task._core_hash = self._compute_python_core_hash(task)

    ##
    # Compute a content-based fingerprint for any task type.
    #
    # Dispatches to the appropriate method based on task attributes:
    #   - PythonTask:    has _fn_def or _core_hash attribute
    #   - FunctionCall:  has _event attribute and get_library_required method
    #   - Task:          everything else
    #
    # @param task  Any task object with _tracked_inputs populated.
    # @return  SHA-256 hex string.
    def fingerprint(self, task):
        if hasattr(task, '_fn_def') or hasattr(task, '_core_hash'):
            return self._python_task_fingerprint(task)
        if hasattr(task, '_event') and hasattr(task, 'get_library_required'):
            return self._function_call_fingerprint(task)
        return self._task_fingerprint(task)

    def _task_fingerprint(self, task):
        """
        Fingerprint for a regular shell Task.
        SHA-256 of: type + command + sorted {remote_name: file_content_hash}.
        """
        components = [('type', 'Task'), ('command', task.command)]
        input_hashes = {}
        for (file_obj, remote_name) in getattr(task, '_tracked_inputs', []):
            source = self._file_source(file_obj)
            if source and os.path.exists(source):
                with open(source, 'rb') as f:
                    input_hashes[remote_name] = hashlib.sha256(f.read()).hexdigest()
            else:
                input_hashes[remote_name] = f"PENDING:{id(file_obj)}"
        if input_hashes:
            components.append(('inputs', sorted(input_hashes.items())))
        return hashlib.sha256(json.dumps(components, sort_keys=True).encode()).hexdigest()

    def _python_task_fingerprint(self, task):
        """
        Fingerprint for a PythonTask.
        SHA-256 of: core_hash + sorted user input file content hashes.
        Internal staging files (w_, f_, a_, o_ prefix) are excluded.
        """
        core_hash = getattr(task, '_core_hash', None)
        if core_hash is None:
            # Fallback: try to compute now (only works if _fn_def still present)
            if hasattr(task, '_fn_def') and task._fn_def is not None:
                core_hash = self._compute_python_core_hash(task)
                task._core_hash = core_hash
            else:
                core_hash = 'UNKNOWN'

        components = [('core_hash', core_hash)]
        input_hashes = []
        for (file_obj, remote_name) in getattr(task, '_tracked_inputs', []):
            if remote_name.startswith(('w_', 'f_', 'a_', 'o_')):
                continue
            source = self._file_source(file_obj)
            if source and os.path.exists(source):
                with open(source, 'rb') as f:
                    input_hashes.append(hashlib.sha256(f.read()).hexdigest())
            else:
                input_hashes.append(f"PENDING:{id(file_obj)}")
        if input_hashes:
            components.append(('inputs', sorted(input_hashes)))
        return hashlib.sha256(json.dumps(components, sort_keys=True).encode()).hexdigest()

    def _function_call_fingerprint(self, task):
        """
        Fingerprint for a FunctionCall.
        SHA-256 of: type + library name + serialized event dict.
        """
        components = [
            ('type', 'FunctionCall'),
            ('library', task.get_library_required() or ''),
        ]
        try:
            event_hash = hashlib.sha256(cloudpickle.dumps(task._event)).hexdigest()
            components.append(('event', event_hash))
        except Exception:
            components.append(('event_str', str(getattr(task, '_event', ''))))
        return hashlib.sha256(json.dumps(components, sort_keys=True).encode()).hexdigest()

    def _compute_python_core_hash(self, task):
        """
        Compute a stable hash for a PythonTask from its function + arguments.

        Reads task._fn_def = (func, args, kwargs). Collects user-defined callables
        and hashes their source code. Canonicalizes args/kwargs to remove run-specific
        identifiers (Dask UUID suffixes).
        """
        func, args, kwargs = task._fn_def

        user_funcs = _collect_callables(args) + _collect_callables(kwargs)
        canonical_args = _canonicalize_value(args)
        canonical_kwargs = _canonicalize_value(kwargs)

        components = [('type', 'PythonTask')]

        if user_funcs:
            fn_hashes = []
            for fn in user_funcs:
                if not callable(fn):
                    continue
                try:
                    src = inspect.getsource(fn).encode('utf-8')
                    fn_hashes.append(hashlib.sha256(src).hexdigest())
                except (OSError, TypeError):
                    continue
            if fn_hashes:
                components.append(('user_fns', sorted(fn_hashes)))

        try:
            components.append(('args', hashlib.sha256(cloudpickle.dumps(canonical_args)).hexdigest()))
        except Exception:
            components.append(('args_str', str(canonical_args)))

        try:
            components.append(('kwargs', hashlib.sha256(cloudpickle.dumps(canonical_kwargs)).hexdigest()))
        except Exception:
            components.append(('kwargs_str', str(canonical_kwargs)))

        return hashlib.sha256(json.dumps(components, sort_keys=True).encode()).hexdigest()

    @staticmethod
    def _file_source(file_obj):
        """Safely get the local path of a vine.File object."""
        try:
            return file_obj.source() if hasattr(file_obj, 'source') else None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Cache lookup API
    # ------------------------------------------------------------------

    ##
    # Look up a task hash in the cache.
    # @param task_hash  SHA-256 hex string.
    # @return  metadata dict if a COMPLETED entry exists, else None.
    def lookup(self, task_hash):
        entry = self._cache.get(task_hash)
        if entry and entry.get('status') == 'COMPLETED':
            return entry
        return None

    ##
    # Return True if any tracked input file is not locally readable on disk.
    #
    # Internal PythonTask staging files (w_, f_, a_, o_ prefix) are skipped
    # because they are always re-created by submit_finalize().
    #
    # When this returns True the fingerprint contains PENDING placeholders and
    # should NOT be recorded — it would never match a future run's hash.
    #
    # @param task  Task object with _tracked_inputs populated.
    def has_pending_inputs(self, task):
        for (file_obj, remote_name) in getattr(task, '_tracked_inputs', []):
            if remote_name.startswith(('w_', 'f_', 'a_', 'o_')):
                continue
            source = self._file_source(file_obj)
            if not source or not os.path.exists(source):
                return True
        return False

    ##
    # Check whether any input of the task is the output of an inflight task.
    #
    # Uses Python object identity (is) so that the same vine.File object shared
    # between two tasks is detected as a dependency.
    #
    # @param task            Task object with _tracked_inputs populated.
    # @param cache_inflight  dict: vine_task_id -> {outputs: [(file, name, flags)]}
    # @return True if a live upstream dependency is found.
    def has_upstream_dependency(self, task, cache_inflight):
        tracked_inputs = getattr(task, '_tracked_inputs', [])
        if not tracked_inputs or not cache_inflight:
            return False
        for (input_file, _remote_name) in tracked_inputs:
            for inflight_info in cache_inflight.values():
                for (output_file, _out_name, _flags) in inflight_info.get('outputs', []):
                    if input_file is output_file:
                        return True
        return False

    # ------------------------------------------------------------------
    # Cache recording API
    # ------------------------------------------------------------------

    ##
    # Copy the completed task's output file to the persistent cache directory.
    #
    # @param task       Completed task object with an output_file attribute.
    # @param task_hash  SHA-256 hex string used as the file stem.
    # @return  Destination path, or None if no copyable output exists.
    def copy_output_to_cache(self, task, task_hash):
        output_file = getattr(task, 'output_file', None)
        if output_file is None:
            return None
        source = self._file_source(output_file)
        if not source or not os.path.exists(source):
            return None
        dest = os.path.join(self._cache_dir, f"{task_hash}.output")
        shutil.copy2(source, dest)
        return dest

    ##
    # Record a completed task in the transaction log and in-memory cache.
    #
    # @param task_hash           SHA-256 hex string.
    # @param task                Completed vine.Task (or duck-typed equivalent).
    # @param python_output_path  Local path to the cached output file, or None.
    def record(self, task_hash, task, python_output_path):
        def _res(attr):
            res = getattr(task, attr, None)
            if res is None:
                return None
            return {f: getattr(res, f, None) for f in _RESOURCE_FIELDS}

        wall_time = 0
        try:
            wall_time = task.resources_measured.wall_time or 0
        except Exception:
            pass

        metadata = {
            'task_id': task_hash,
            'status': 'COMPLETED',
            'exit_code': getattr(task, 'exit_code', 0),
            'result': getattr(task, 'result', None),
            'success': True,
            'std_output': getattr(task, 'std_output', ''),
            'python_output_file': python_output_path,
            'wall_time': wall_time,
            'resources_measured': _res('resources_measured'),
            'resources_requested': _res('resources_requested'),
            'resources_allocated': _res('resources_allocated'),
            'hostname': getattr(task, 'hostname', 'unknown'),
            'timestamp': time.time(),
        }
        self._log.append('COMPLETED', task_hash, **metadata)
        self._cache[task_hash] = metadata

    ##
    # Clear all in-memory and on-disk cache state.
    def clear(self):
        self._cache = {}
        self._log.clear()

# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
