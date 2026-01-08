# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


from ndcctools.taskvine.utils import load_variable_from_library
import time


def compute_task(bg, task_expr):
    func, args, kwargs = task_expr
    cache = {}

    def _follow_path(value, path):
        current = value
        for token in path:
            if isinstance(current, (list, tuple)):
                current = current[token]
            elif isinstance(current, dict):
                current = current[token]
            else:
                current = getattr(current, token)
        return current

    def on_ref(r):
        x = cache.get(r.task_key)
        if x is None:
            x = bg.load_task_output(r.task_key)
            cache[r.task_key] = x
        if r.path:
            return _follow_path(x, r.path)
        return x

    r_args = bg._visit_task_output_refs(args, on_ref, rewrite=True)
    r_kwargs = bg._visit_task_output_refs(kwargs, on_ref, rewrite=True)

    return func(*r_args, **r_kwargs)


def compute_single_key(vine_key):
    bg = load_variable_from_library('graph')

    task_key = bg.cid2pykey[vine_key]
    task_expr = bg.task_dict[task_key]

    output = compute_task(bg, task_expr)

    time.sleep(bg.extra_task_sleep_time[task_key])

    bg.save_task_output(task_key, output)
