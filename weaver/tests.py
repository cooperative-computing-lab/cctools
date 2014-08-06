#!/usr/bin/env python

# Copyright (c) 2012- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver test script """

from weaver.function import PythonFunction

import os
import shutil
import subprocess
import sys

EXAMPLES_DIR = 'examples'
OUTPUT_DIR   = os.path.join('/tmp', os.environ['USER'] + '-weaver-tests')

NESTED_ABSTRACTIONS = False
INLINE_TASKS        = 1

def run_weaver(script_path, execute=False, engine_arguments=None, engine_wrapper=None, workflow_arguments=None):
    script_name = os.path.splitext(os.path.basename(script_path))[0]
    output_path = os.path.join(OUTPUT_DIR, '{0}'.format(script_name))
    log_path    = os.path.join(OUTPUT_DIR, '{0}.log'.format(script_name))
    command     = './weaver.py {0} {1} {2} {3} {4} -f -d all -l {5} -o {6} {7} {8}'.format(
        '-a' if NESTED_ABSTRACTIONS else '',
        '-t ' + str(INLINE_TASKS),
        '-x' if execute else '',
        '-w ' + engine_wrapper if engine_wrapper else '',
        '-e "' + engine_arguments + '"' if engine_arguments else '',
        log_path, output_path, script_path,
        ' '.join(map(str, workflow_arguments or [])))
    command     = PythonFunction.PYTHON_VERSION + ' ' + command

    process = subprocess.Popen(command, shell=True,
                               stdout=subprocess.PIPE,
                               stderr=open(os.devnull, 'w'))
    result  = process.communicate()[0].decode()
    if process.returncode != 0:
        raise RuntimeError
    return result

def _test_execution(script_name, **kwds):
    try:
        kwds['execute'] = True
        run_weaver(os.path.join(EXAMPLES_DIR, script_name), **kwds)
        return True
    except (OSError, RuntimeError):
        return False

def test_allpairs():
    return _test_execution('allpairs.py')

def test_arguments():
    try:
        result = run_weaver(os.path.join(EXAMPLES_DIR, 'arguments.py'), execute=False, workflow_arguments=['hello', 'world'])
        return result.strip() == "['hello', 'world']"
    except (OSError, RuntimeError):
        return False

def test_batch():
    return _test_execution('batch.py')

def test_bxgrid():
    return _test_execution('bxgrid.py', engine_wrapper='parrot_run')

def test_collect():
    return _test_execution('collect.py', engine_arguments='-g ref_count')

def test_functions():
    return _test_execution('functions.py')

def test_iterate():
    return _test_execution('iterate.py')

def test_group():
    return _test_execution('group.py')

def test_map():
    return _test_execution('map.py')

def test_merge():
    return _test_execution('merge.py')

def test_nests():
    return _test_execution('nests.py')

def test_options():
    try:
        result = run_weaver(os.path.join(EXAMPLES_DIR, 'options.py'), execute=False)
        return result == """Options(cpu=2, memory=512M, disk=10G, batch=None, local=None, collect=None, environment={})
Options(cpu=4, memory=512M, disk=10G, batch=None, local=None, collect=None, environment={})
Options(cpu=4, memory=512M, disk=10G, batch=None, local=None, collect=None, environment={})
Options(cpu=4, memory=512M, disk=1G, batch=None, local=None, collect=None, environment={})
"""
    except (OSError, RuntimeError):
        return False

def test_stash():
    return _test_execution('stash.py')

def test_scripts():
    return _test_execution('scripts.py')

def test_subnests():
    return _test_execution('subnests.py')


TESTS = [
    ('allpairs',  test_allpairs),
    ('arguments', test_arguments),
    ('batch',     test_batch),
    ('bxgrid',    test_bxgrid),
    ('collect',   test_collect),
    ('functions', test_functions),
    ('group',     test_group),
    ('iterate',   test_iterate),
    ('map',       test_map),
    ('merge',     test_merge),
    ('nests',     test_nests),
    ('options',   test_options),
    ('stash',     test_stash),
    ('scripts',   test_scripts),
    ('subnests',  test_subnests),
]

def run_tests():
    if os.path.exists(OUTPUT_DIR):
        print('Clearing {0} directory'.format(OUTPUT_DIR))
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR)

    print('Running tests {0} Nested Abstractions and Inlined Tasks ({1})'.format(
        'with' if NESTED_ABSTRACTIONS else 'without', INLINE_TASKS))
    for test_name, test_func in TESTS:
        sys.stdout.write('{0:>10} ... '.format(test_name))
        sys.stdout.flush()
        if test_func():
            sys.stdout.write('success\n')
        else:
            sys.stdout.write('failure\n')
    print('')

if __name__ == '__main__':
    if len(sys.argv) > 1:
        _TESTS = []
        for arg in sys.argv[1:]:
            _TESTS.append((arg, eval('test_' + arg)))
        TESTS = _TESTS

    NESTED_ABSTRACTIONS = False
    INLINE_TASKS        = 1
    run_tests()

    NESTED_ABSTRACTIONS = False
    INLINE_TASKS        = 4
    run_tests()

    NESTED_ABSTRACTIONS = True
    INLINE_TASKS        = 1
    run_tests()

    NESTED_ABSTRACTIONS = True
    INLINE_TASKS        = 4
    run_tests()

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
