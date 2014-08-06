''' workbench.py - Workflow Benchmark Suite

Examples:

    # Generate Chained workflow with 128 tasks
    $ ./weaver.py -o workbench examples/workbench.py chained noop 128
    
    # Generate Concurrent workflow with 128 tasks
    $ ./weaver.py -o workbench examples/workbench.py concurrent noop 128
    
    # Generate FanOut workflow with 128 tasks and 1K input file
    $ ./weaver.py -o workbench examples/workbench.py fanout cat 128 1024
    
    # Generate FanIn workflow with 128 tasks and 1K input file
    $ ./weaver.py -o workbench examples/workbench.py fanin cat 128 1024

    # Generate Map workflow with 128 tasks and 1K input files
    $ ./weaver.py -o workbench examples/workbench.py map cat 128 1024
'''

import itertools
import os

from weaver.logger import debug, fatal, D_USER

# WorkBench Functions

def make_noop_function():
    executable = os.path.join(CurrentNest().work_dir, 'noop_script')
    function   = ShellFunction('exit 0', executable=executable)
    return function

def make_cat_function():
    executable = os.path.join(CurrentNest().work_dir, 'cat_script')
    function   = ShellFunction('cat $@', executable=executable, cmd_format='{EXE} {IN} > {OUT}')
    return function

WORKBENCH_FUNCTIONS = {
    'noop'      : make_noop_function,
    'cat'       : make_cat_function,
}

def make_function(func_name, *func_args):
    try:
        function = WORKBENCH_FUNCTIONS[func_name](*func_args)
    except KeyError:
        fatal(D_USER, 'Invalid function {0}'.format(func_name))

    return function

# WorkBench Utilities

KB = 2**10
MB = 2*210
GB = 2**30

BYTE_FILES = {
    1*KB   : 'input.1K',
    1*MB   : 'input.1M',
    16*MB  : 'input.16M',
    64*MB  : 'input.64M',
    128*MB : 'input.128M',
    1*GB   : 'input.1G',
    2*GB   : 'input.2G',
    4*GB   : 'input.4G',
    8*GB   : 'input.8G',
}
CACHE_DIR  = '/var/tmp/pdonnel3/data/'
CHUNK_SIZE = 1024 * 1024

CurrentNest().Counter = itertools.count()

def generate_input_file(bytes, name=None):
    nest = CurrentNest()
    name = name or '{0:08X}.input'.format(next(nest.Counter))
    path = os.path.join(nest.work_dir, name)

    try:
        cache = os.path.join(CACHE_DIR, BYTE_FILES[bytes])
    except (OSError, KeyError):
        cache = None

    if os.path.exists(cache):
        os.symlink(cache, path)
    else:
        with open(path, 'w+') as fs:
            chunk_size    = CHUNK_SIZE if bytes > CHUNK_SIZE else bytes
            bytes_written = 0
            bytes_data    = 'x' * chunk_size
            for i in range(0, bytes, chunk_size):
                fs.write(bytes_data)
                bytes_written += chunk_size
            if bytes_written < bytes:
                fs.write('x'*(bytes - bytes_written))
    return name

# WorkBench Patterns

def run_chained(func_name, tasks, *func_args):
    debug(D_USER, 'Generating Chained Pattern with Function {0}'.format(func_name))

    tasks     = int(tasks)
    arguments = map(int, func_args)
    function  = make_function(func_name, *arguments)

    output = None
    for task in range(tasks):
        output = function(output, '{0:04d}.output'.format(task))

def run_concurrent(func_name, tasks, *func_args):
    debug(D_USER, 'Generating Concurrent Pattern with Function {0}'.format(func_name))

    tasks     = int(tasks)
    arguments = map(int, func_args)
    function  = make_function(func_name, *arguments)

    Iterate(function, tasks, '{NUMBER}.output')

def run_fanout(func_name, tasks, bytes, *func_args):
    debug(D_USER, 'Generating FanOut Pattern with Function {0}'.format(func_name))

    tasks     = int(tasks)
    bytes     = int(bytes)
    input     = generate_input_file(bytes, 'fanout.input')
    arguments = map(int, func_args)
    function  = make_function(func_name, *arguments)

    Iterate(function, tasks, '{NUMBER}.output', includes=input)

def run_fanin(func_name, tasks, bytes, *func_args):
    debug(D_USER, 'Generating FanIn Pattern with Function {0}'.format(func_name))

    tasks     = int(tasks)
    bytes     = int(bytes)
    arguments = map(int, func_args)
    function  = make_function(func_name, *arguments)
    inputs    = []

    for input in range(tasks):
        inputs.append(generate_input_file(bytes))

    function(inputs, 'fanin.output')

def run_map(func_name, tasks, bytes, *func_args):
    debug(D_USER, 'Generating Map Pattern with Function {0}'.format(func_name))

    tasks     = int(tasks)
    bytes     = int(bytes)
    arguments = map(int, func_args)
    function  = make_function(func_name, *arguments)
    inputs    = []

    for input in range(tasks):
        inputs.append(generate_input_file(bytes))

    Map(function, inputs, '{BASE_WOEXT}.output')

WORKBENCH_PATTERNS = {
    'chained'   : run_chained,
    'concurrent': run_concurrent,
    'fanout'    : run_fanout,
    'fanin'     : run_fanin,
    'map'       : run_map,
}

# WorkBench Main Dispatch

Arguments = CurrentScript().arguments

try:
    WORKBENCH_PATTERNS[Arguments[0]](*Arguments[1:])
except KeyError:
    fatal(D_USER, 'Invalid pattern: {0}'.format(Arguments[0]), print_traceback=True)
except IndexError:
    fatal(D_USER, 'No pattern specified', print_traceback=True)
