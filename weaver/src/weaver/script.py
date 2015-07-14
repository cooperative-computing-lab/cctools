# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver script module """

from weaver.compat  import execfile
from weaver.logger  import D_SCRIPT, debug, fatal
from weaver.nest    import Nest
from weaver.options import Options
from weaver.stack   import WeaverScripts, stack_context_manager
from weaver.util    import Container

import weaver.logger # Needed for Script.show_usage

import collections
import os
import sys
import time

# ugly technique to obtain version number for -v optoin
import __main__
cctools_version = __main__.cctools_version

# Built-ins

ABSTRACTIONS = ['Abstraction', 'AllPairs', 'Iterate', 'Map', 'MapReduce',
                'Merge']
DATASETS     = ['Dataset', 'FileList', 'Glob',
                'SQLDataset', 'MySQLDataset',
                'And', 'Or', 'Query']
FUNCTIONS    = ['Function', 'ScriptFunction', 'ShellFunction',
                'PythonFunction', 'ParseFunction', 'Pipeline']
NESTS        = ['Nest', 'Define', 'Export']
OPTIONS      = ['Options']
STACKS       = ['CurrentAbstraction', 'CurrentNest', 'CurrentOptions',
                'CurrentScript']


# Script class

@stack_context_manager(D_SCRIPT, WeaverScripts)
class Script(object):
    """ Weaver Script class.

    Parses command line environment and sets up run-time configuration.
    """
    SCRIPT_OPTIONS_TABLE = {
        '-b': lambda self, args:
                self.set_options(args.popleft().split(',')),
        '-I': lambda self, args:
                setattr(self, 'import_builtins', False),
        '-N': lambda self, args:
                setattr(self, 'normalize_paths', False),
        '-d': lambda self, args:
                weaver.logger.enable(args.popleft().split(',')),
        '-W': lambda self, args:
                setattr(self, 'force', False),
        '-g': lambda self, args:
                setattr(self, 'include_symbols', True),
        '-o': lambda self, args:
                weaver.logger.set_log_path(args.popleft()),
        '-x': lambda self, args:
                setattr(self, 'execute_dag', True),
        '-O': lambda self, args:
                setattr(self, 'output_directory', str(args.popleft())),
        '-a': lambda self, args:
                setattr(self, 'nested_abstractions', True),
        '-t': lambda self, args:
                setattr(self, 'inline_tasks', int(args.popleft())),
        '-w': lambda self, args:
                setattr(self, 'engine_wrapper', args.popleft()),
        '-e': lambda self, args:
                setattr(self, 'engine_arguments', args.popleft()),
        '-v': lambda self, args:
                self.show_version(),
        '-h': lambda self, args:
                self.show_usage()
    }

    def __init__(self, args):
        self.path                = None
        self.force               = True        # Ignore warnings
        self.import_builtins     = True        # Load built-ins
        self.output_directory    = os.curdir   # Where to create artifacts
        self.start_time          = time.time() # Record beginning of compiling
        self.options             = Options()
        self.nested_abstractions = False
        self.inline_tasks        = 1
        self.execute_dag         = False
        self.globals             = {}
        self.engine_wrapper      = None
        self.engine_arguments    = None
        self.include_symbols     = False
        self.normalize_paths     = True

        args = collections.deque(args)
        while args:
            arg = args.popleft()
            try:
                if arg.startswith('-'):
                    self.SCRIPT_OPTIONS_TABLE[arg](self, args)
                else:
                    self.path = arg
                    self.arguments = list(args)
                    args.clear()
            except (IndexError, KeyError):
                fatal(D_SCRIPT, 'invalid command line option: {0}'.format(arg))

        if self.normalize_paths:
            self.output_directory = os.path.abspath(self.output_directory)

        debug(D_SCRIPT, 'path                = {0}'.format(self.path))
        debug(D_SCRIPT, 'force               = {0}'.format(self.force))
        debug(D_SCRIPT, 'import_builtins     = {0}'.format(self.import_builtins))
        debug(D_SCRIPT, 'output_directory    = {0}'.format(self.output_directory))
        debug(D_SCRIPT, 'start_time          = {0}'.format(self.start_time))
        debug(D_SCRIPT, 'options             = {0}'.format(self.options))
        debug(D_SCRIPT, 'nested_abstractions = {0}'.format(self.nested_abstractions))
        debug(D_SCRIPT, 'inline_tasks        = {0}'.format(self.inline_tasks))
        debug(D_SCRIPT, 'execute_dag         = {0}'.format(self.execute_dag))
        debug(D_SCRIPT, 'engine_wrapper      = {0}'.format(self.engine_wrapper))
        debug(D_SCRIPT, 'engine_arguments    = {0}'.format(self.engine_arguments))
        debug(D_SCRIPT, 'normalize_paths     = {0}'.format(self.normalize_paths))

        if self.path is None:
            self.show_usage()

    def __str__(self):
        return 'Script({0})'.format(self.path)

    def _import(self, module, symbols):
        """ Import ``symbols`` from ``module`` into global namespace. """
        # Import module
        m = 'weaver.{0}'.format(module)
        m = __import__(m, self.globals, self.globals, symbols, -1)

        # Import symbols from module into global namespace, which we store as
        # an attribute for later use (i.e. during compile)
        for symbol in symbols:
            self.globals[symbol] = getattr(m, symbol)
            debug(D_SCRIPT, 'Imported {0} from {1}'.format(symbol, module))

    def set_options(self, options):
        for c in options:
            key, value = c.split('=')
            try:
                setattr(self.options, key.strip(), value.strip())
            except AttributeError:
                fatal(D_SCRIPT, 'Invalid option: {0}={1}'.format(key, value))

    @staticmethod
    def show_version():
        global cctools_version
        sys.stderr.write('weaver '+cctools_version+"\n")
        sys.exit(1)

    @staticmethod
    def show_usage():
        """ Print usage description and abort. """
        subsystems = [eval('weaver.logger.{0}'.format(f))
                        for f in dir(weaver.logger)
                        if 'D_' in f]
        sys.stderr.write('''Usage: {0} [options] <script> [arguments]

General Options:
  -h              Show this help message.
  -v              Show version string.
  -W              Stop on warnings.
  -g              Include debugging symbols in DAG.
  -I              Do not automatically import built-ins.
  -N              Do not normalize paths.
  -b <options>    Set batch job options (cpu, memory, disk, batch, local, collect).
  -d <subsystem>  Enable debugging for subsystem.
  -o <log_path>   Set log path (default: stderr).
  -O <directory>  Set output directory.

Optimization Options:
  -a              Automatically nest abstractions.
  -t <group_size> Inline tasks based on group size.

Engine Options:
  -x              Execute DAG using workflow engine after compiling.
  -e <arguments>  Set arguments to workflow engine when executing.
  -w <wrapper>    Set workflow engine wrapper.

Subsystems:
  {1}
'''.format(sys.argv[0], ', '.join(subsystems)))
        sys.exit(1)

    def compile(self):
        """ Compile script in the specified working directory. """
        with self:
            # Save active script instance and set this one as active
            work_dir = self.output_directory

            # Add nest path and path to script to Python module path to allow
            # for importing modules outside of $PYTHONPATH
            sys.path.insert(0, os.path.abspath(os.path.dirname(work_dir)))
            sys.path.insert(0, os.path.abspath(os.path.dirname(self.path)))

            # Load built-ins if specified on command line.  If built-ins are
            # not automatically loaded by the Script object, then the user must
            # load them manually in their Weaver scripts using the standard
            # Python import facilities.
            if self.import_builtins:
                self._import('abstraction', ABSTRACTIONS)
                self._import('dataset', DATASETS)
                self._import('function', FUNCTIONS)
                self._import('nest', NESTS)
                self._import('options', OPTIONS)
                self._import('stack', STACKS)

            # Execute nest
            with Nest(work_dir, wrapper=self.engine_wrapper) as nest:
                with self.options:
                    try:
                        execfile(self.path, self.globals)
                        nest.compile()
                    except Exception as e:
                        fatal(D_SCRIPT, 'Error compiling script: {0}'.format(e), print_traceback=True)

                    if self.execute_dag:
                        debug(D_SCRIPT, 'Executing generated DAG {0} with {1}'.format(
                            nest.dag_path, nest.path))
                        nest.execute(self.engine_arguments, exit_on_failure=True)

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
