# Copyright (c) 2011- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver engine module """

from weaver.abstraction import SENTINEL
from weaver.compat      import map
from weaver.function    import Function
from weaver.logger      import D_ENGINE, debug, warn, fatal
from weaver.stack       import CurrentScript
from weaver.util        import normalize_path, WeaverError

import os
import subprocess


# Base Engine

class Engine(Function):
    """ Weaver execution engine """
    def __init__(self, path):
        Function.__init__(self, path)
        debug(D_ENGINE, 'Created {0}'.format(self))

    def emit(self, command, inputs, outputs, options):
        """ Write task to appropriate DAG file. """
        raise NotImplementedError

    def execute(self, exit_on_failure=True):
        """ Execute DAG using Engine. """
        raise NotImplementedError

    def __str__(self):
        return 'Engine({0})'.format(self.path)


# Makeflow Engine

class Makeflow(Engine):
    """ Weaver Makeflow engine """

    def __init__(self, path=None, dag_path=None, wrapper=None, track_imports=True, track_exports=True):
        Engine.__init__(self, path or 'makeflow')
        self.dag_path    = dag_path
        self.dag_file    = None
        self.wrapper     = wrapper or ''
        self.exports     = set()
        self.variables   = {}

        # Keep track of inputs and outputs.
        self.track_imports = track_imports
        self.track_exports = track_exports

        self.inputs  = set()
        self.outputs = set()

    def __call__(self, *args, **kwds):
        self.cmd_format = 'MAKEFLOW "{0}" "{1}" "{2}"'.format(
            self.dag_path, self.work_dir, self.wrapper)

        # Prune inputs of outputs (happens when we have a SubNest).
        self.inputs = [i for i in self.inputs if str(i) not in set(map(str, self.outputs))]
        Engine.__call__(self, *args, **kwds)

    def emit_task(self, abstraction, function, command, inputs, outputs, options):
        """ Write task to DAG file. """
        # Track inputs and outputs.
        if self.track_imports:
            for i in inputs:
                self.inputs.add(i)

        if self.track_exports:
            for o in outputs:
                self.outputs.add(o)

        debug(D_ENGINE, 'Emitting {0}, [{1}], [{2}], {3}'.format(
            command, ', '.join(map(str, inputs)), ', '.join(map(str, outputs)),
            options))

        # Write task outputs and inputs
        self.dag_file.write('{0}: {1}\n'.format(
            ' '.join(map(str, outputs)), ' '.join(map(str, inputs))))

        # Write debugging symbols if enabled
        if CurrentScript().include_symbols:
            if abstraction == SENTINEL:
                self.dag_file.write('\t'.join(['', '# SYMBOL', str(function)]) + '\n')
            else:
                self.dag_file.write('\t'.join(['', '# SYMBOL', str(abstraction)]) + '\n')

        # Write environmental variables
        if options.local:
            self.dag_file.write('\t@BATCH_LOCAL=1\n')
        if options.batch:
            self.dag_file.write('\t@BATCH_OPTIONS={0}\n'.format(options.batch))
        if options.collect:
            self.dag_file.write('\t@_MAKEFLOW_COLLECT_LIST+={0}\n'.format(
                ' '.join(map(str, options.collect))))
        for k, v in options.environment.items():
            self.dag_file.write('\t@{0}={1}\n'.format(k, v))

        # Write task command
        self.dag_file.write('\t{0}\n'.format(command))
        self.dag_file.flush()

    def emit_exports(self):
        """ Write exports to DAG file """
        for export in self.exports:
            self.dag_file.write('export {0}\n'.format(export))
        self.dag_file.flush()

    def emit_variables(self):
        """ Write variables to DAG file """
        for key, value in self.variables.items():
            self.dag_file.write('{0}={1}\n'.format(key, value))
        self.dag_file.flush()

    def execute(self, arguments=None, exit_on_failure=False):
        """ Execute DAG using Makeflow. """
        if self.dag_file is None:
            raise WeaverError(D_ENGINE, 'Cannot execute an empty DAG')

        # Ensure that DAG is written to disk.
        self.dag_file.flush()

        # Execute emitted DAG from the current Nest path.
        try:
            command_list = [self.path, os.path.relpath(self.dag_path, self.work_dir)]
            if self.wrapper:
                command_list.insert(0, self.wrapper)
            if arguments:
                command_list.extend(arguments.split())
            debug(D_ENGINE, 'Executing DAG {0} using {1} in {2}'.format(
                self.dag_path, self.path, self.work_dir))
            subprocess.check_call(command_list, cwd=self.work_dir)
        except subprocess.CalledProcessError as e:
            if exit_on_failure:
                log_func = fatal
            else:
                log_func = warn

            log_func(D_ENGINE, 'Failed to execute DAG {0} using {1}:\n{2}'.format(
                self.dag_path, self.path, e))

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
