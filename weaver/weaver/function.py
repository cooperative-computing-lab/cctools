# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver function module """

from weaver.compat  import callable, getfuncname
from weaver.data    import parse_input_list, parse_output_list
from weaver.logger  import D_FUNCTION, debug
from weaver.options import Options
from weaver.stack   import CurrentAbstraction, CurrentNest
from weaver.util    import find_executable, parse_string_list, type_str, WeaverError

import inspect
import itertools
import os
import sys


# Base Function class

class Function(object):
    """ This is the base Function class.

    A :class:`Function` provides the :meth:`command` method that specifies how
    to generate the command for the executable associated with the
    :class:`Function` instance.

    At a minimum, the user must specify the name of the `executable`.  For
    convenience, the function :func:`~weaver.util.find_executable` is used to
    locate the executable.

    **Positional Arguments:**

    - `executable`  -- Path or name of executable.

    **Keyword Arguments:**

    - `cmd_format`  -- String template used to generate command string.
    - `find_dirs`   -- Additional directories to search for executable.

    The `cmd_format` supports the following fields:

    - `{executable}`, `{EXE}` -- The executable file.
    - `{inputs}`, `{IN}`      -- The inputs files.
    - `{outputs}`, `{OUT}`    -- The output files.
    - `{arguments}`, `{ARG}`  -- The arguments.

    The default command string template is :data:`~weaver.Function.CMD_FORMAT`.
    """
    #: Default command string format template
    CMD_FORMAT = '{executable} {arguments} {inputs} > {outputs}'

    def __init__(self, executable, cmd_format=None, find_dirs=None,
        environment=None):
        self.cmd_format  = cmd_format or Function.CMD_FORMAT
        self.path        = find_executable(executable, find_dirs)
        self.environment = environment or dict()
        self.includes    = set([self.path])

        debug(D_FUNCTION, 'Created Function {0}({1}, {2})'.format(
            type_str(self), self.path, self.cmd_format))

    def __call__(self, inputs=None, outputs=None, arguments=None,
        includes=None, local=False, environment=None, collect=False):
        abstraction = CurrentAbstraction()
        nest        = CurrentNest()

        # Engine Functions define inputs and output member attributes
        try:
            inputs  = inputs  or self.inputs
            outputs = outputs or self.outputs
        except AttributeError:
            pass

        inputs   = parse_input_list(inputs)
        outputs  = parse_output_list(outputs, inputs)
        includes = parse_input_list(includes) + parse_input_list(self.includes)
        command  = self.command_format(inputs, outputs, arguments)
        options  = Options(environment=dict(self.environment), collect=inputs if collect else None)

        if local:
            options.local = True

        if environment:
            options.environment.update(environment)

        nest.schedule(abstraction, self, command,
            list(inputs) + list(includes), outputs, options)

        return outputs

    def command_format(self, inputs=None, outputs=None, arguments=None):
        """
        Returns command string by formatting function template with `inputs`
        and `outputs` arguments.

        This method requires the user to **explicitly** specify the `inputs`
        and `outputs` to be used in the command string.
        """
        inputs    = ' '.join(parse_string_list(inputs))
        outputs   = ' '.join(parse_string_list(outputs))
        arguments = ' '.join(parse_string_list(arguments))
        return self.cmd_format.format(
            executable  = self.path,
            EXE         = self.path,
            inputs      = inputs,
            IN          = inputs,
            outputs     = outputs,
            OUT         = outputs,
            arguments   = arguments,
            ARG         = arguments)

    def __str__(self):
        return self.cmd_format.format(
            executable  = self.path,
            EXE         = self.path,
            inputs      = '{inputs}',
            IN          = '{IN}',
            outputs     = '{outputs}',
            OUT         = '{OUT}',
            arguments   = '{arguments}',
            ARG         = '{ARG}')


# Scripting Function classes

class ScriptFunction(Function):
    """ This is the base scripting Function class.

    This class allows for users to define :class:`Function` objects by
    embedding scripts inside of their code.

    **Positional Arguments:**

        - `source`      -- Source code for the script.

    **Keyword Arguments:**

        - `executable`  -- Path or name to use for the script.
        - `cmd_format`  -- String template used to generate command string.

    If `executable` is ``None``, then a unique script name will be generated.
    """
    def __init__(self, source, executable=None, cmd_format=None):
        if executable is None:
            executable = next(CurrentNest().stash)

        with open(executable, 'w') as fs:
            fs.write(source)
        os.chmod(executable, 0o755)

        Function.__init__(self, executable, cmd_format)


class ShellFunction(ScriptFunction):
    """ This allows the user to embed a shell script.

    **Positional Arguments:**

        - `source`      -- Source code for the script.

    **Keyword Arguments:**

        - `shell`       -- Shell to be used to execute script.
        - `executable`  -- Path or name to use for the script.
        - `cmd_format`  -- String template used to generate command string.

    The supported values for `shell` are ``sh``, ``ksh``, ``bash``, ``csh``,
    and ``tcsh``.  The class assumes that the shells are located in ``/bin``.
    If you pass an absolute path instead of one of the mentioned `shell`
    values, then that will be used as the `shell` path and the basename of the
    specified `shell` path will be used as the script extension.
    """
    SHELL_TABLE = {
        'sh'    :   '/bin/sh',
        'ksh'   :   '/bin/ksh',
        'bash'  :   '/bin/bash',
        'csh'   :   '/bin/csh',
        'tcsh'  :   '/bin/tcsh',
    }
    SHELL_DEFAULT = 'sh'

    def __init__(self, source, shell=None, executable=None, cmd_format=None):
        if shell is None or not os.path.isabs(shell):
            if shell not in ShellFunction.SHELL_TABLE:
                shell = ShellFunction.SHELL_DEFAULT
            shell_path = ShellFunction.SHELL_TABLE[shell]
        else:
            shell_path = shell
            shell = os.path.basename(shell)
        source = '#!%s\n' % shell_path + source
        ScriptFunction.__init__(self, source, executable, cmd_format)


class PythonFunction(ScriptFunction):
    """ This allows the user to embed Python scripts as functions.

    **Positional Arguments:**

        - `function`    -- Name of Python function to materialize as a script.

    **Keyword Arguments:**

        - `executable`  -- Path or name to use for the script.
        - `cmd_format`  -- String template used to generate command string.
    """
    PYTHON_VERSION  = 'python{0}.{1}'.format(sys.version_info[0], sys.version_info[1])
    PYTHON_TEMPLATE = '''#!/usr/bin/env {0}
import {{0}}
{{1}}
if __name__ == '__main__':
    {{2}}(*sys.argv[1:])
'''.format(PYTHON_VERSION)

    def __init__(self, function, executable=None, cmd_format=None):
        # TODO: this doesn't work with Python3
        body = inspect.getsource(function)
        name = getfuncname(function)
        imports = ['os', 'sys']
        try:
            imports.extend(function.func_imports)
        except AttributeError:
            pass
        source = self.PYTHON_TEMPLATE.format(', '.join(imports), body, name)
        ScriptFunction.__init__(self, source, executable, cmd_format)


# Function argument parser

def parse_function(function, py_func_builder=PythonFunction, environment=None):
    """ Return a :class:`Function` object based on the input `function`.

    If `function` is already a :class:`Function`, then return it.  If it is a
    string, then parse it and automagically construct a :class:`Function`
    object.  Otherwise, raise a :class:`~weaver.util.WeaverError`.

    This means that a `function` must be one of the following:

    1. An existing :class:`Function`.
    2. A string template (ex. `{executable} {arguments} {inputs} {outputs}`)
    3. A real Python function that will be converted.

    .. note::

        The parser expects that the **first word** in the `function` string to
        refer to the name of the executable to be used for the
        :class:`Function`.
    """
    if isinstance(function, Function):
        return function

    if isinstance(function, str):
        if ' ' in function:
            flist = function.split(' ')
            return Function(flist[0],
                cmd_format  = ' '.join(['{executable}'] + flist[1:]),
                environment = environment)

        return Function(function, environment=environment)

    if callable(function):
        return py_func_builder(function)

    raise WeaverError(D_FUNCTION,
        'could not parse function argument: {0}'.format(function))

ParseFunction = parse_function


# Pipeline function

class Pipeline(Function):
    DEFAULT_SEPARATOR = '&&'

    def __init__(self, functions, separator=None):
        self.functions = [parse_function(f) for f in functions]
        Function.__init__(self, self.functions[0].path,
            cmd_format='Pipeline({0})'.format(map(str, self.functions)))
        self.includes  = set([f.path for f in self.functions])
        if separator is None:
            self.separator = Pipeline.DEFAULT_SEPARATOR
        else:
            self.separator = separator

    def command_format(self, inputs=None, outputs=None, arguments=None):
        divider = ' ' + self.separator + ' '
        return divider.join([f.command_format(inputs, outputs, arguments)
                             for f in self.functions])

    def __str__(self):
        return self.cmd_format

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
