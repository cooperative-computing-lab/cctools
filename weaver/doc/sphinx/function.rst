:mod:`weaver.function` -- Weaver function module
================================================

.. module:: weaver.function

This module provides the :class:`Function` class which is used to specify the
how an executable is to be used to generate a :class:`~weaver.engine.Task`
command string. To simplify construction :class:`Function` objects, the
:func:`parse_function` can be used to automagically construct a
:class:`Function` with just a string specfication.

Function class
--------------

.. autoclass:: weaver.function.Function
    :members:

**Example:**

.. testsetup::

    from weaver.function import Function

.. doctest::

    >>> # Cat function
    >>> cat = Function('cat')
    >>> cat.command_format('/etc/hosts', '/tmp/hosts')
    '/bin/cat  /etc/hosts > /tmp/hosts'
    >>> cat.command_format(['/etc/hosts', '/etc/resolv.conf'], '/tmp/hosts')
    '/bin/cat  /etc/hosts /etc/resolv.conf > /tmp/hosts'

    >>> # Convert function
    >>> convert = Function('convert', cmd_format = '{executable} {inputs} {outputs}')
    >>> convert.command_format('image.jpg', 'image.png')
    '/usr/bin/convert image.jpg image.png'

    >>> # Resize function
    >>> resize = Function('convert', cmd_format = '{executable} -resize {arguments} {inputs} {outputs}')
    >>> resize.command_format('image.jpg', 'image.png', '50%')
    '/usr/bin/convert -resize 50% image.jpg image.png'

    >>> # Invalid function
    >>> invalid = Function('asdffdsa')
    Traceback (most recent call last):
        ...
    weaver.util.WeaverError: utility: could not find executable: asdffdsa

Scripting Function classes
--------------------------

This module provides a few classes that allow users to embed or inline scripts
inside their source code.  The base class for this type of functionality is the
:class:`ScriptFunction` class.  Usually, users will inline short shell scripts
and so to facilitate this a :class:`ShellFunction` class is provided.
Additionally, sometimes users will define a script as normal Python function.
In these cases, these can also be used as standalone scripts by using the
:class:`PythonFunction` class.

Script Function class
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: weaver.function.ScriptFunction
    :members:

**Example:**

.. testsetup::

    from weaver.function import ScriptFunction

.. doctest::

    >>> cat = ScriptFunction('#/bin/sh\ncat $@')
    >>> cat.command_format('/etc/hosts', '/tmp/hosts')
    'script_0000  /etc/hosts > /tmp/hosts'
    >>> cat.source
    '#/bin/sh\ncat $@'

Shell Function class
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: weaver.function.ShellFunction
    :members:

**Example:**

.. testsetup::

    from weaver.function import ShellFunction

.. doctest::

    >>> # Using default shell (sh)
    >>> cat = ShellFunction('cat $@')
    >>> cat.command_format('/etc/hosts', '/tmp/hosts')
    'script_0001.sh  /etc/hosts > /tmp/hosts'
    >>> cat.source
    '#!/bin/sh\ncat $@'

    >>> # Using bash
    >>> cat = ShellFunction('cat $@', shell = 'bash')
    >>> cat.command_format('/etc/hosts', '/tmp/hosts')
    'script_0002.bash  /etc/hosts > /tmp/hosts'
    >>> cat.source
    '#!/bin/bash\ncat $@'

    >>> # Using absolute path to shell
    >>> cat = ShellFunction('cat $@', shell = '/usr/local/bin/zsh')
    >>> cat.command_format('/etc/hosts', '/tmp/hosts')
    'script_0003.zsh  /etc/hosts > /tmp/hosts'
    >>> cat.source
    '#!/usr/local/bin/zsh\ncat $@'

Python Function class
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: weaver.function.PythonFunction
    :members:

**Example:**

.. testsetup::

    from weaver.function import PythonFunction
    from weaver.util import find_executable

.. doctest::

    >>> pyfunc = PythonFunction(find_executable)
    >>> pyfunc.command_format('cat', 'output.txt')
    'find_executable.py  cat > output.txt'


Function argument parser
------------------------

.. autofunction:: parse_function

**Example:**

.. testsetup::

    from weaver.function import parse_function
    from weaver.util import find_executable

.. doctest::

    >>> # Cat function
    >>> cat = parse_function('cat {inputs} > {outputs}')
    >>> cat.command_format('/etc/hosts', '/tmp/hosts')
    '/bin/cat /etc/hosts > /tmp/hosts'

    >>> # Convert function
    >>> convert = parse_function('convert {inputs} {outputs}')
    >>> convert.command_format('image.jpg', 'image.png')
    '/usr/bin/convert image.jpg image.png'

    >>> # Resize function
    >>> resize = parse_function('convert -resize {ARG} {IN} {OUT}')
    >>> resize.command_format('image.jpg', 'image.png', '50%')
    '/usr/bin/convert -resize 50% image.jpg image.png'

    >>> # Python function
    >>> pyfunc = parse_function(find_executable)
    >>> pyfunc.command_format('cat', 'output.txt')
    'find_executable.py  cat > output.txt'

    >>> # Invalid function
    >>> invalid = parse_function(['cat'])
    Traceback (most recent call last):
        ...
    weaver.util.WeaverError: function: could not parse function: ['cat']
