:mod:`weaver.data` -- Weaver data module
========================================

.. module:: weaver.data

This module provides classes for data objects such as files.


File argument parsers
---------------------

Weaver provides two functions to simplify parsing input and output files.
These functions are generally used internally by
:class:`~weaver.abstraction.Abstraction` objects rather than directly by the
user.

Input File list parser
~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: parse_input_list

**Example:**

.. testsetup::

    from weaver.data import parse_input_list

.. doctest::

    >>> # None input list
    >>> parse_input_list(None)
    []

    >>> # Input list of string
    >>> parse_input_list('/etc/hosts')
    [<weaver.data.File object at 0x...>]

    >>> # Input list of strings (files)
    >>> parse_input_list(['/bin/cat', '/bin/ls'])
    [<weaver.data.File object at 0x...>, <weaver.data.File object at 0x...>]

    >>> # Input list iterator
    >>> parse_input_list(['/bin/%s' %x  for x in ('cat', 'ls')])
    [<weaver.data.File object at 0x...>, <weaver.data.File object at 0x...>]
    >>> map(str, parse_input_list(['/bin/%s' %x  for x in ('cat', 'ls')]))
    ['/bin/cat', '/bin/ls']

    
Output File list parser
~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: parse_output_list

**Example:**

.. testsetup::

    from weaver.data import parse_output_list

.. doctest::

    >>> # None output list
    >>> parse_input_list(None)
    []

    >>> # Single file output
    >>> parse_output_list('output.txt')
    [<weaver.data.File object at 0x...>]

    >>> # Output based on template and input_list
    >>> parse_output_list('{basename}.txt', ['a', 'b'])
    [<weaver.data.File object at 0x...>, <weaver.data.File object at 0x...>]
    >>> map(str, parse_output_list('{basename}.txt', ['a', 'b']))
    ['a.txt', 'b.txt']

    >>> # Output list based on iterable
    >>> parse_output_list(['/bin/%s' %x  for x in ('cat', 'ls')])
    [<weaver.data.File object at 0x...>, <weaver.data.File object at 0x...>]
    >>> map(str, parse_output_list(['/bin/%s' %x  for x in ('cat', 'ls')]))
    ['/bin/cat', '/bin/ls']
