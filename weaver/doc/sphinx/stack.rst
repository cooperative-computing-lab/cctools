:mod:`weaver.stack` -- Weaver stack module
==========================================

.. module:: weaver.stack


Stack class
-----------

.. autoclass:: Stack
    :members:

.. testsetup::

    from weaver.stack import Stack

.. doctest::

    >>> s = Stack()
    >>> s.push(1)
    1
    >>> s
    [1]
    >>> s.top()
    1
    >>> s.push(2)
    2
    >>> s
    [1, 2]
    >>> s.pop()
    2
    >>> s
    [1]
    >>> s.pop()
    1
    >>> s
    []
    >>> s.pop()
    Traceback (most recent call last):
        ...
    IndexError: list index out of range
