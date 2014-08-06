:mod:`weaver.logger` -- Weaver logging module
=============================================

.. module:: weaver.logger

This module provides a simple logging class named :class:`Logger`.  While it
is possible to create multiple instances of :class:`Logger`, the general usage
is to access the module instance, :data:`WeaverLogger`, which is instantiated
when the module is loaded.  This :class:`Logger` object can be used directly
through the :func:`debug`, :func:`warn`, and :func:`fatal` convenience
functions.

Logger class
------------

.. autoclass:: Logger 
    :members:

Logging functions
-----------------

.. autofunction:: enable 

    Alias for :meth:`_WeaverLogger.enable`.

.. autofunction:: disable

    Alias for :meth:`_WeaverLogger.disable`.

.. autofunction:: debug

    Alias for :meth:`_WeaverLogger.debug`.

.. autofunction:: fatal

    Alias for :meth:`_WeaverLogger.fatal`.

.. autofunction:: warn

    Alias for :meth:`_WeaverLogger.warn`.

Logging example
---------------

.. testsetup::

    from weaver.logger import D_ALL, _WeaverLogger, debug, warn, enable

    import os

    _WeaverLogger.stream = open(os.devnull, 'w+')

.. doctest::

    >>> warn('my-system', 'warning!')
    >>> debug('my-system', 'useful information (will not be recorded)')
    >>> enable(D_ALL)
    >>> debug('my-system', 'useful information (will be recorded)')
