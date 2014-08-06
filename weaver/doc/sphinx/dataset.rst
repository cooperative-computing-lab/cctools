:mod:`weaver.dataset` -- Weaver dataset module
==============================================

.. module:: weaver.dataset

DataSet class
-------------

.. autoclass:: DataSet

File DataSet classes
--------------------

.. autoclass:: FileList

**Example:**

.. testsetup::

    from weaver.dataset import FileList
    from weaver.nest import Nest
    from weaver.stack import WeaverNests
    import os
    import tempfile
    WeaverNests.push(Nest())

.. doctest::

    >>> # Create temporary text file with list of 'files'
    >>> tmpfile = tempfile.NamedTemporaryFile()
    >>> tmpfile.write('\n'.join(map(str, range(0, 10))))
    >>> tmpfile.flush()

    >>> # Create FileList using temporary file and print it
    >>> ds = FileList(tmpfile.name)
    >>> for f in ds: print(str(f).strip())
    0
    1
    2
    3
    4
    5
    6
    7
    8
    9

    >>> # Clean up
    >>> tmpfile.close()


.. autoclass:: Glob

.. testsetup::

    from weaver.dataset import Glob

.. doctest::

    >>> # Create Glob DataSet using temporary file and print it
    >>> ds = Glob('../../weaver/*.py')
    >>> for f in sorted(ds): print(str(f))
    ../../weaver/__init__.py
    ../../weaver/abstraction.py
    ../../weaver/compat.py
    ../../weaver/data.py
    ../../weaver/dataset.py
    ../../weaver/engine.py
    ../../weaver/function.py
    ../../weaver/logger.py
    ../../weaver/nest.py
    ../../weaver/options.py
    ../../weaver/script.py
    ../../weaver/stack.py
    ../../weaver/util.py

Query Class
-----------

.. autoclass:: Query

.. testsetup::

    from weaver.dataset import Glob, Query

.. doctest::

    >>> ds = Glob('../../weaver/*.py')
    >>> qy = Query(ds, ds.c.size >= 4096)
    >>> for f in sorted(qy): print(str(f))
    ../../weaver/abstraction.py
    ../../weaver/data.py
    ../../weaver/dataset.py
    ../../weaver/function.py
    ../../weaver/logger.py
    ../../weaver/nest.py
    ../../weaver/script.py
    ../../weaver/util.py
