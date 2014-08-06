:mod:`weaver.util` -- Weaver utility module
===========================================

.. module:: weaver.util

This module contains various utility classes and functions used throughout
Weaver.

Cloneable mixin
---------------

.. autoclass:: Cloneable
    :members:

**Example:**

.. testsetup::

    from weaver.util import Cloneable

.. doctest::

    >>> class C(Cloneable): pass        # Create class with Cloneable mixin
    >>> n0 = C()                        # Instantiate instance of class object
    >>> n0.a                            # n0 does not have member 'a'
    Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
    AttributeError: 'C' object has no attribute 'a'
    >>> n1 = n0.clone(**{'a': True})    # Clone n0 and add member 'a'
    >>> n0.a                            # n0 still does not have member 'a'
    Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
    AttributeError: 'C' object has no attribute 'a'
    >>> n1.a                            # n1 does have member 'a'
    True

Container class
---------------

.. autoclass:: Container

**Example:**

.. testsetup::

    from weaver.util import Container

.. doctest::

    >>> d = {'a': 0, 'b': 1}        # Make dictionary
    >>> c = Container(**d)          # Convert dictionary to object
    >>> c.a                         # Test object members
    0
    >>> c.b
    1
    >>> c = Container(foo=0, bar=1) # Make object with keyword arguments
    >>> c.foo
    0
    >>> c.bar
    1


Stash class
-----------

.. autoclass:: Stash
    :members:

.. testsetup::

    from weaver.util import Stash
    import os
    import shutil

.. doctest::

    >>> s = Stash()

    >>> # Check out Stash root directory and depth
    >>> s.root
    './_Stash'
    >>> sorted(os.listdir(s.root))
    ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F']
    >>> s.depth
    3

    >>> # Print next two file paths
    >>> print next(s)
    ./_Stash/0/0/0/00000000
    >>> print next(s)
    ./_Stash/0/0/0/00000001

    >>> # Cleanup
    >>> shutil.rmtree(s.root)


WeaverError class
-----------------

.. autoclass:: WeaverError

**Example:**

.. testsetup::

    from weaver.logger import D_UTIL
    from weaver.util import WeaverError

.. doctest::

    >>> try:
    ...     raise WeaverError(D_UTIL, 'WeaverError')
    ... except WeaverError as e:
    ...     print(e)
    utility: WeaverError


Argument parsing utilities
--------------------------

.. autofunction:: parse_object_list

**Example:**

.. testsetup::

    from weaver.util import parse_object_list

.. doctest::

    >>> parse_object_list(0)
    [0]
    >>> parse_object_list(range(10))  #doctest: +ELLIPSIS
    <generator object flatten at 0x...>
    >>> parse_object_list("string")
    ['string']
    >>> parse_object_list(("string", True, "string"))  #doctest: +ELLIPSIS
    <generator object flatten at 0x...>

.. autofunction:: parse_string_list

**Example:**

.. testsetup::

    from weaver.util import parse_string_list

.. doctest::

    >>> parse_string_list(range(10))
    <generator object <genexpr> at 0x...>

Iterable utilities
------------------

.. autofunction:: groups

**Example:**

.. testsetup::

    from weaver.util import groups

.. doctest::

    >>> tuple(groups(range(9), 2))
    ((0, 1), (2, 3), (4, 5), (6, 7), (8, None))
    >>> tuple(groups(range(10), 2))
    ((0, 1), (2, 3), (4, 5), (6, 7), (8, 9))

.. autofunction:: flatten

**Example:**

.. testsetup::

    from weaver.util import flatten

.. doctest::

    >>> tuple(flatten((True, True, (True, True), True, (True, True))))
    (True, True, True, True, True, True, True)

Logical utilities
-----------------

.. autofunction:: all_thunks

**Example:**

.. testsetup::

    from weaver.util import all_thunks

.. doctest::

    >>> all_thunks(False, [lambda i: True, lambda i: True])
    True
    >>> all_thunks(False, [lambda i: True, lambda i: i])
    False

.. autofunction:: any_thunks

**Example:**

.. testsetup::

    from weaver.util import any_thunks

.. doctest::

    >>> any_thunks(False, [lambda i: True, lambda i: i])
    True
    >>> any_thunks(False, [lambda i: i, lambda i: i])
    False

Miscellaneous utilities
-----------------------

.. autofunction:: get_username_or_id

**Example:**

.. testsetup::

    from weaver.util import get_username_or_id
    import os

.. doctest::

    >>> get_username_or_id() == os.environ['USER']
    True

Path utilities
--------------

.. autofunction:: find_executable

**Example:**

.. testsetup::

    from weaver.util import find_executable

.. doctest::

    >>> find_executable('asdfasdfasdf')
    Traceback (most recent call last):
        ...
    util.WeaverError: utility: could not find executable: asdfasdfasdf
    >>> find_executable('/bin/cat')
    '/bin/cat'
    >>> find_executable('cat')
    '/bin/cat'
    >>> find_executable('fsck', find_dirs='/sbin')
    '/sbin/fsck'
    >>> find_executable('fsck', find_dirs=['/opt/bin', '/sbin'])
    '/sbin/fsck'

.. autofunction:: normalize_path

**Example:**

.. testsetup::

    from weaver.util import normalize_path
    import os

.. doctest::

    >>> normalize_path('/bin/cat')
    '/bin/cat'
    >>> normalize_path('index.rst', ref_path=os.curdir)
    'index.rst'
    >>> normalize_path('index.rst', ref_path=os.path.join(os.curdir, '_build'))
    '../index.rst'

Type utilities
--------------

.. autofunction:: iterable

**Example:**

.. testsetup::

    from weaver.util import iterable

.. doctest::

    >>> iterable(0)
    False
    >>> iterable([])
    True
    >>> iterable(range(10))
    True

.. autofunction:: type_str

**Example:**

.. testsetup::

    from weaver.util import type_str, WeaverError

.. doctest::

    >>> type_str(WeaverError)
    'WeaverError'
    >>> e = WeaverError(D_UTIL, 'type_str')
    >>> type_str(e)
    'WeaverError'
    >>> type_str(e, full = True)
    'weaver.util.WeaverError'
    >>> type_str(dict)
    'dict'
    >>> type_str(dict(), full = True)
    'dict'
