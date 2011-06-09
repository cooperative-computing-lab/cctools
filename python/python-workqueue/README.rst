python-workqueue - README
=========================

Overview
--------

This is a set of Python bindings for the `WorkQueue`_ Master/Worker Framework.

Dependencies
------------

Required:
    - `Python <http://www.python.org>`_ (2.4, 2.6, 2.7)
    - `CCTools <http://cse.nd.edu/~ccl/software/download.shtml>`_ 

Although other versions of Python may work, python-workqueue has only been
tested with Python 2.4, 2.6, and 2.7.

Installation
------------

Configure your local environment using ``configure.py``::

    $ ./configure.py -i <path_to_includes> -l <path_to_libs>

For example if you are on AFS, you can point the script to the public CCTools
installation::
    
    $ ./configure.py -i /afs/nd.edu/user37/ccl/software/cctools/include \
                     -l /afs/nd.edu/user37/ccl/software/cctools/lib

To build the Python module, use the ``build_ext`` command with the ``setup.py``
script::

    $ ./setup.py build_ext --inplace

To enable some debugging, build the module as so::
 
    $ ./setup.py build_ext --inplace --debug --undef NDEBUG

To install the Python module, use ``install`` command with the ``setup.py``
script::

    $ ./setup.py install --prefix <dest_dir>

Usage
-----

Please see the ``workqueue_example.py`` for an example of how to use the
library.  For the most part there is a 1-to-1 correspondence between the C
WorkQueue functions and the Python methods.

Execute the example as follows::

    $ python workqueue_example.py

Remember to start a worker.

.. _`WorkQueue`: http://www.cse.nd.edu/~ccl/software/workqueue
