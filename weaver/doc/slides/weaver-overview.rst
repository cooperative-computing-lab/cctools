Weaver
======

:Topic:     **Weaver: Brief Overview**
:Author:    Peter Bui <pbui@cse.nd.edu>
:Date:      January 13, 2010

.. role:: raw-html(raw)
    :format: html

Problem
-------

Plethora of tools and software:

- **Abstractions**:      AllPairs, MapReduce, Wavefront
- **Storage Systems**:   Chirp/Multi, ROARS, HDFS

.. note::

    **(1)** How to integrate these components and **(2)** provide the user a
    high level interface?

Solution
--------

**Weaver**

.. image::  ../diagrams/stack.png
    :align:     right

- High level interface to `Makeflow`_
- Simplified programming model:

  **Functions** are applied to sets of files.  The pattern of execution is
  specified by particular **Abstractions**.

.. _`Makeflow`: http://cse.nd.edu/~ccl/software/makeflow/

System Overview
---------------

**Execution Pipeline**

.. image::  ../diagrams/system.png
    :align:     center

Map Example
-----------

.. code-block:: python

    from weaver.function import SimpleFunction
    from glob import glob

    ImgToPNG = SimpleFunction('convert', '', '.png')
    ImgToJPG = SimpleFunction('convert', '', '.jpg')

    xpms = glob('/usr/share/pixmaps/*.xpm')
    pngs = Map(ImgToPNG, xpms)
    jpgs = Map(ImgToJPG, pngs)

MapReduce Example
-----------------

.. code-block:: python

    from glob import glob

    def wc_mapper(key, value):
        for w in value.split():
            print '%s\t%d' % (w, 1)

    def wc_reducer(key, values):
        value = sum(int(v) for v in values)
        print '%s\t%d' % (key, value)

    MapReduce(  mapper  = wc_mapper,
                reducer = wc_reducer,
                input   = glob('weaver/*.py'),
                output  = 'wc.txt') 

Future Work
-----------

**Optimization**:

- Allow for use of *native* abstraction.
- Increase performance of `Makeflow`_ versions.

**Data Locality**:

- Implement `ChirpQueue`, `HDFSQueue` (?)
- Possibly modify `WorkQueue`

**Query**:

- Consider notion of queryable `datasets`
- Implement `select` type query for ROARS

