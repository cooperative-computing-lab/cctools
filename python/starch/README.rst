Starch - README
===============

Overview
--------

Starch is a script that creates standalone application archives in the form of
self-extracting executables (SFX).  Users may specify the command, executables,
libraries, data, and environment scripts associated with the application by
specifying the appropriate command line options or by using a configuration
file and passing it to starch.

Installation
------------

Use the standard Python setup.py script as so::

    $ ./setup.py install

Usage
-----

::

    Usage: starch.py [options] <sfx_path>

    Options:
        -h, --help      show this help message and exit
        -c cmd          command to execute
        -x exe          add executable
        -l lib          add library
        -d npath:opath  add data (new path:old path)
        -e env          add environment script
        -C cfg          configuration file
        -a              automatically detect library dependencies (default: True)
        -A              do not automatically detect library dependencies
        -v              display verbose messages (default: False)

Configuration File
------------------

Instead of specifying the command, executables, libraries, data, and
environment options on the command line, users may specify the settings in a
simple INI-style configuration file and pass that to starch.  A simple example
configuration file looks like so::
    
    [starch]
    executables = echo date hostname
    libraries   = libz.so
    data	= hosts.txt:/etc/hosts localtime:/etc/localtime images:/usr/share/pixmaps
    command	= echo $(hostname) $(date $@)

In this example, we specify three executables: ``echo``, ``date``, and
``hostname``.  We include the ``libz.so`` library and a collection of data
files.  In this case, we copy ``/etc/hosts`` to ``hosts.txt``,
``/etc/localtime`` to ``localtime``, and we recursively copy the
``/usr/share/pixmaps`` directory to ``images``.  Finally, we set our command to
simply print out the hostname and date.

Environmental Variables
-----------------------

The following environmental variables modify the behavior of the generated SFX
application.

- **SFX_DIR**:           Target directory where SFX will be extracted.
- **SFX_EXTRACT_ONLY**:  Only extract the SFX (by default it will be extracted and ran).
- **SFX_EXTRACT_FORCE**: Extract the SFX even if target directory exists.
- **SFX_KEEP**:          Keep extracted files (by default the files will be removed after execution).
- **SFX_UNIQUE**:        Use ``mktempd`` to generate unique SFX target directory.  This will prevent **SFX_KEEP** from working properly since the names will keep on changing.

Examples
--------

Package the date program::

   $ starch -x date date.sfx

Package the date program using a configuration file::

   $ starch -C date.cfg date.sfx

Run standalone date program::

   $ ./date.sfx +%s

Only extract the archive::

   $ env SFX_EXTRACT_ONLY=1 ./data.sfx

Run and keep extracted directory::
    
   $ env SFX_KEEP=1 ./data.sfx

Run with unique directory::

   $ env SFX_UNIQUE=1 ./data.sfx

Advanced example involving a complex shell command::

   $ starch -v -x tar -x rm extract_and_remove.sfx 'for f in $@; do if ! tar xvf $f; then exit 1; fi; done; rm $@'
   $ ./extract_and_remove.sfx *.tar.gz
