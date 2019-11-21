# Installing CCTools from source

### Using the official released version...

Download a source package from the [download
page](http://ccl.cse.nd.edu/software/download). And follow this recipe while
logged in as any ordinary user:

```sh
$ tar zxf cctools-*-source.tar.gz
$ cd cctools-*-source
$ ./configure
$ make

# by default, CCTools is installed at ~/cctools. See below to change this default.
$ make install
```

### ...or from the git repository

Instead of installing from the source of the current released version, you can
can directly build the latest version from our git repository:

```sh
$ git clone https://github.com/cooperative-computing-lab/cctools cctools-source
$ cd cctools-source
$ ./configure
$ make
$ make install
```

!!! warning
    If you need to compile CCTools with **python** or **perl** support, you
    need [SWIG](http://www.swig.org) somewhere in your system. Follow the
    [instructions below.](#python-and-perl).

    SWIG is only needed for compilation, and it is no longer needed in the
    binaries produced.
    
!!! note
    After installation, you will need to set the environment variables `PATH`, `PYTHONPATH`, and `PERL5LIB` as explained [here.](#setting-your-environment)

!!! note
    If you need to install CCTools in a directory different from
    `${HOME}/cctools`, you can use `./configure --prefix /some/desired/dir`.
    Remember to export `PATH`, `PYTHONPATH`, and `PERL5LIB` as above, but changing
    `~/cctools` to the directory of your choosing.




## Setting Your Environment

If you installed **CCTools** from a source tarball, from github, or from a binary tarball, you will need to set some environment variables. This is not needed if you installed **CCTools** from conda or spack.

First determine the `python` and `perl` versions you are using:

```sh
# Note: your output may vary according to your python version.
$ python -c 'from sys import version_info; print("{}.{}".format(version_info.major, version_info.minor))'
3.7

# Note: your output may vary according to your perl version.
$ perl -e 'print("$^V\n")'
5.16.3
```

Now we update the environment variables with the values we found:

```sh
$ export PATH=~/cctools/bin:$PATH

# Change 3.7 to the python version you found above.
$ export PYTHONPATH=~/cctools/lib/python3.7/site-packages:${PYTHONPATH}

# Change 5.16.3 to the perl version you found above.
$ export PERL5LIB=~/cctools/lib/perl5/site_perl/5.16.3:${PERL5LIB}
```


## Special Cases

The software will happily build and run without installing any external
packages. Optionally, the CCTools will interoperate with a variety of external
packages for security and data access. To use these, you must download and
install them separately:

* [Python](https://www.python.org)
* [Perl](https://www.perl.org)
* [CVMFS](https://cvmfs.readthedocs.io/en/stable/cpt-quickstart.html)
* [FUSE](http://fuse.sourceforge.net)
* [iRODS](http://irods.org) (version 4.0.3) 
* [Globus](http://www.globus.org) (version 5.0.3) 
* [Hadoop](http://hadoop.apache.org)
* [xRootD](http://xrootd.slac.stanford.edu)

Once the desired packages are correctly installed, unpack the CCTools and then
issue a configure command that points to all of the other installations. Then,
make and install as before. For example:

```sh
$ ./configure --with-globus-path /usr/local/globus
$ make && make install
```

### python and perl

CCTools needs [SWIG](http://www.swig.org) during compilation to provide python
and perl support.  SWIG is available through conda, or as a package of many
linux distributions. Once SWIG is installed, the `configure` script should
automatically find it if the executable `swig` is somewhere in your `PATH`.

!!! note
    If `./configure` cannot find your SWIG installation, you can use a command line option as follows:

    `./configure --with-swig-path /path/to/swig`

### iRODS

Building Parrot with support for the iRODS service requires some custom build
instructions, since Parrot requires access to some internals of iRODS that are
not usually available. To do this, first make a source build of iRODS in your
home directory:

```sh
cd $HOME
git clone https://github.com/irods/irods-source
cd irods-source
git checkout 4.0.3
$ packaging/build.sh --run-in-place icommands
cd ..
```

Then, configure and build CCTools relative to that installation:

```
$ git clone https://github.com/cooperative-computing-lab/cctools cctools-source
$ cd cctools-source
$ ./configure --with-irods-path ../irods-src
$ make && make install
```


### MPI

Building with MPI requires a valid MPI installation to be in your path.
Generally CCTools compiles with both intel-ompi and MPICH. If you do not have
mpi installed already, we suggest downloading the latest MPICH from [the MPICH
website](https://www.mpich.org/). The latest known supporting version of MPICH
with CCTools is MPICH-3.2.1. Simply build MPICH as is best for your
site/system, and then place the binaries in your path. We also suggest
configuring MPICH to use ` gcc` as the compiling software. For example:

```sh
$ tar -xvf MPICH-3.2.1.tar.gz
$ cd MPICH-3.2.1 CC=gcc CXX=g++
$ ./configure
$ make && make install
```

Once MPI is in your path, configure CCTools to use MPI and then install. For example:

```sh
$ cd ~/cctools
$ ./configure --with-mpicc-path=`which mpicc`
$ make && install `
```

Now, our tools will be MPI enabled, allowing you to run Makeflow as an MPI job,
as well as using both WorkQueue as MPI jobs, and submitting Makeflow and
WorkQueue together as a single MPI job.

### Build on Mac OSX

In order to build CCTools on Mac OS X you must first have the Xcode Command
Line Tools installed. For OS X 10.9 and later this can be done using the
following command:

```sh
xcode-select --install
```

Then, click "Install" in the window that appears on the screen. If the command
line tools are already installed, you will get an error and can proceed with
the instructions in the "Installing From Source" section above. For OS X
versions before 10.9, you will need to first install Xcode. Xcode can be found
in the App Store or on the installation disk.


## Install from Binary Tarball

Binary packages are available for several operating systems at the [download
page](http://ccl.cse.nd.edu/software/download) Simply unpack the tarball
in any directory that you like, and then add the `bin` directory to your path.

For example, to install CCTools for RHEL7 / CentOS7 in your home directory in a directory
called `cctools`:

```sh
$ tar xvf cctools-*-centos7-x86_64.tar.gz
```

!!! note
    After unpackaging the tarball you will need to set the environment variables `PATH`, `PYTHONPATH`, and `PERL5LIB` as explained [here.](#setting-your-environment)




