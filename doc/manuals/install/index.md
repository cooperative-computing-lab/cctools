# Installing the Cooperative Computing Tools

The **CCTools** software runs on Linux and Mac computers,
whether laptops, desktops, or large high performance clusters.
There are several ways to install:

- [Install From Conda](#install-from-conda) is **best for most users** on laptops or clusters.
- [Install From Spack](#install-from-spack) is recommended for high performance
clusters already using Spack.
- [Install From Github](#install-from-github) is recommended for developers
or those trying out the latest features.

## Install From Conda

Installing via the Conda package manager will give you a personal
installation of the software in your home directory.
You may already have Conda installed.  To check:

```sh
$ conda -V
```

This displays the version of conda currently installed. If it fails, then you should install [Miniconda](https://docs.conda.io/projects/conda/en/latest/user-guide/install).
Miniconda is a __light__ version of Anaconda, and we recommend it as it is much faster to install.
We also recommend installing the version for `Python 3.9`

Once Conda is installed, then install **CCTools** with:

```sh
# run this command once
$ conda create -n cctools-env -y -c conda-forge --strict-channel-priority python ndcctools

# run this command every time you want to use cctools
$ conda activate cctools-env

# run this command every time you want to update your version of cctools (after `activate` as above).
$ conda update -y -c conda-forge ndcctools
```

!!! note
    You could simply run the command `conda install -y -c conda-forge ndcctools` without creating
    a new environment. However, sometimes conda takes a long time to resolve
    all dependencies when adding new packages to an already existing
    environment.

And that's it! You can test your setup following the instructions [here](#testing-your-installation).

!!! warning
    On Mac, the available from conda **CCTools** does not work with `Python 2.7` or with `perl`. For such case, please compile **CCTools** from [source](#install-from-github).

## Install From Spack

Alternatively, you can install **CCTools** using the [spack.io](https://spack.io)
package manager. Spack will compile **CCTools** for you, and it is recommended
for HPC sites for which a conda installation is not available, or which have
special software stack requirements (such as specially compiled python versions).

First you need to check that the `spack` command is available. In a terminal type:

```sh
$ spack help
```

If this command fails, then please install spack following the instructions [here.](https://spack.io)

Once spack is installed, install **CCTools** with:

```sh
$ spack install cctools
```

To use **CCTools**, you need to load it into the spack environment with:

```sh
$ spack load cctools
```

You only need to do `spack install` once, but you will need `spack load
cctools` everytime you want to use **CCTools**.

Once this command finished, you can test your installation following the
instructions [here](#testing-your-installation).

## Install From Github

If you wish to install the latest version of **CCTools** to
try a new feature or develop new code then you can install from github.
This requires that you first install the necessary
software dependencies, and the easiest way to do this is to create
a new `cctools-dev` environment via Conda:

```sh
unset PYTHONPATH
conda create -y -n cctools-dev -c conda-forge --strict-channel-priority python=3 gcc_linux-64 gxx_linux-64 gdb m4 perl swig make zlib libopenssl-static openssl conda-pack
conda activate cctools-dev
```

Now that you are inside the `cctools-dev` environment, you can check out
the software repository and build it:

```sh
git clone git://github.com/cooperative-computing-lab/cctools.git cctools-src
cd cctools-src
./configure --with-base-dir $CONDA_PREFIX --prefix $CONDA_PREFIX
make
make install
```

The next time you log in, be sure to activate the environment again:

```sh
unset PYTHONPATH
conda activate cctools-dev
```

!!! note
    To use the installed software you will need to set the environment variables `PATH`, `PYTHONPATH`, and `PERL5LIB` as explained [here.](#setting-your-environment)

## Install From Source Tarball

In a similar way, you can download a source package from the 
[download page](http://ccl.cse.nd.edu/software/download).
Then unpack the tarball and compile:

```sh
$ tar zxf cctools-*-source.tar.gz
$ cd cctools-src
$ ./configure --prefix $HOME/cctools
$ make
$ make install
```

!!! note
    To use the installed software you will need to set the environment variables `PATH`, `PYTHONPATH`, and `PERL5LIB` as explained [here.](#setting-your-environment)

## Install From Binary Tarball

The **CCTools** software is pre-built for a small number of platforms.
Download a source package from the [download
page](http://ccl.cse.nd.edu/software/download). And follow this recipe while
logged in as any ordinary user:

```sh
$ tar zxf cctools-*-source.tar.gz
$ cd cctools-*-source
$ ./configure --prefix $HOME/cctools
$ make
$ make install
```

!!! note
    To use the installed software you will need to set the environment variables `PATH`, `PYTHONPATH`, and `PERL5LIB` as explained [here.](#setting-your-environment)

## Setting Your Environment

If you installed **CCTools** from github, source tarball, or from a binary tarball, you will need to set some environment variables.

First determine the `python` and `perl` versions you are using:

```sh
# Note: your output may vary according to your python version.
$ python -c 'from sys import version_info; print("{}.{}".format(version_info.major, version_info.minor))'
3.7

# Note: your output may vary according to your perl version.
$ perl -e 'print("$^V\n")'
5.16.3
```

Now update your environment variables with those versions:

```sh
$ export PATH=$HOME/cctools/bin:$PATH

# Change 3.7 to the python version you found above.
$ export PYTHONPATH=$HOME/cctools/lib/python3.7/site-packages:${PYTHONPATH}

# Change 5.16.3 to the perl version you found above.
$ export PERL5LIB=$HOME/cctools/lib/perl5/site_perl/5.16.3:${PERL5LIB}
```

## Testing Your Installation

You can test that the key programs are available with:

```sh
$ work_queue_worker --version
work_queue_worker version 7.4.3 FINAL (released 2022-02-03 16:26:52 +0000)

$ makeflow --version
makeflow version 7.4.3 FINAL (released 2022-02-03 16:26:52 +0000)
...
```

You can test that the python and perl modules are available with:

```sh
$ python -c 'import work_queue; print(work_queue.WORK_QUEUE_DEFAULT_PORT)'
9123

$ perl -MWork_Queue -e 'print("$Work_Queue::WORK_QUEUE_DEFAULT_PORT\n")'
9123
```

If the above commands fail, please make sure that you follow one (and only
one!) of the methods above. For example, if you are using a conda installation,
make sure that your PYTHONPATH is unset.


!!! warning
    Remember that for installations from source you need [swig](http://www.swig.org) at compile time, and to set
    the environment variables `PATH`, `PYTHONPATH` and `PERL5LIB` appropriately, as explained [here.](#setting-your-environment)

    For **conda** and **spack** installation you should not need to manually
    set any of these variables, and in fact setting them may produce errors.


## Special Cases

The software will happily build and run without installing any external
packages. Optionally, the CCTools will interoperate with a variety of external
packages for security, data access, and novel research methods.
To use these, you must download and
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

### SWIG

CCTools needs [SWIG](http://www.swig.org) during compilation to provide python
and perl support.  SWIG is available through conda, or as a package of many
linux distributions. Once SWIG is installed, the `configure` script should
automatically find it if the executable `swig` is somewhere in your `PATH`.

!!! note
    If `./configure` cannot find your SWIG installation, you can use a command line option as follows: `./configure --with-swig-path /path/to/swig`


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

```sh
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
$ ./configure --with-mpi-path=`which mpicc`
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

## License

The Cooperative Computing Tools are Copyright (C) 2003-2004 Douglas Thain and Copyright (C) 2022 The University of Notre Dame.    All rights reserved.   This software is distributed under the GNU General Public License.  
