# Installing the Cooperative Computing Tools

The Cooperative Computing Tools are Copyright (C) 2003-2004 Douglas Thain  
and Copyright (C) 2005- The University of Notre Dame.  
All rights reserved.  
This software is distributed under the GNU General Public License.  
See the file COPYING for details.

## Overview

The Cooperative Computing Tools (cctools) are a collection of programs that
enable large scale distributed computing on systems such as clusters, clouds,
and grids. These tools are commonly used in fields of science and engineering
that rely on large amounts of computing.

## Install from Binary Tarball

Binary packages are available for several operating systems at the [download
page](http://ccl.cse.nd.edu/software/download.shtml) Simply unpack the tarball
in any directory that you like, and then add the `bin` directory to your path.
For example, to install cctools for RHEL7 in your home directory:

`# replace VERSION with a version number found in the download page.
CCTOOLS_NAME=cctools-VERSION-x86_64-redhat7 cd $HOME wget
http://ccl.cse.nd.edu/software/files/${CCTOOLS_NAME}.tar.gz tar zxvf
${CCTOOLS_NAME}.tar.gz export PATH=$HOME/${CCTOOLS_NAME}/bin:$PATH `

## Install From Source Tarball

Download a source package from the [download
page](http://ccl.cse.nd.edu/software/download.shtml). And follow this recipe
while logged in as any ordinary user:

`# replace VERSION with a version number found in the download page.
CCTOOLS_NAME=cctools-VERSION-source wget
http://ccl.cse.nd.edu/software/files/${CCTOOLS_NAME}.tar.gz tar zxvf
${CCTOOLS_NAME}.tar.gz cd ${CCTOOLS_NAME} ./configure make make install export
PATH=${HOME}/cctools/bin:$PATH `

## Install From Git Repository

Or, you can directly build the latest version from our git repository:

`git clone https://github.com/cooperative-computing-lab/cctools cctools-source
cd cctools-source ./configure make make install export
PATH=${HOME}/cctools/bin:$PATH `

## Special Cases

The software will happily build and run without installing any external
packages. Optionally, the cctools will interoperate with a variety of external
packages for security and data access. To use these, you must download and
install them separately:

* [iRODS](http://www.irods.org) (version 4.0.3) 
* [Globus](http://www.globus.org) (version 5.0.3) 
* [FUSE](http://fuse.sourceforge.net)
* [Hadoop](http://hadoop.apache.org)
* [xRootD](http://xrootd.slac.stanford.edu)

Once the desired packages are correctly installed, unpack the cctools and then
issue a configure command that points to all of the other installations. Then,
make and install as before. For example:

`./configure --with-globus-path /usr/local/globus ... make make install export
PATH=${HOME}/cctools/bin:$PATH `

Building Parrot with support for the iRODS service requires some custom build
instructions, since Parrot requires access to some internals of iRODS that are
not usually available. To do this, first make a source build of iRODS in your
home directory:

`cd $HOME git clone https://github.com/irods/irods-source cd irods-source git
checkout 4.0.3 packaging/build.sh --run-in-place icommands `

Then, configure and build cctools relative to that installation:

`git clone https://github.com/cooperative-computing-lab/cctools cctools-source
cd cctools-source ./configure **--with-irods-path $HOME/irods-src ...** make
make install `

## Build with MPI

Building with MPI requires a valid MPI installation to be in your path.
Generally cctools compiles with both intel-ompi and MPICH. If you do not have
mpi installed already, we suggest downloading the latest MPICH from [the MPICH
website](https://www.mpich.org/). The latest known supporting version of MPICH
with cctools is MPICH-3.2.1. Simply build MPICH as is best for your
site/system, and then place the binaries in your path. We also suggest
configuring MPICH to use ` gcc` as the compiling software. For example: ` tar
-xvf MPICH-3.2.1.tar.gz cd MPICH-3.2.1 CC=gcc CXX=g++ ./configure
--prefix=/path/to/your/install/location make install ` Once MPI is in your
path, configure cctools to use MPI and then install. For example: ` cd
/path/to/cctools ./configure --with-mpicc-path=`which mpicc` make install `
Now, our tools will be MPI enabled, allowing you to run Makeflow as an MPI
job, as well as using both WorkQueue as MPI jobs, and submitting Makeflow and
WorkQueue together as a single MPI job.

## Build on Mac OSX

In order to build CCTools on Mac OS X you must first have the Xcode Command
Line Tools installed. For OS X 10.9 and later this can be done using the
following command:

`xcode-select --install `

Then, click "Install" in the window that appears on the screen. If the command
line tools are already installed, you will get an error and can proceed with
the instructions in the "Installing From Source" section above. For OS X
versions before 10.9, you will need to first install Xcode. Xcode can be found
in the App Store or on the installation disk.

