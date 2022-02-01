# Installing the Cooperative Computing Tools

## Overview

The Cooperative Computing Tools (**CCTools**) are a collection of programs that
enable large scale distributed computing on systems such as clusters, clouds,
and grids. These tools are commonly used in fields of science and engineering
that rely on large amounts of computing.


**CCTools** can be installed on Linux and Mac.



## Install from Conda

If you need a personal installation of **CCTools**, say for your laptop, or an
account in your campus cluster, the easiest way to install **CCTools** is using the
**conda** package manager. To check that you have conda install, in a terminal try:

```sh
$ conda list
```

If it fails, then you need to install either
[miniconda](https://docs.conda.io/projects/conda/en/latest/user-guide/install)
or [anaconda](https://docs.anaconda.com/anaconda/install). Miniconda is a
__light__ version of anaconda, and we recommend it as it is much faster to
install. We also recommend installing the versions for `Python 3.9`

!!! warning
    On Mac, the available from conda **CCTools** does not work with `Python 2.7` or with `perl`. For such case, please compile **CCTools** from [source](#from-source.md).

With the `conda` command available, create a new environment and install **CCTools** with:

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


## Install from Spack

Alternatively, you can install **CCTools** using the [spack.io](https://www.spack.org)
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

## Install From Source

To install from source, please follow the instructions [here](from-source.md).

You will only need to install **CCTools** from source if you want to track our
latest developments on our [github
repository](https://github.com/cooperative-computing-lab/cctools), to propose a bug fix, or if you need to add a driver support
to **parrot** not normally included in the **conda** installation, such as
CVMFS, or iRODS.

## Install from Binary Tarball

Binary packages are available for several operating systems. Please follow the instructions [here](from-source.md#install-from-binary-tarball).


## Testing Your Installation

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

You can test the availability of *CCTools** commands with:

```sh
$ makeflow --version

makeflow version X.Y.Z...
...(output trimmed)...
```

!!! warning
    Remember that for installations from source you need [swig](http://www.swig.org) at compile time, and to set
    the environment variables `PATH`, `PYTHONPATH` and `PERL5LIB` appropriately, as explained [here.](from-source.md#setting-your-environment)

    For **conda** and **spack** installation you should not need to manually
    set any of these variables, and in fact setting them may produce errors.


# License

The Cooperative Computing Tools are Copyright (C) 2003-2004 Douglas Thain and Copyright (C) 2005- The University of Notre Dame.  

All rights reserved.  

This software is distributed under the GNU General Public License.  

