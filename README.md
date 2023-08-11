# The Cooperative Computing Tools

## About

The Cooperative Computing Tools (cctools) is a software
package for enabling large scale distributed computing
on clusters, clouds, and grids.  It is used primarily for
attacking large scale problems in science and engineering.

You can read more about this software at [ReadTheDocs](https://cctools.readthedocs.io)
It is developed by members of the [Cooperative Computing Lab](https://ccl.cse.nd.edu)
at the [University of Notre Dame](https://www.nd.edu),
led by [Prof. Douglas Thain](https://dthain.github.io).
The file [CREDITS](CREDITS) lists the many people that have contributed to the software over the years.

## Quick Install Via Miniconda

The easiest way to install the binaries is via [Miniconda](https://docs.conda.io/en/latest/miniconda.html)

```
conda install -y -c conda-forge ndcctools
```

## Build From Source

To build from source and install in your home directory:

```
./configure --prefix ${HOME}/cctools
make
make install
```

Then run the executables out of your home directory like this:
```
export PATH=$HOME/cctools/bin:$PATH
makeflow -v
vine_status
```

## Copyright and License Notices

```
------------------------------------------------------------
This software package is
Copyright (c) 2003-2004 Douglas Thain
Copyright (c) 2005-2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
------------------------------------------------------------
This product includes software developed by and/or derived
from the Globus Project (http://www.globus.org/)
to which the U.S. Government retains certain rights.
------------------------------------------------------------
This product includes code derived from the RSA Data
Security, Inc. MD5 Message-Digest Algorithm.
------------------------------------------------------------
This product includes public domain code for the
SHA1 algorithm written by Peter Gutmann, David Ireland,
and A. M. Kutchman.
------------------------------------------------------------
This product includes the source code for the MT19937-64
Mersenne Twister pseudorandom number generator, written by 
Makoto Matsumoto and Takuji Nishimura.
```


