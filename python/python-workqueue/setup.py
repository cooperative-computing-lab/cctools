#!/usr/bin/env python2

# Copyright (c) 2009- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Python-WorkQueue setup script """

from distutils.core import setup, Extension
from distutils.command.build_ext import build_ext

from os import pathsep


# Custom Commands --------------------------------------------------------------

class BuildExt(build_ext):
    def finalize_options(self):
	build_ext.finalize_options(self)

	# Split libraries Python does not expand libraries by default
	if self.libraries:
	    self.libraries = self.libraries[0].split(pathsep)


# Configuration ----------------------------------------------------------------

setup(
    name	 = 'python-workqueue',
    version	 = '0.1.0',
    description	 = 'Python WorkQueue bindings',
    author	 = 'Peter Bui',
    author_email = 'pbui@cse.nd.edu',
    ext_modules  = [Extension('workqueue', sources = ['workqueue.c'])],
    cmdclass	 = {'build_ext' : BuildExt}
)

# vim: sts=4 sw=4 ts=8 ft=python
