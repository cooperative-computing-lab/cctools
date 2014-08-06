#!/usr/bin/env python

# Copyright (c) 2012- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver setup script """

from distutils         import log
from distutils.core    import setup
from distutils.cmd     import Command
from distutils.command import install_scripts

from glob import glob
from stat import ST_MODE

from subprocess import check_call

import os
import sys


# Test runner

class TestCommand(Command):
    user_options=[]
    def initialize_options(self):
        None
    def finalize_options(self):
        None
    """ Run test suites """
    def run(self):
        os.system('./tests.py')


# Script installer

class InstallScripts(install_scripts.install_scripts):
    def run(self):
        if not self.skip_build:
            self.run_command('build_scripts')
        self.outfiles = self.copy_tree(self.build_dir, self.install_dir)
        if os.name == 'posix':
            for file in self.get_outputs():
                if self.dry_run:
                    log.info("changing mode of %s", file)
                else:
                    mode = ((os.stat(file)[ST_MODE]) | 0o555) & 0o7777
                    log.info("changing mode of %s to %o", file, mode)
                    os.chmod(file, mode)
                    file_new = '.'.join(file.split('.')[:-1])
                    log.info("renaming %s to %s", file, file_new)
                    os.rename(file, file_new)


# Setup Configuration

setup(
    name         = 'Weaver',
    version      = '0.2.0',
    description  = 'High level distributed computing workflow framework in Python',
    author       = 'Peter Bui',
    author_email = 'pbui@cse.nd.edu',
    url          = 'http://bitbucket.org/pbui/weaver',
    packages     = ['weaver'],
    scripts      = ['weaver.py'],
    cmdclass     = {'test': TestCommand,
                    'install_scripts': InstallScripts}
)

# vim: sts=4 sw=4 ts=8 expandtab ft=python
