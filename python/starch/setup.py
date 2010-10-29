#!/usr/bin/env python2

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver setup script """

from distutils         import log
from distutils.core    import setup
from distutils.cmd     import Command
from distutils.command import install_scripts

from subprocess import check_call

from stat import ST_MODE

import os
import sys


# Test Command -----------------------------------------------------------------

class TestCommand(Command):
    user_options = []
    description  = 'Run test suites'

    def initialize_options(self):
        self.cwd = os.getcwd()
        if not os.path.exists(os.path.join(self.cwd, 'build')):
            os.makedirs(os.path.join(self.cwd, 'build'))

    def finalize_options(self):
        pass

    def run(self):
        sfxs = [('date.sfx', '-x date'),
                ('example.sfx', '-C example.cfg')]

        for sfx_name, sfx_args in sfxs:
            logfile  = open(os.path.join(self.cwd, 'build', sfx_name + '.log'), 'a')
            sfx_path = os.path.join(self.cwd, 'build', sfx_name)
            command  = './starch.py %s %s' % (sfx_args, sfx_path)
            try:
                sys.stdout.write('Starching %s ... ' % sfx_name)
                sys.stdout.flush()
                check_call(command.split(), stderr = logfile, stdout = logfile)
                check_call([sfx_path], stderr = logfile, stdout = logfile)
                check_call(['env', 'SFX_KEEP=1', sfx_path], stderr = logfile, stdout = logfile)
                check_call(['env', 'SFX_KEEP=0', sfx_path], stderr = logfile, stdout = logfile)
                check_call(['env', 'SFX_UNIQUE=1', sfx_path], stderr = logfile, stdout = logfile)
            except Exception as e:
                sys.stdout.write('failure\n%s\n\n' % str(e))
                continue
            finally:
                logfile.close()
            sys.stdout.write('success\n')


# Script installer -------------------------------------------------------------

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
		    mode = ((os.stat(file)[ST_MODE]) | 0555) & 07777
		    log.info("changing mode of %s to %o", file, mode)
		    os.chmod(file, mode)
		    # Basically the same but remove .py extension
		    file_new = '.'.join(file.split('.')[:-1])
		    os.rename(file, file_new)
		    log.info("renaming %s to %s", file, file_new)


# Setup Configuration ----------------------------------------------------------

setup(
    name	 = 'Starch',
    version	 = '0.0.2',
    description	 = 'STandalone application ARCHiver',
    author	 = 'Peter Bui',
    author_email = 'pbui@cse.nd.edu',
    url		 = 'http://bitbucket.org/pbui/starch',
    scripts      = ['starch.py'],
    cmdclass	 = {'install_scripts': InstallScripts, 'test': TestCommand}
)

# vim: sts=4 sw=4 ts=8 expandtab ft=python
