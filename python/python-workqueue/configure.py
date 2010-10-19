#!/usr/bin/env python2

# Copyright (c) 2009- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Python-WorkQueue configure script """

from optparse     import OptionParser
from ConfigParser import RawConfigParser

import os
import sys


# Constants --------------------------------------------------------------------

INCLUDE_DIRS  = ['/usr/include', '/usr/local/include']
LIBRARY_DIRS  = ['/usr/lib', '/usr/local/lib']

if os.uname()[-1] == 'x86_64':
    LIBRARY_DIRS  = LIBRARY_DIRS + map(lambda s: s + '64', LIBRARY_DIRS)

WORKQUEUE_LIBS = ['dttools']
KERBEROS_LIBS  = ['krb5', 'k5crypto', 'crypto', 'com_err']


# Functions -------------------------------------------------------------------

def static_library(lib_dir, libname):
    return os.path.join(lib_dir, 'lib' + libname + '.a')


def shared_library(lib_dir, libname):
    return os.path.join(lib_dir, 'lib' + libname + '.so')


def find_library(library, lib_dirs):
    for lib_dir in lib_dirs:
	if os.path.exists(static_library(lib_dir, library)) or \
	   os.path.exists(shared_library(lib_dir, library)):
	    return True
    return False


def find_libraries(libraries, lib_dirs):
    llist = []

    for l in libraries:
	print 'Checking for', l, 'library...',
	if find_library(l, lib_dirs):
	    print 'ok'
	    llist.append(l)
	else:
	    print 'fail'

    return llist


def print_tab_list(plist):
    for item in plist:
	print '\t', item


def configure(args):
    usage  = 'usage: %prog [options]'
    desc   = 'Configure script to setup local environment for python-workqueue'
    parser = OptionParser(usage=usage, description=desc) 
    
    parser.add_option('-c', '--config_file', action='store', type='string',
		      metavar='file', default='setup.cfg',
		      help='Output setup configuration file')
    
    parser.add_option('-i', '--include_dir', action='append', type='string',
		      metavar='path', help='Path to header files')
    
    parser.add_option('-l', '--library_dir', action='append', type='string',
		      metavar='path', help='Path to library files')
    
    parser.add_option('-p', '--prefix', action='store', type='string',
		      metavar='path', help='Install prefix path')
    
    (options, args) = parser.parse_args(args)

    include_dirs = INCLUDE_DIRS
    library_dirs = LIBRARY_DIRS
    
    config = RawConfigParser()

    if options.include_dir:
	include_dirs.extend(options.include_dir)

    if options.library_dir:
	library_dirs.extend(options.library_dir)

    if options.prefix:
	config.add_section('install')
	config.set('install', 'prefix', options.prefix)
	print 'Using the following install prefix:', options.prefix 
	
	include_dirs.append(os.path.join(options.prefix, 'include'))
	library_dirs.append(os.path.join(options.prefix, 'lib'))

    config.add_section('build_ext')
    config.set('build_ext', 'include_dirs', os.pathsep.join(include_dirs))
    config.set('build_ext', 'library_dirs', os.pathsep.join(library_dirs))

    print 'Using the following include paths:'
    print_tab_list(include_dirs)
    print 'Using the following library paths:'
    print_tab_list(library_dirs)

    libraries = find_libraries(WORKQUEUE_LIBS + KERBEROS_LIBS, library_dirs)

    if libraries:
	config.set('build_ext', 'libraries', os.pathsep.join(libraries))
	print 'Using the following libraries:'
	print_tab_list(libraries)

    with open(options.config_file, 'w+') as config_file:
	config.write(config_file)
	print 'Configuration saved to:', options.config_file


# Main Execution ---------------------------------------------------------------

if __name__ == '__main__':
    configure(sys.argv)

# vim: sts=4 sw=4 ts=8 ft=python
