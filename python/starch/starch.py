#!/usr/bin/env python2

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Starch """

from cStringIO  import StringIO
from os.path    import basename, realpath
from os.path    import join as path_join
from optparse   import OptionParser
from subprocess import Popen, PIPE
from tarfile    import REGTYPE, TarInfo, open as Tar
from time       import time
from tempfile   import NamedTemporaryFile
from shutil     import copyfile, copyfileobj

from ConfigParser import ConfigParser, NoSectionError, NoOptionError

import os
import sys

# Todo -------------------------------------------------------------------------

"""
1. Keep track of files added to avoid duplication (efficiency).
"""

# Global variables -------------------------------------------------------------

STARCH_AUTODETECT = True
STARCH_VERBOSE    = False

# Shell scripts ----------------------------------------------------------------

SFX_SH='''#!/bin/sh

SFX_FILE=$0
SFX_EXIT_STATUS=0

if [ -z $SFX_DIR ]; then
    basename=$(basename $SFX_FILE)
    SFX_DIR=$(hostname).$USER.$basename.dir

    if [ ! -z $CONDOR_SCRATCH_DIR ]; then
        SFX_DIR=$CONDOR_SCRATCH_DIR/$SFX_DIR
    elif [ ! -z $_CONDOR_SCRATCH_DIR ]; then
        SFX_DIR=$_CONDOR_SCRATCH_DIR/$SFX_DIR
    else
        SFX_DIR=/tmp/$SFX_DIR
    fi

    if [ ! -z $SFX_UNIQUE ] && [ $SFX_UNIQUE = 1 ]; then
        SFX_DIR=$(mktemp -d -u $SFX_DIR.XXXXXX)
    fi
fi

extract() {
    if [ ! -d $SFX_DIR -o $SFX_EXTRACT_FORCE -eq 1 ]; then
        mkdir -p $SFX_DIR 2> /dev/null
        archive=$(awk '/^__ARCHIVE__/ {print NR + 1; exit 0; }' $SFX_FILE)
        tail -n+$archive $SFX_FILE | tar xj -C $SFX_DIR
        SFX_EXIT_STATUS=$?
    fi
}

run() {
    $SFX_DIR/run.sh $@
    SFX_EXIT_STATUS=$?
}

if [ -z $SFX_EXTRACT_ONLY ]; then
    SFX_EXTRACT_ONLY=0
fi

if [ -z $SFX_EXTRACT_FORCE ]; then
    SFX_EXTRACT_FORCE=0
fi

if [ -z $SFX_KEEP ]; then
    SFX_KEEP=0
fi

if [ $SFX_EXTRACT_ONLY -eq 1 ]; then
    extract
    echo $SFX_DIR
    SFX_KEEP=1
else
    extract && run $@
fi

if [ $SFX_KEEP -ne 1 ]; then
    rm -fr $SFX_DIR
fi

exit $SFX_EXIT_STATUS

__ARCHIVE__
'''

RUN_SH = '''#!/bin/sh
SFX_DIR=$(dirname $0)

if [ -d $SFX_DIR/env ]; then
    for f in $SFX_DIR/env/*; do
        . $f
    done
fi

if [ -d $SFX_DIR/bin ]; then
    export PATH=$SFX_DIR/bin:$PATH
fi

if [ -d $SFX_DIR/lib ]; then
    export LD_LIBRARY_PATH=$SFX_DIR/lib:$LD_LIBRARY_PATH
fi
%s
'''

# Create SFX --------------------------------------------------------------------

def create_sfx(sfx_path, executables, libraries, data, environments, command):
    """ Create self-extracting executable

    Directory structure:
        bin         Executables
        lib         Libraries
        env         Environment Scripts (static)
        run.sh      Script that contains command
    """

    tmp_file = NamedTemporaryFile(delete = False)
    tmp_file.close()

    arc_path = tmp_file.name
    archive  = Tar(arc_path, 'w:bz2')

    debug('adding executables...')
    executables = find_executables(executables)
    for exe_path, real_path in executables:
        exe_name = basename(exe_path)
        exe_info = archive.gettarinfo(real_path, path_join('bin', exe_name))
        exe_info.mode = 0755

        debug('    adding executable: ' + exe_name)
        archive.addfile(exe_info, open(real_path))

    debug('adding libraries...')
    libraries = find_libraries(libraries, executables)
    for lib_path, real_path in libraries:
        lib_name = basename(lib_path)
        lib_info = archive.gettarinfo(real_path, path_join('lib', lib_name))

        debug('    adding library: ' + lib_name)
        archive.addfile(lib_info, open(real_path))
    
    debug('adding data...')
    for data_path, real_path in map(lambda s: s.split(':'), data):
        add_data_to_archive(archive, data_path, real_path)

    debug('adding environment scripts...')
    for env_path, real_path in find_files(environments, 'PWD'):
        env_name = basename(env_path)
        env_info = archive.gettarinfo(real_path, path_join('env', env_name))

        debug('    adding environment script: ' + env_path)
        archive.addfile(env_info, open(real_path))

    run_info = TarInfo('run.sh')
    run_info_data  = RUN_SH % command
    run_info.mode  = 0755
    run_info.mtime = time()
    run_info.size  = len(run_info_data)

    debug('adding run.sh...')
    archive.addfile(run_info, StringIO(run_info_data))
    archive.close()

    if os.path.exists(sfx_path):
        os.unlink(sfx_path)

    debug('creating sfx...')
    sfx_file = open(sfx_path, 'a+')
    copyfileobj(StringIO(SFX_SH), sfx_file)
    copyfileobj(open(arc_path, 'r'), sfx_file)
    sfx_file.close()

    debug('cleaning up...')
    os.chmod(sfx_path, 0755)
    os.unlink(arc_path)

def add_data_to_archive(archive, data_path, real_path):
    if os.path.isdir(real_path):
        for root, dirs, files in os.walk(real_path):
            for n in files + dirs:
                dp = os.path.join(root.replace(real_path, data_path), n)
                rp = os.path.join(root, n)
                add_data_to_archive(archive, dp, rp)
    else:
        data_info = archive.gettarinfo(realpath(real_path), data_path)
        debug('    adding data: ' + data_path)
        archive.addfile(data_info, open(real_path))

# Print utilities --------------------------------------------------------------

def debug(s):
    if STARCH_VERBOSE:
        print '[D]', s

def warn(s):
    print >>sys.stderr, '[W]', s

def error(s):
    print >>sys.stderr, '[E]', s
    sys.exit(1)

# Find file utilities ----------------------------------------------------------

def find_files(files, env_var):
    paths = ['.']
    if env_var in os.environ:
        paths.extend(os.environ[env_var].split(':'))

    for file in files:
        for path in paths:
            file_path = path_join(path, file)
            if os.path.exists(file_path):
                yield file_path, realpath(file_path)
                continue
    raise StopIteration


def find_executables(executables):
    exes = []
    for ep, rp in find_files(executables, 'PATH'):
        exes.append((ep, rp))
    return exes


def find_libraries(libraries, executables):
    libs = []

    for lp, rp in find_files(libraries, 'LD_LIBRARY_PATH'):
        libs.append((lp, rp))

    if STARCH_AUTODETECT:
        for exe_path, real_path in executables:
            p = Popen(['ldd', exe_path], stdout = PIPE)
            for line in p.communicate()[0].split('\n'):
                try:
                    lib_path = line.split('=>')[-1].strip().split()[0]
                    if os.path.exists(lib_path):
                        libs.append((lib_path, realpath(lib_path)))
                except:
                    pass

    return libs

# Configuration Parser ---------------------------------------------------------

class StarchConfigParser(ConfigParser):
    def get(self, section, name, default = None):
        try:
            return ConfigParser.get(self, section, name)
        except (NoSectionError, NoOptionError), e:
            return default

# Parse commandline options ----------------------------------------------------

def parse_command_line_options():
    global STARCH_VERBOSE
    global STARCH_AUTODETECT

    parser = OptionParser('%prog [options] <sfx_path>')

    parser.add_option('-c', dest = 'command', action = 'store',
        help = 'command to execute', metavar = 'cmd', default = '')
    parser.add_option('-x', dest = 'executables', action = 'append',
        help = 'add executable', metavar = 'exe', default = [])
    parser.add_option('-l', dest = 'libraries', action = 'append',
        help = 'add library', metavar = 'lib', default = [])
    parser.add_option('-d', dest = 'data', action = 'append',
        help = 'add data (new path:old path)', metavar = 'npath:opath', default = [])
    parser.add_option('-e', dest = 'environments', action = 'append',
        help = 'add environment script', metavar = 'env', default = [])
    parser.add_option('-C', dest = 'config', action = 'store',
        help = 'configuration file', metavar = 'cfg', default = None) 
    parser.add_option('-a', dest = 'autodetect', action = 'store_true',
        help = 'automatically detect library dependencies (default: True)', default = True)
    parser.add_option('-A', dest = 'autodetect', action = 'store_false',
        help = 'do not automatically detect library dependencies')
    parser.add_option('-v', dest = 'verbose', action = 'store_true',
        help = 'display verbose messages (default: False)', default = False)

    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
        sys.exit(1)

    STARCH_VERBOSE    = options.verbose
    STARCH_AUTODETECT = options.autodetect

    if options.config:
        config = StarchConfigParser()
        config.read(options.config)

        options.executables.extend(config.get('starch', 'executables', '').split())
        options.libraries.extend(config.get('starch', 'libraries', '').split())
        options.data.extend(config.get('starch', 'data', '').split())
        options.environments.extend(config.get('starch', 'environments', '').split())
        if not options.command:
            options.command = config.get('starch', 'command', '')

    if not options.executables:
        error('no executables specified')

    if not options.command:
        options.command = os.path.basename(options.executables[0]) + ' $@'
        warn('no command specified, so using: %s' % options.command)

    return args[0], options.executables, options.libraries, \
                    options.data, options.environments, options.command

# Main execution ---------------------------------------------------------------

if __name__ == '__main__':
    create_sfx(*parse_command_line_options())

# vim: sts=4 sw=4 ts=8 expandtab ft=python
