# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

Apparently Macports works a little different than other package managers. The source code is installed into a destroot directory first, and then macports installs things from the destroot folder into /opt/local/... or whatever the root is for Macports. This is so that things can be easily deleted again during an uninstall. The Portfile in this folder allows everything to be installed in the destroot, and then in order to get things in the right places, it defines a post-destroot section of modifications within the destroot folder before things get fully installed.

If there is a problem, you should be uninstall:
sudo port uninstall ndcctools
update the following file with fixes:
/opt/local/var/macports/sources/rsync.macports.org/release/tarballs/ports/sysutils/ndcctools/Portfile
and reinstall
sudo port install ndcctools


Updates to the Portfile should be submitted as a port enhancement using the rules from the following website:
https://guide.macports.org/chunked/project.contributing.html
