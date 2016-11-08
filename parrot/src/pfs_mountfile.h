/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_MOUNTFILE
#define PFS_MOUNTFILE

void pfs_mountfile_parse_file(const char *mountfile);
void pfs_mountfile_parse_string(const char *string);
int pfs_mountfile_parse_mode(const char *modestring);

#endif
