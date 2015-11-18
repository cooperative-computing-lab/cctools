/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef HOSTNAME_H
#define HOSTNAME_H

#include <stddef.h>

int getcanonical (const char *nodename, char *canonical, size_t l);

int getcanonicalhostname (char *canonical, size_t l);

int getshortname (char *shortname, size_t l);

#endif /* HOSTNAME_H */

/* vim: set noexpandtab tabstop=4: */
