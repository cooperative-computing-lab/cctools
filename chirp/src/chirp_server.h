/* Copyright (C) 2015- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
*/

#include "username.h"

#include <limits.h>
#include <stdint.h>
#include <time.h>

extern char chirp_hostname[HOST_NAME_MAX];
uint64_t    chirp_minimum_space_free;
extern char chirp_owner[USERNAME_MAX];
extern char chirp_port[128];
extern char chirp_project_name[128];
time_t      chirp_starttime;
extern char chirp_transient_path[PATH_MAX];

/* vim: set noexpandtab tabstop=4: */
