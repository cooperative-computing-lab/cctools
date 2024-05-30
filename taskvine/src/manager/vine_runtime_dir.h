/*
Copyright (C) 2023- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_manager.h"

/*
This module creates the directory hierarchy for logs and staging files.
This module is private to the manager and should not be invoked by the end user.
*/

#ifndef VINE_RUNTIME_DIR_H
#define VINE_RUNTIME_DIR_H

/* Create the runtime directory hierarchy.
 * Return NULL on failure.
 */
char *vine_runtime_directory_create();

#endif
