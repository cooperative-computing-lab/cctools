/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MAKEFLOW_MOUNTS_H
#define MAKEFLOW_MOUNTS_H

#include "dag.h"

/* makeflow_mounts_parse_mountfile parses the mountfile and loads the info of each dependency into the dag structure d.
 * @param mountfile: the path of a mountfile
 * @param d: a dag structure
 * @return 0 on success, -1 on failure.
 */
int makeflow_mounts_parse_mountfile(const char *mountfile, struct dag *d);

/* makeflow_mounts_install installs all the dependencies specified in the mountfile.
 * @param d: a dag structure
 * @return 0 on success, -1 on failure.
 */

int makeflow_mounts_install(struct dag *d);

/* makeflow_mount_check_consistency checks the consistency between an entry from the mountfile and an entry from the log file
 * Both of these two entries share the same target field.
 * @param target: the target of a mount entry
 * @param source: the source of a mount entry specified in the mountfile
 * @param source_log: the source of the mount entry specified in the log file
 * @param cache_dir: the cache dir storing all the files specified in the mountfile
 * @param cache_name: the file name inside the cache dir
 * @return 0 on success, non-zero on failure.
 */
int makeflow_mount_check_consistency(const char *target, const char *source, const char *source_log, const char *cache_dir, const char *cache_name);

/* check_mount_target checks whether the validity of the target of each mount entry.
 * @param d: a dag structure
 * return 0 on success; return non-zero on failure.
 */
int makeflow_mount_check_target(struct dag *d);

#endif

/* vim: set noexpandtab tabstop=4: */
