/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef ELFHEADER_H
#define ELFHEADER_H

#include <limits.h>

/** Get the interpreter (PT_INTERP) for the executable.
 *
 * @param fd The open file descriptor to the executable.
 * @param interp The current interpreter.
 * @return 0 on success; -1 + errno on failure.
 */
int elf_get_interp(int fd, char interp[PATH_MAX]);

/** Set the interpreter (PT_INTERP) for the executable.
 *
 * @param fd The open O_RDWR file descriptor to the executable.
 * @param interp The new interpreter.
 * @return 0 on success; -1 + errno on failure.
 */
int elf_set_interp(int fd, const char *interp);

#endif /* ELFHEADER_H */
