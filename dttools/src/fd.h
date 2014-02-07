/*
 * Copyright (C) 2013- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#ifndef FD_H
#define FD_H

/** Get the maximum number of open file descriptors to a process.
 *
 * @return n Maximum number of file descriptors.
 */
int fd_max (void);

/** Close all non-standard file descriptors.
 * @return status 0 on success, otherwise holds errno.
 */
int fd_nonstd_close (void);

/** Close fd and open "/dev/null".
 * @param fd File descriptor to reopen.
 * @param oflag Flags to open of "/dev/null".
 * @return status 0 on success, otherwise holds errno.
 */
int fd_null (int fd, int oflag);

#endif
