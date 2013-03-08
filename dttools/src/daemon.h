/*
 * Copyright (C) 2011- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#ifndef DAEMON_H
#define DAEMON_H
/** Daemonize the current process.
 *
 * This involves creating a new process in a new session. The current
 * directory is changed to root, "/". The process umask is set to 0.
 * All open file descriptors are closed. stdin, stdout, and stderr
 * are opened to "/dev/null".
 *
 * daemonize writes the daemon process ID to pidfile before changing
 * directories.
 *
 * @param cdroot Change to root directory of filesystem.
 * @param pidfile PID file for daemon.
 *
 */
void daemonize (int cdroot, const char *pidfile);

#endif /* DAEMONIZE_H */
