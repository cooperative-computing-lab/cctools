/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <sys/types.h>

/*
 * Setup paranoia mode.  Launch an external watchdog process, share a process
 * table with it.
 * Returns the PID of the watchdog process, -1 on failure
 * 
 * If this fails, all subsequent pfs_paranoia_* commands will fail.
 */
int pfs_paranoia_setup(void);

/*
 * Returns the fd used to monitor the watchdog.
 * Returns -1 on failure.
 */
int pfs_paranoia_monitor_fd(void);

/*
 * Signal the watchdog process it's time to cleanup
 * Returns -1 on failure.
 */
int pfs_paranoia_cleanup(void);

/*
 * Add another process
 * Return non-zero on failure
 */
int pfs_paranoia_add_pid(pid_t);

/*
 * Delete a process from monitoring
 * Return non-zero on failure
 */
int pfs_paranoia_delete_pid(pid_t);

/*
 * Call when the payload process has been forked, as the child.
 */
int pfs_paranoia_payload(void);
