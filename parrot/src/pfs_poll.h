/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_POLL_H
#define PFS_POLL_H

#define PFS_POLL_READ 1
#define PFS_POLL_WRITE 2
#define PFS_POLL_EXCEPT 4

/* Remove all of the wakeup conditions */
void pfs_poll_init();

/* Block until a signal or wake condition */
void pfs_poll_sleep();

/* Wake when this fd becomes active */
void pfs_poll_wakeon( int fd, int which );

/* Wake after this interval */
void pfs_poll_wakein( struct timeval tv );

/* Cut short the next poll interval */
void pfs_poll_abort();

/* Remove all wakeups for this process */
void pfs_poll_clear( int pid );

/* Return a static string showing these poll flags */
char *pfs_poll_string( int flags );

#endif
