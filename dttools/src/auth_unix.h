/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef AUTH_UNIX_H
#define AUTH_UNIX_H

int auth_unix_register(void);

void auth_unix_challenge_dir( const char *path );
void auth_unix_passwd_file( const char *path );
void auth_unix_timeout_set( int secs );

#endif
