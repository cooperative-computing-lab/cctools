/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef AUTH_GLOBUS_H
#define AUTH_GLOBUS_H

int auth_globus_register(void);
int auth_globus_has_delegated_credential(void);
void auth_globus_use_delegated_credential(int yesno);

#endif
