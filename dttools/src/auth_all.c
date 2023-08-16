/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "auth_all.h"

#include <errno.h>
#include <string.h>

int auth_register_byname(const char *name)
{
	if(!strcmp(name, "kerberos"))
		return auth_kerberos_register();
	if(!strcmp(name, "globus"))
		return auth_globus_register();
	if(!strcmp(name, "unix"))
		return auth_unix_register();
	if(!strcmp(name, "hostname"))
		return auth_hostname_register();
	if(!strcmp(name, "address"))
		return auth_address_register();
	if(!strcmp(name, "ticket"))
		return auth_ticket_register();
	errno = EINVAL;
	return 0;
}

int auth_register_all(void)
{
	return auth_kerberos_register() + auth_globus_register() + auth_unix_register() + auth_ticket_register() + auth_hostname_register() + auth_address_register();
}

/* vim: set noexpandtab tabstop=8: */
