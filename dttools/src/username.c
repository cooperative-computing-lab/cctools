/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "username.h"

#include <pwd.h>
#include <unistd.h>
#include <sys/types.h>
#include <string.h>

int username_is_super()
{
	return !getuid();
}

int username_get(char *name)
{
	struct passwd *p;

	p = getpwuid(getuid());
	if(p) {
		strcpy(name, p->pw_name);
		return 1;
	} else {
		return 0;
	}
}

int username_home(char *dir)
{
	struct passwd *p;

	p = getpwuid(getuid());
	if(p) {
		strcpy(dir, p->pw_dir);
		return 1;
	} else {
		return 0;
	}
}

int username_set(const char *name)
{
	struct passwd *p;
	int result;
	uid_t uid;
	gid_t gid;

	p = getpwnam(name);
	if(!p) {
		return 0;
	}

	uid = p->pw_uid;
	gid = p->pw_gid;

	if(geteuid() == uid)
		return 1;

	result = seteuid(0);
	if(result < 0)
		return 0;

	setuid(uid);
	setgid(gid);

	return 1;
}

/* vim: set noexpandtab tabstop=8: */
