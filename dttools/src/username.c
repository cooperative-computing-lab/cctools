/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "username.h"

#include <pwd.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

int username_is_super()
{
	return !getuid();
}

int username_get(char *name)
{
	struct passwd *p;

	p = getpwuid(getuid());
	if (p) {
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
	if (p) {
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
	if (!p) {
		return 0;
	}

	uid = p->pw_uid;
	gid = p->pw_gid;

	if (geteuid() == uid)
		return 1;

	result = seteuid(0);
	if (result < 0)
		return 0;

	/* Drop the group id before the user id: once setuid() below gives up
	root, this process will typically no longer have permission to change
	its group id at all, so doing it in the other order would usually
	leave the process running as the target user but still holding its
	*original* group id. Both calls' return values are checked -- silently
	continuing after either fails could leave the process holding more
	privilege than the caller believes it dropped. */
	if (setgid(gid) < 0)
		return 0;

	if (setuid(uid) < 0)
		return 0;

	return 1;
}

/* vim: set noexpandtab tabstop=8: */
