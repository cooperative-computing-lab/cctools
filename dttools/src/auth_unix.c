/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "auth.h"
#include "debug.h"
#include "xxmalloc.h"
#include "stringtools.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pwd.h>
#include <sys/stat.h>
#include <dirent.h>

static char challenge_dir[AUTH_LINE_MAX] = "/tmp";
static char alternate_passwd_file[AUTH_LINE_MAX] = "\0";
static int  challenge_timeout = 5;

void auth_unix_challenge_dir(const char *path)
{
	strcpy(challenge_dir, path);
}

void auth_unix_passwd_file(const char *path)
{
	strcpy(alternate_passwd_file, path);
}

void auth_unix_timeout_set( int secs )
{
	challenge_timeout = secs;
}


static int auth_unix_assert(struct link *link, time_t stoptime)
{
	int success = 0;
	FILE *file;
	char line[AUTH_LINE_MAX];

	debug(D_AUTH, "unix: waiting for challenge");
	if(link_readline(link, line, sizeof(line), stoptime)) {
		debug(D_AUTH, "unix: challenge is %s", line);
		file = fopen(line, "w");
		if(file) {
			fsync(fileno(file));
			fclose(file);
			debug(D_AUTH, "unix: issued response");
			if(auth_barrier(link, "yes\n", stoptime)) {
				debug(D_AUTH, "unix: response accepted");
				success = 1;
			} else {
				debug(D_AUTH, "unix: response rejected");
			}
		} else {
			debug(D_AUTH, "unix: could not meet challenge: %s", strerror(errno));
			link_putliteral(link, "no\n", stoptime);
		}
		unlink(line);
	} else {
		debug(D_AUTH, "unix: couldn't read challenge");
	}

	return success;
}

static void make_challenge_path(char *path)
{
	int result;

	while(1) {
		sprintf(path, "%s/challenge.%d.%d", challenge_dir, (int) getpid(), rand());
		result = unlink(path);
		if(result == 0) {
			break;
		} else {
			if(errno == ENOENT) {
				break;
			} else {
				debug(D_AUTH, "unix: %s is in use, still trying...",path);
				continue;
			}
		}
	}

	debug(D_AUTH, "unix: challenge path is %s", path);
}

#if CCTOOLS_OPSYS_DARWIN || CCTOOLS_OPSYS_FREEBSD || CCTOOLS_OPSYS_CYGWIN

/*
Darwin does not have fgetpwent in libc,
but all other platforms do.
*/

struct passwd *fgetpwent(FILE * file)
{
	static struct passwd p;
	static char line[1024];

	if(!fgets(line, sizeof(line), file))
		return 0;
	string_chomp(line);

	p.pw_name = strtok(line, ":");
	p.pw_passwd = strtok(0, ":");
	char *uid = strtok(0, ":");
	char *gid = strtok(0, ":");
	p.pw_gecos = strtok(0, ":");
	p.pw_dir = strtok(0, ":");
	p.pw_shell = strtok(0, ":");

	p.pw_uid = atoi(uid);
	p.pw_gid = atoi(gid);

	return &p;
}

#endif

static struct passwd *auth_get_passwd_from_uid(uid_t uid)
{
	if(alternate_passwd_file[0]) {
		struct passwd *p;
		FILE *file;

		file = fopen(alternate_passwd_file, "r");
		if(file) {
			while(1) {
				p = fgetpwent(file);
				if(!p)
					break;

				if(p->pw_uid == uid) {
					fclose(file);
					return p;
				}
			}
			fclose(file);
			return 0;
		} else {
			debug(D_AUTH, "unix: couldn't open %s: %s", alternate_passwd_file, strerror(errno));
			return 0;
		}
	} else {
		return getpwuid(uid);
	}
}

static int auth_unix_accept(struct link *link, char **subject, time_t stoptime)
{
	char path[AUTH_LINE_MAX];
	char line[AUTH_LINE_MAX];
	int success = 0;
	struct stat buf;
	struct passwd *p;

	debug(D_AUTH, "unix: generating challenge");
	make_challenge_path(path);
	link_putfstring(link, "%s\n", stoptime, path);

	debug(D_AUTH, "unix: waiting for response");
	if(link_readline(link, line, sizeof(line), stoptime)) {
		if(!strcmp(line, "yes")) {
			int file_exists = 0;
			int i=0;

			for(i=0;i<challenge_timeout;i++) {
				/*
				This is an odd hack, but invoking ls -la appears to help to force
				some NFS clients to refresh cached metadata.
				*/

				DIR *d = opendir(challenge_dir);
				if(d) {
				    closedir(d);
				}

				if(stat(path,&buf)==0) {
					file_exists = 1;
					break;
				} else {
					debug(D_AUTH,"unix: client claims success, but I don't see it yet...");
					sleep(1);
				}
			}

			if(file_exists) {
				debug(D_AUTH, "unix: got response");
				debug(D_AUTH, "unix: client is uid %d", buf.st_uid);
				p = auth_get_passwd_from_uid(buf.st_uid);
				if(p) {
					debug(D_AUTH, "unix: client is subject %s", p->pw_name);
					link_putliteral(link, "yes\n", stoptime);
					*subject = xxstrdup(p->pw_name);
					success = 1;
				} else {
					debug(D_AUTH, "unix: there is no user corresponding to uid %d", buf.st_uid);
					link_putliteral(link, "no\n", stoptime);
				}
			} else {
				debug(D_AUTH, "unix: client failed the challenge: %s", strerror(errno));
				link_putliteral(link, "no\n", stoptime);
			}
		} else {
			debug(D_AUTH, "unix: client declined the challenge");
		}
	}

	unlink(path);

	return success;
}

int auth_unix_register(void)
{
	debug(D_AUTH, "unix: registered");
	return auth_register("unix", auth_unix_assert, auth_unix_accept);
}
