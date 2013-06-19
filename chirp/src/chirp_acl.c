/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_acl.h"
#include "chirp_filesystem.h"
#include "chirp_group.h"
#include "chirp_protocol.h"
#include "chirp_ticket.h"

#include "debug.h"
#include "hash_table.h"
#include "stringtools.h"
#include "username.h"
#include "xxmalloc.h"

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>

static int read_only_mode = 0;
static const char *default_acl = 0;

extern const char *chirp_super_user;
extern const char *chirp_ticket_path;

void chirp_acl_force_readonly()
{
	read_only_mode = 1;
}

void chirp_acl_default(const char *d)
{
	default_acl = d;
}

static void make_acl_name(const char *filename, int get_parent, char *aclname)
{
	char tmp[CHIRP_PATH_MAX];
	sprintf(tmp, "%s/%s", filename, CHIRP_ACL_BASE_NAME);
	string_collapse_path(tmp, aclname, 1);
}

static int ticket_read(char *ticket_filename, struct chirp_ticket *ct)
{
	CHIRP_FILE *tf = cfs_fopen(ticket_filename, "r");
	if(!tf)
		return 0;
	char *b;
	size_t l;
	if(!cfs_freadall(tf, &b, &l)) {
		cfs_fclose(tf);
		return 0;
	}
	cfs_fclose(tf);

	int result = chirp_ticket_read(b, ct);

	free(b);

	return result;
}

static int ticket_write(const char *ticket_filename, struct chirp_ticket *ct)
{
	CHIRP_FILE *tf = cfs_fopen(ticket_filename, "w");
	if (!tf)
		return 0;

	char *str = chirp_ticket_tostring(ct);

	cfs_fprintf(tf, "%s", str);
	free(str);

	int result = cfs_ferror(tf);
	cfs_fclose(tf);
	if (result) {
		errno = EACCES;
		return -1;
	}
	return 0;
}

/*
do_chirp_acl_get returns the acl flags associated with a subject and directory.
If the subject has rights there, they are returned and errno is undefined.
If the directory exists, but the subject has no rights, returns zero with errno=0.
If the rights cannot be obtained, returns zero with errno set appropriately.
*/

static int do_chirp_acl_get(const char *dirname, const char *subject, int *totalflags)
{
	CHIRP_FILE *aclfile;
	char aclsubject[CHIRP_LINE_MAX];
	int aclflags;

	errno = 0;
	*totalflags = 0;

	/* if the subject is a ticket, then we need the rights we have for the
	 * directory along with the rights of the subject in that directory
	 */
	const char *digest;
	if(chirp_ticket_isticketsubject(subject, &digest)) {
		/* open the ticket file, read the public key */
		char ticket_filename[CHIRP_PATH_MAX];
		struct chirp_ticket ct;
		chirp_ticket_filename(ticket_filename, subject, NULL);
		if(!ticket_read(ticket_filename, &ct))
			return 0;
		if(!do_chirp_acl_get(dirname, ct.subject, totalflags)) {
			chirp_ticket_free(&ct);
			return 0;
		}
		size_t i;
		size_t longest = 0;
		int mask = 0;
		for(i = 0; i < ct.nrights; i++) {
			char safewhere[CHIRP_PATH_MAX];
			char where[CHIRP_PATH_MAX];
			sprintf(safewhere, "%s/%s", chirp_ticket_path, ct.rights[i].directory);
			string_collapse_path(safewhere, where, 1);

			if(strncmp(dirname, where, strlen(where)) == 0) {
				if(strlen(where) > longest) {
					longest = strlen(where);
					mask = chirp_acl_text_to_flags(ct.rights[i].acl);
				}
			}
		}
		*totalflags &= mask;
	} else {
		aclfile = chirp_acl_open(dirname);
		if(aclfile) {
			while(chirp_acl_read(aclfile, aclsubject, &aclflags)) {
				if(string_match(aclsubject, subject)) {
					*totalflags |= aclflags;
				} else if(!strncmp(aclsubject, "group:", 6)) {
					if(chirp_group_lookup(aclsubject, subject)) {
						*totalflags |= aclflags;
					}
				}
			}
			chirp_acl_close(aclfile);
		} else {
			return 0;
		}
	}

	if(read_only_mode) {
		*totalflags &= CHIRP_ACL_READ | CHIRP_ACL_LIST;
	}

	return 1;
}


int chirp_acl_check_dir(const char *dirname, const char *subject, int flags)
{
	int myflags;

	if(cfs->do_acl_check()==0) return 1;

	if(!do_chirp_acl_get(dirname, subject, &myflags)) {
		/*
		Applications are very sensitive to this error condition.
		A missing ACL file indicates permission denied,
		but a missing directory entirely indicates no such entry.
		*/

		if(cfs_isdir(dirname)) {
			errno = EACCES;
		} else {
			errno = ENOENT;
		}
		return 0;
	}

	/* The superuser can implicitly list and admin */

	if(chirp_super_user && !strcmp(subject, chirp_super_user)) {
		myflags |= CHIRP_ACL_LIST | CHIRP_ACL_ADMIN;
	}

	if((flags & myflags) == flags) {
		return 1;
	} else {
		errno = EACCES;
		return 0;
	}
}

static int do_chirp_acl_check(const char *filename, const char *subject, int flags, int follow_links)
{
	char linkname[CHIRP_PATH_MAX];
	char temp[CHIRP_PATH_MAX];
	char dirname[CHIRP_PATH_MAX];

	if(cfs->do_acl_check()==0) return 1;

	/*
	   Symbolic links require special handling.
	   If requested, follow the link and look for rights in that directory.
	 */

	if(follow_links && flags != CHIRP_ACL_DELETE) {
		int length = cfs->readlink(filename, linkname, sizeof(linkname));
		if(length > 0) {
			linkname[length] = 0;

			/* If the link is relative, construct a full path */

			if(linkname[0] != '/') {
				sprintf(temp, "%s/../%s", filename, linkname);
				string_collapse_path(temp, linkname, 1);
			}

			/* Use the linkname now to look up the ACL */

			debug(D_DEBUG, "symlink %s points to %s", filename, linkname);
			filename = linkname;
		}
	}

	/*
	   If the file being checked is an ACL file,
	   then it may be written with the admin flag, but never deleted.
	 */

	if(!strcmp(string_back(filename, CHIRP_ACL_BASE_LENGTH), CHIRP_ACL_BASE_NAME)) {
		if(flags & CHIRP_ACL_DELETE) {
			errno = EACCES;
			return 0;
		}
		if(flags & CHIRP_ACL_WRITE) {
			flags &= ~CHIRP_ACL_WRITE;
			flags |= CHIRP_ACL_ADMIN;
		}
	}

	/* Now get the name of the directory containing the file */

	string_collapse_path(filename, temp, 1);
	if(!cfs_isdir(temp))
		string_dirname(temp, dirname);
	else
		strcpy(dirname, temp);

	/* Perform the permissions check on that directory. */

	return chirp_acl_check_dir(dirname, subject, flags);
}

int chirp_acl_check(const char *filename, const char *subject, int flags)
{
	return do_chirp_acl_check(filename, subject, flags, 1);
}

int chirp_acl_check_link(const char *filename, const char *subject, int flags)
{
	return do_chirp_acl_check(filename, subject, flags, 0);
}

char *chirp_acl_ticket_callback (const char *digest)
{
	char path[CHIRP_PATH_MAX];
	struct chirp_ticket ct;

	chirp_ticket_filename(path, NULL, digest);

	if (ticket_read(path, &ct)) {
		char *ticket = xxstrdup(ct.ticket);
		chirp_ticket_free(&ct);
		return ticket;
	}

	return NULL;
}

int chirp_acl_ticket_delete(const char *ticket_dir, const char *subject, const char *ticket_subject)
{
	char ticket_filename[CHIRP_PATH_MAX];
	const char *digest;
	char *esubject;
	struct chirp_ticket ct;
	int status = 0;

	if(!chirp_ticket_isticketsubject(ticket_subject, &digest)) {
		errno = EINVAL;
		return -1;
	}
	if(!chirp_acl_whoami( subject, &esubject))
		return -1;

	chirp_ticket_filename(ticket_filename, ticket_subject, NULL);

	if(!ticket_read(ticket_filename, &ct)) {
		free(esubject);
		return -1;
	}

	if(strcmp(esubject, ct.subject) == 0 || strcmp(chirp_super_user, subject) == 0) {
		status = cfs->unlink(ticket_filename);
	} else {
		errno = EACCES;
		status = -1;
	}
	chirp_ticket_free(&ct);
	free(esubject);
	return status;
}

int chirp_acl_ticket_get(const char *ticket_dir, const char *subject, const char *ticket_subject, char **ticket_esubject, char **ticket, time_t * ticket_expiration, char ***ticket_rights)
{
	char *esubject;
	if(!chirp_acl_whoami( subject, &esubject))
		return -1;

	const char *digest;
	if(!chirp_ticket_isticketsubject(ticket_subject, &digest)) {
		errno = EINVAL;
		return -1;
	}

	struct chirp_ticket ct;
	char ticket_filename[CHIRP_PATH_MAX];
	chirp_ticket_filename(ticket_filename, ticket_subject, NULL);
	if(!ticket_read(ticket_filename, &ct)) {
		free(esubject);
		errno = EINVAL;
		return -1;
	}
	if(strcmp(ct.subject, subject) == 0 || strcmp(subject, chirp_super_user) == 0) {
		*ticket_esubject = xxstrdup(ct.subject);
		*ticket = xxstrdup(ct.ticket);

		time_t now;
		time(&now);
		now = mktime(gmtime(&now));	/* convert to UTC */
		*ticket_expiration = ct.expiration - now;

		size_t n;
		*ticket_rights = (char **) xxmalloc(sizeof(char *) * 2 * (ct.nrights + 1));
		for(n = 0; n < ct.nrights; n++) {
			(*ticket_rights)[n * 2 + 0] = xxstrdup(ct.rights[n].directory);
			(*ticket_rights)[n * 2 + 1] = xxstrdup(ct.rights[n].acl);
		}
		(*ticket_rights)[n * 2 + 0] = NULL;
		(*ticket_rights)[n * 2 + 1] = NULL;

		chirp_ticket_free(&ct);
		free(esubject);
		return 0;
	} else {
		chirp_ticket_free(&ct);
		free(esubject);
		errno = EACCES;
		return -1;
	}
}

int chirp_acl_ticket_list(const char *ticket_dir, const char *subject, char ***ticket_subjects)
{
	size_t n = 0;
	*ticket_subjects = NULL;

	struct chirp_dirent *d;
	struct chirp_dir *dir;

	dir = cfs->opendir(ticket_dir);
	if(dir == NULL)
		return -1;

	while((d = cfs->readdir(dir))) {
		if(strcmp(d->name, ".") == 0 || strcmp(d->name, "..") == 0)
			continue;
		char path[CHIRP_PATH_MAX];
		sprintf(path, "%s/%s", ticket_dir, d->name);
		const char *digest;
		if(chirp_ticket_isticketfilename(d->name, &digest)) {
			struct chirp_ticket ct;
			if(!ticket_read(path, &ct))
				continue;	/* expired? */
			if(strcmp(subject, ct.subject) == 0 || strcmp(subject, "all") == 0) {
				char ticket_subject[CHIRP_PATH_MAX];
				n = n + 1;
				*ticket_subjects = (char **) xxrealloc(*ticket_subjects, (n + 1) * sizeof(char *));
				chirp_ticket_subject(ticket_subject, d->name);
				(*ticket_subjects)[n - 1] = xxstrdup(ticket_subject);
				(*ticket_subjects)[n] = NULL;
			}
			chirp_ticket_free(&ct);
		}
	}
	cfs->closedir(dir);

	return 0;
}

int chirp_acl_gctickets(const char *ticket_dir)
{
	struct chirp_dir *dir;
	struct chirp_dirent *d;

	dir = cfs->opendir(ticket_dir);
	if(!dir) {
		return -1;
	}
	while((d = cfs->readdir(dir))) {
		const char *digest;
		if(chirp_ticket_isticketfilename(d->name, &digest)) {
			/* open the ticket file, read the public key */
			struct chirp_ticket ct;
			char path[CHIRP_PATH_MAX];
			sprintf(path, "%s/%s", ticket_dir, d->name);
			if(ticket_read(path, &ct)) {
				chirp_ticket_free(&ct);
				continue;
			}
			debug(D_CHIRP, "ticket %s expired (or corrupt), garbage collecting", digest);
			cfs->unlink(path);
		}
	}
	cfs->closedir(dir);
	return 0;
}

int chirp_acl_ticket_create(const char *ticket_dir, const char *subject, const char *newsubject, const char *ticket, const char *duration)
{
	time_t now;		/*, delta; */
	time_t offset = (time_t) strtoul(duration, NULL, 10);
	const char *digest;
	char ticket_subject[CHIRP_PATH_MAX];
	char ticket_filename[CHIRP_PATH_MAX];
	char expiration[128];

	now = time(NULL);
	now = mktime(gmtime(&now));	/* convert to UTC */
	sprintf(expiration, "%lu", (unsigned long) (now + offset));

	/* Note about tickets making tickets:
	 * A ticket created by a ticket authenticated user has the same effective
	 * subject (see the ticket_register RPC in chirp_server.c). Also, the
	 * expiration time is less than or equal to the expiration time of the
	 * ticket used to authenticate.
	 */
	if(chirp_ticket_isticketsubject(subject, &digest)) {
		struct chirp_ticket ct;
		chirp_ticket_filename(ticket_filename, subject, NULL);
		if(!ticket_read(ticket_filename, &ct))
			return -1;
		if(ct.expiration < now + offset) {
			sprintf(expiration, "%lu", (unsigned long) ct.expiration);
		}
		chirp_ticket_free(&ct);
	}

	if(!cfs_isdir(ticket_dir)) {
		errno = ENOTDIR;
		return -1;
	}

	chirp_ticket_name(ticket, ticket_subject, ticket_filename);

	CHIRP_FILE *f = cfs_fopen(ticket_filename, "w");
	if(!f) {
		errno = EACCES;
		return -1;
	}
	cfs_fprintf(f, "subject \"%s\"\n", newsubject);
	cfs_fprintf(f, "expiration \"%s\"\n", expiration);
	cfs_fprintf(f, "ticket \"%s\"\n", ticket);
	cfs_fprintf(f, "rights \"/\" \"n\"\n");

	cfs_fflush(f);
	int result = cfs_ferror(f);
	if(result) {
		errno = EACCES;
		return -1;
	}
	cfs_fclose(f);
	return 0;
}

int chirp_acl_ticket_modify(const char *ticket_dir, const char *subject, const char *ticket_subject, const char *path, int flags)
{
	char ticket_filename[CHIRP_PATH_MAX];
	const char *digest;
	char *esubject;
	struct chirp_ticket ct;
	int status = 0;

	if(!chirp_ticket_isticketsubject(ticket_subject, &digest)) {
		errno = EINVAL;
		return -1;
	}
	/* Note about tickets making tickets:
	 * We check whether the ticket has the rights associated with the mask in
	 * the next line. So, a ticket can only make a ticket with rights it
	 * already has.
	 */
	if(!chirp_acl_check_dir(path, subject, flags))
		return -1;	/* you don't have the rights for the mask */
	if(!chirp_acl_whoami(subject, &esubject))
		return -1;

    chirp_ticket_filename(ticket_filename, ticket_subject, NULL);

	if(!ticket_read(ticket_filename, &ct)) {
		free(esubject);
		return -1;
	}

	if(strcmp(esubject, ct.subject) == 0 || strcmp(chirp_super_user, subject) == 0) {
		size_t n;
		int replaced = 0;
		for(n = 0; n < ct.nrights; n++) {
			char safewhere[CHIRP_PATH_MAX];
			char where[CHIRP_PATH_MAX];
			sprintf(safewhere, "%s/%s", ticket_dir, ct.rights[n].directory);
			string_collapse_path(safewhere, where, 1);

			if(strcmp(where, path) == 0) {
				free(ct.rights[n].acl);
				ct.rights[n].acl = xxstrdup(chirp_acl_flags_to_text(flags));	/* replace old acl mask */
				replaced = 1;
			}
		}
		if(!replaced) {
			assert(strlen(path) >= strlen(ticket_dir));
			ct.rights = xxrealloc(ct.rights, sizeof(*ct.rights) * (++ct.nrights) + 1);
			char directory[CHIRP_PATH_MAX];
			char collapsed_directory[CHIRP_PATH_MAX];
			sprintf(directory, "/%s", path + strlen(ticket_dir));
			string_collapse_path(directory, collapsed_directory, 1);
			ct.rights[ct.nrights - 1].directory = xxstrdup(collapsed_directory);
			ct.rights[ct.nrights - 1].acl = xxstrdup(chirp_acl_flags_to_text(flags));
		}
		status = ticket_write(ticket_filename, &ct);
	} else {
		errno = EACCES;
		status = -1;
	}
	chirp_ticket_free(&ct);
	free(esubject);
	return status;
}

int chirp_acl_whoami(const char *subject, char **esubject)
{
	const char *digest;
	if(chirp_ticket_isticketsubject(subject, &digest)) {
		/* open the ticket file */
		struct chirp_ticket ct;
		char ticket_filename[CHIRP_PATH_MAX];

		chirp_ticket_filename(ticket_filename, subject, NULL);
		if(!ticket_read(ticket_filename, &ct))
			return 0;
		*esubject = xxstrdup(ct.subject);
		chirp_ticket_free(&ct);
		return 1;
	} else {
		*esubject = xxstrdup(subject);
		return 1;
	}
}

int chirp_acl_set(const char *dirname, const char *subject, int flags, int reset_acl)
{
	char aclname[CHIRP_PATH_MAX];
	char newaclname[CHIRP_PATH_MAX];
	char aclsubject[CHIRP_LINE_MAX];
	int aclflags;
	CHIRP_FILE *aclfile, *newaclfile;
	int result;
	int replaced_acl_entry = 0;

	if(!cfs_isdir(dirname)) {
		errno = ENOTDIR;
		return -1;
	}

	sprintf(aclname, "%s/%s", dirname, CHIRP_ACL_BASE_NAME);
	sprintf(newaclname, "%s/%s.%d", dirname, CHIRP_ACL_BASE_NAME, (int) getpid());

	if(reset_acl) {
		aclfile = cfs_fopen("/dev/null", "r");
	} else {
		aclfile = cfs_fopen(aclname, "r");

		/* If the acl never existed, then we can simply create it. */
		if(!aclfile && errno == ENOENT) {
			if(default_acl) {
				aclfile = cfs_fopen(default_acl, "r");
			} else {
				aclfile = cfs_fopen("/dev/null", "r");	/* use local... */
			}
		}
	}

	if(!aclfile) {
		errno = EACCES;
		return -1;
	}

	replaced_acl_entry = 0;

	newaclfile = cfs_fopen(newaclname, "w");
	if(!newaclfile) {
		cfs_fclose(aclfile);
		errno = EACCES;
		return -1;
	}

	while(chirp_acl_read(aclfile, aclsubject, &aclflags)) {
		if(!strcmp(subject, aclsubject)) {
			aclflags = flags;
			replaced_acl_entry = 1;
		}
		if(aclflags != 0) {
			cfs_fprintf(newaclfile, "%s %s\n", aclsubject, chirp_acl_flags_to_text(aclflags));
		}
	}
	cfs_fclose(aclfile);

	if(!replaced_acl_entry) {
		cfs_fprintf(newaclfile, "%s %s\n", subject, chirp_acl_flags_to_text(flags));
	}

	/* Need to force a write in order to get response from ferror */

	cfs_fflush(newaclfile);
	result = cfs_ferror(newaclfile);
	cfs_fclose(newaclfile);

	if(result) {
		errno = EACCES;
		result = -1;
	} else {
		result = cfs->rename(newaclname, aclname);
		if(result < 0) {
			cfs->unlink(newaclname);
			errno = EACCES;
			result = -1;
		}
	}

	return result;
}

CHIRP_FILE *chirp_acl_open(const char *dirname)
{
	char aclname[CHIRP_PATH_MAX];
	CHIRP_FILE *file;

	make_acl_name(dirname, 0, aclname);
	file = cfs_fopen(aclname, "r");
	if(!file && default_acl)
		file = cfs_fopen(default_acl, "r");
	return file;
}

int chirp_acl_read(CHIRP_FILE * aclfile, char *subject, int *flags)
{
	char acl[CHIRP_LINE_MAX];
	char tmp[CHIRP_LINE_MAX];

	while(cfs_fgets(acl, sizeof(acl), aclfile)) {
		if(sscanf(acl, "%[^ ] %[rwldpvax()]", subject, tmp) == 2) {
			*flags = chirp_acl_text_to_flags(tmp);
			return 1;
		} else {
			continue;
		}
	}

	return 0;
}

void chirp_acl_close(CHIRP_FILE * aclfile)
{
	cfs_fclose(aclfile);
}


const char *chirp_acl_flags_to_text(int flags)
{
	static char text[20];

	text[0] = 0;

	if(flags & CHIRP_ACL_READ)
		strcat(text, "r");
	if(flags & CHIRP_ACL_WRITE)
		strcat(text, "w");
	if(flags & CHIRP_ACL_LIST)
		strcat(text, "l");
	if(flags & CHIRP_ACL_DELETE)
		strcat(text, "d");
	if(flags & CHIRP_ACL_PUT)
		strcat(text, "p");
	if(flags & CHIRP_ACL_ADMIN)
		strcat(text, "a");
	if(flags & CHIRP_ACL_EXECUTE)
		strcat(text, "x");
	if(flags & CHIRP_ACL_RESERVE) {
		strcat(text, "v");
		strcat(text, "(");
		if(flags & CHIRP_ACL_RESERVE_READ)
			strcat(text, "r");
		if(flags & CHIRP_ACL_RESERVE_WRITE)
			strcat(text, "w");
		if(flags & CHIRP_ACL_RESERVE_LIST)
			strcat(text, "l");
		if(flags & CHIRP_ACL_RESERVE_DELETE)
			strcat(text, "d");
		if(flags & CHIRP_ACL_RESERVE_PUT)
			strcat(text, "p");
		if(flags & CHIRP_ACL_RESERVE_RESERVE)
			strcat(text, "v");
		if(flags & CHIRP_ACL_RESERVE_ADMIN)
			strcat(text, "a");
		if(flags & CHIRP_ACL_RESERVE_EXECUTE)
			strcat(text, "x");
		strcat(text, ")");
	}

	if(text[0] == 0) {
		strcpy(text, "n");
	}

	return text;
}

int chirp_acl_text_to_flags(const char *t)
{
	int flags = 0;

	while(*t) {
		if(*t == 'r')
			flags |= CHIRP_ACL_READ;
		if(*t == 'w')
			flags |= CHIRP_ACL_WRITE;
		if(*t == 'l')
			flags |= CHIRP_ACL_LIST;
		if(*t == 'd')
			flags |= CHIRP_ACL_DELETE;
		if(*t == 'p')
			flags |= CHIRP_ACL_PUT;
		if(*t == 'a')
			flags |= CHIRP_ACL_ADMIN;
		if(*t == 'x')
			flags |= CHIRP_ACL_EXECUTE;
		if(*t == 'v') {
			flags |= CHIRP_ACL_RESERVE;
			if(t[1] == '(') {
				t += 2;
				while(*t && *t != ')') {
					if(*t == 'r')
						flags |= CHIRP_ACL_RESERVE_READ;
					if(*t == 'w')
						flags |= CHIRP_ACL_RESERVE_WRITE;
					if(*t == 'l')
						flags |= CHIRP_ACL_RESERVE_LIST;
					if(*t == 'd')
						flags |= CHIRP_ACL_RESERVE_DELETE;
					if(*t == 'p')
						flags |= CHIRP_ACL_RESERVE_PUT;
					if(*t == 'v')
						flags |= CHIRP_ACL_RESERVE_RESERVE;
					if(*t == 'a')
						flags |= CHIRP_ACL_RESERVE_ADMIN;
					if(*t == 'x')
						flags |= CHIRP_ACL_RESERVE_EXECUTE;
					++t;
				}
			}
		}
		++t;
	}

	return flags;
}

int chirp_acl_from_access_flags(int flags)
{
	int acl = 0;
	if(flags & R_OK)
		acl |= CHIRP_ACL_READ;
	if(flags & W_OK)
		acl |= CHIRP_ACL_WRITE;
	if(flags & X_OK)
		acl |= CHIRP_ACL_EXECUTE;
	if(flags & F_OK)
		acl |= CHIRP_ACL_READ;
	if(acl == 0)
		acl |= CHIRP_ACL_READ;
	return acl;
}

int chirp_acl_from_open_flags(int flags)
{
	int acl = 0;
	if(flags & O_WRONLY)
		acl |= CHIRP_ACL_WRITE;
	if(flags & O_RDWR)
		acl |= CHIRP_ACL_READ | CHIRP_ACL_WRITE;
	if(flags & O_CREAT)
		acl |= CHIRP_ACL_WRITE;
	if(flags & O_TRUNC)
		acl |= CHIRP_ACL_WRITE;
	if(flags & O_APPEND)
		acl |= CHIRP_ACL_WRITE;
	if(acl == 0)
		acl |= CHIRP_ACL_READ;
	return acl;
}

int chirp_acl_init_root(const char *path)
{
	char aclpath[CHIRP_PATH_MAX];
	char username[USERNAME_MAX];
	CHIRP_FILE *file;

	if(!cfs->do_acl_check()) return 1;

	file = chirp_acl_open(path);
	if(file) {
		chirp_acl_close(file);
		return 1;
	}

	username_get(username);

	sprintf(aclpath, "%s/%s", path, CHIRP_ACL_BASE_NAME);
	file = cfs_fopen(aclpath, "w");
	if(file) {
		cfs_fprintf(file, "unix:%s %s\n", username, chirp_acl_flags_to_text(CHIRP_ACL_READ | CHIRP_ACL_WRITE | CHIRP_ACL_DELETE | CHIRP_ACL_LIST | CHIRP_ACL_ADMIN));
		cfs_fclose(file);
		return 1;
	} else {
		return 0;
	}
}

int chirp_acl_init_copy(const char *path)
{
	char oldpath[CHIRP_LINE_MAX];
	char newpath[CHIRP_LINE_MAX];
	char subject[CHIRP_LINE_MAX];
	CHIRP_FILE *oldfile;
	CHIRP_FILE *newfile;
	int result = 0;
	int flags;

	if(!cfs->do_acl_check()) return 1;

	sprintf(oldpath, "%s/..", path);
	sprintf(newpath, "%s/%s", path, CHIRP_ACL_BASE_NAME);

	oldfile = chirp_acl_open(oldpath);
	if(oldfile) {
		newfile = cfs_fopen(newpath, "w");
		if(newfile) {
			while(chirp_acl_read(oldfile, subject, &flags)) {
				cfs_fprintf(newfile, "%s %s\n", subject, chirp_acl_flags_to_text(flags));
			}
			cfs_fclose(newfile);
			result = 1;
		}
		chirp_acl_close(oldfile);
	}

	return result;
}

int chirp_acl_init_reserve(const char *path, const char *subject)
{
	char dirname[CHIRP_PATH_MAX];
	char aclpath[CHIRP_PATH_MAX];
	CHIRP_FILE *file;
	int newflags = 0;
	int aclflags;

	if(!cfs->do_acl_check()) return 1;

	string_dirname(path, dirname);

	if(!do_chirp_acl_get(dirname, subject, &aclflags))
		return 0;

	if(aclflags & CHIRP_ACL_RESERVE_READ)
		newflags |= CHIRP_ACL_READ;
	if(aclflags & CHIRP_ACL_RESERVE_WRITE)
		newflags |= CHIRP_ACL_WRITE;
	if(aclflags & CHIRP_ACL_RESERVE_LIST)
		newflags |= CHIRP_ACL_LIST;
	if(aclflags & CHIRP_ACL_RESERVE_DELETE)
		newflags |= CHIRP_ACL_DELETE;
	if(aclflags & CHIRP_ACL_RESERVE_PUT)
		newflags |= CHIRP_ACL_PUT;
	if(aclflags & CHIRP_ACL_RESERVE_RESERVE)
		newflags |= CHIRP_ACL_RESERVE;
	if(aclflags & CHIRP_ACL_RESERVE_ADMIN)
		newflags |= CHIRP_ACL_ADMIN;
	if(aclflags & CHIRP_ACL_RESERVE_EXECUTE)
		newflags |= CHIRP_ACL_EXECUTE;

	/*
	   compatibility note:
	   If no sub-rights are associated with the v right,
	   then give all of the ordinary subrights.
	 */

	if(newflags == 0)
		newflags = CHIRP_ACL_READ | CHIRP_ACL_WRITE | CHIRP_ACL_LIST | CHIRP_ACL_DELETE | CHIRP_ACL_ADMIN;

	sprintf(aclpath, "%s/%s", path, CHIRP_ACL_BASE_NAME);
	file = cfs_fopen(aclpath, "w");
	if(file) {
		cfs_fprintf(file, "%s %s\n", subject, chirp_acl_flags_to_text(newflags));
		cfs_fclose(file);
		return 1;
	} else {
		return 0;
	}
}
