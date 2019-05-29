/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_client.h"
#include "chirp_group.h"
#include "chirp_protocol.h"
#include "chirp_ticket.h"

#include "auth.h"
#include "auth_hostname.h"
#include "catch.h"
#include "copy_stream.h"
#include "debug.h"
#include "domain_name_cache.h"
#include "full_io.h"
#include "link.h"
#include "list.h"
#include "macros.h"
#include "shell.h"
#include "sleeptools.h"
#include "string_array.h"
#include "stringtools.h"
#include "url_encode.h"
#include "xxmalloc.h"
#include "address.h"

#if defined(HAS_ATTR_XATTR_H)
#	include <attr/xattr.h>
#elif defined(HAS_SYS_XATTR_H)
#	include <sys/xattr.h>
#endif
#ifndef ENOATTR
#	define ENOATTR  EINVAL
#endif
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <errno.h>
#include <sys/mount.h>
#include <sys/param.h>
#include <sys/socket.h>
#ifdef HAS_SYS_STATFS_H
#	include <sys/statfs.h>
#endif
#include <unistd.h>

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Prevent openssl from opening $HOME/.rnd */
#define OPENSSL_RANDFILE \
	"if [ -r /dev/urandom ]; then\n" \
	"   export RANDFILE=/dev/urandom\n" \
	"elif [ -r /dev/random ]; then\n" \
	"   export RANDFILE=/dev/random\n" \
	"else\n" \
	"   unset RANDFILE\n" \
	"   export HOME=/\n" \
	"fi\n"

/* The maximum chunk of memory the server will allocate to handle I/O */
#define MAX_BUFFER_SIZE (16*1024*1024)

static int global_serial = 0;

struct chirp_client {
	struct link *link;
	char hostport[CHIRP_PATH_MAX];
	int broken;
	int serial;
};

static INT64_T convert_result(INT64_T result)
{
	if(result >= 0) {
		return result;
	} else {
		switch (result) {
		case CHIRP_ERROR_NOT_AUTHENTICATED:
		case CHIRP_ERROR_NOT_AUTHORIZED:
			errno = EACCES;
			break;
		case CHIRP_ERROR_DOESNT_EXIST:
			errno = ENOENT;
			break;
		case CHIRP_ERROR_ALREADY_EXISTS:
			errno = EEXIST;
			break;
		case CHIRP_ERROR_TOO_BIG:
			errno = EFBIG;
			break;
		case CHIRP_ERROR_NO_SPACE:
			errno = ENOSPC;
			break;
		case CHIRP_ERROR_NO_MEMORY:
			errno = ENOMEM;
			break;
		case CHIRP_ERROR_INVALID_REQUEST:
			errno = EINVAL;
			break;
		case CHIRP_ERROR_TOO_MANY_OPEN:
			errno = EMFILE;
			break;
		case CHIRP_ERROR_BUSY:
			errno = EBUSY;
			break;
		case CHIRP_ERROR_TRY_AGAIN:
			errno = EAGAIN;
			break;
		case CHIRP_ERROR_NOT_DIR:
			errno = ENOTDIR;
			break;
		case CHIRP_ERROR_IS_DIR:
			errno = EISDIR;
			break;
		case CHIRP_ERROR_NOT_EMPTY:
			errno = ENOTEMPTY;
			break;
		case CHIRP_ERROR_CROSS_DEVICE_LINK:
			errno = EXDEV;
			break;
		case CHIRP_ERROR_NO_SUCH_JOB:
			errno = ESRCH;
			break;
		case CHIRP_ERROR_IS_A_PIPE:
			errno = ESPIPE;
			break;
		case CHIRP_ERROR_NOT_SUPPORTED:
			errno = ENOTSUP;
			break;
		case CHIRP_ERROR_NAME_TOO_LONG:
			errno = ENAMETOOLONG;
			break;
		case CHIRP_ERROR_GRP_UNREACHABLE:
		case CHIRP_ERROR_TIMED_OUT:
		case CHIRP_ERROR_DISCONNECTED:
		case CHIRP_ERROR_UNKNOWN:
			errno = ECONNRESET;
			break;
		}
		return -1;
	}
}

static INT64_T get_stat_result(struct chirp_client *c, const char *name, struct chirp_stat *info, time_t stoptime)
{
	char line[CHIRP_LINE_MAX];
	INT64_T fields;

	memset(info, 0, sizeof(*info));

	if(!link_readline(c->link, line, CHIRP_LINE_MAX, stoptime)) {
		debug(D_DEBUG, "link broken reading stat: %s", strerror(errno));
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}

	fields = sscanf(line, "%" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 "\n", &info->cst_dev, &info->cst_ino, &info->cst_mode,
			&info->cst_nlink, &info->cst_uid, &info->cst_gid, &info->cst_rdev, &info->cst_size, &info->cst_blksize, &info->cst_blocks, &info->cst_atime, &info->cst_mtime, &info->cst_ctime);

	info->cst_dev = -1;
	info->cst_rdev = 0;

	if(fields != 13) {
		debug(D_DEBUG, "did not get expected fields for stat result: `%s'", line);
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}

	if (name == NULL)
		name = "(anon)";

	debug(D_DEBUG, "\"%s\" = {dev = %"PRId64", ino = %"PRId64", mode = %"PRId64", nlink = %"PRId64", uid = %"PRId64", gid = %"PRId64", rdev = %"PRId64", size = %"PRId64", blksize = %"PRId64", blocks = %"PRId64", atime = %"PRId64", mtime = %"PRId64", ctime = %"PRId64"}", name, (int64_t)info->cst_dev, (int64_t)info->cst_ino, (int64_t)info->cst_mode, (int64_t)info->cst_nlink, (int64_t)info->cst_uid, (int64_t)info->cst_gid, (int64_t)info->cst_rdev, (int64_t)info->cst_size, (int64_t)info->cst_blksize, (int64_t)info->cst_blocks, (int64_t)info->cst_atime, (int64_t)info->cst_mtime, (int64_t)info->cst_ctime);

	return 0;
}

static INT64_T get_statfs_result(struct chirp_client *c, struct chirp_statfs *info, time_t stoptime)
{
	char line[CHIRP_LINE_MAX];
	INT64_T fields;

	memset(info, 0, sizeof(*info));

	if(!link_readline(c->link, line, CHIRP_LINE_MAX, stoptime)) {
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}

	fields = sscanf(line, "%" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 "\n", &info->f_type, &info->f_bsize, &info->f_blocks, &info->f_bfree, &info->f_bavail, &info->f_files, &info->f_ffree);

	if(fields != 7) {
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}

	return 0;
}

static INT64_T get_result(struct chirp_client *c, time_t stoptime)
{
	char line[CHIRP_LINE_MAX];
	INT64_T result;
	INT64_T fields;

	if(!link_readline(c->link, line, CHIRP_LINE_MAX, stoptime)) {
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}

	fields = sscanf(line, "%" SCNd64, &result);
	if(fields != 1) {
		errno = ECONNRESET;
		c->broken = 1;
		return -1;
	}

	result = convert_result(result);
	if(result >= 0) {
		debug(D_CHIRP, " = %"PRId64 , result);
	} else {
		debug(D_CHIRP, " = %"PRId64" (%s)", result, strerror(errno));
	}

	return result;
}

static INT64_T send_command_varargs(struct chirp_client *c, time_t stoptime, char const *fmt, va_list args)
{
	BUFFER_STACK_ABORT(B, CHIRP_LINE_MAX);

	if(c->broken) {
		errno = ECONNRESET;
		return -1;
	}

	buffer_putvfstring(B, fmt, args);

	debug(D_CHIRP, "%s: %s", c->hostport, buffer_tostring(B));

	INT64_T result = link_putstring(c->link, buffer_tostring(B), stoptime);
	if(result < 0) {
		c->broken = 1;
		errno = ECONNRESET;
	}

	return result;
}

static INT64_T send_command(struct chirp_client *c, time_t stoptime, char const *fmt, ...)
{
	INT64_T result;
	va_list args;

	va_start(args, fmt);
	result = send_command_varargs(c, stoptime, fmt, args);
	va_end(args);

	return result;
}

static INT64_T simple_command(struct chirp_client *c, time_t stoptime, char const *fmt, ...)
{
	INT64_T result;
	va_list args;

	va_start(args, fmt);
	result = send_command_varargs(c, stoptime, fmt, args);
	va_end(args);

	if(result >= 0) {
		return get_result(c, stoptime);
	} else {
		return result;
	}
}

struct chirp_client *chirp_client_connect_condor(time_t stoptime)
{
	FILE *file;
	int fields;
	int save_errno;
	struct chirp_client *client;
	char host[CHIRP_LINE_MAX];
	char hostport[CHIRP_LINE_MAX];
	char cookie[CHIRP_LINE_MAX];
	int port;
	int result;

	/*
	 * Older versions of Condor use chirp.config.
	 * Start with Condor V8.X the file is .chirp.config.
	 * In HTCondor commit c584a30022712292294e823fa99173bfa2ff6049, HTCondor accidentally renamed the file again to .chirp_config. This leaked into some stable releases so we look for that filename too. HTCondor reverted the filename change in 112d3b8ed85f78040acfb30ad47f52f944dae2f8.
	 * https://htcondor-wiki.cs.wisc.edu/index.cgi/tktview?tn=3353
	 */

	file = fopen("chirp.config", "r");
	if(!file)
		file = fopen(".chirp.config", "r");
	if(!file)
		file = fopen(".chirp_config", "r");
	if(!file)
		return 0;

	fields = fscanf(file, "%s %d %s", host, &port, cookie);
	fclose(file);

	if(fields != 3) {
		errno = EINVAL;
		return 0;
	}

	string_nformat(hostport, sizeof(hostport), "%s:%d", host, port);

	client = chirp_client_connect(hostport, 0, stoptime);
	if(!client)
		return 0;

	result = chirp_client_cookie(client, cookie, stoptime);
	if(result != 0) {
		save_errno = errno;
		chirp_client_disconnect(client);
		errno = save_errno;
		return 0;
	}

	return client;
}

struct chirp_client *chirp_client_connect(const char *hostport, int negotiate_auth, time_t stoptime)
{
	struct chirp_client *c;
	char addr[LINK_ADDRESS_MAX];
	char host[DOMAIN_NAME_MAX];
	int save_errno;
	int port;

	if(!address_parse_hostport(hostport,host,&port,CHIRP_PORT)) {
		errno = EINVAL;
		return 0;
	}

	if(!domain_name_cache_lookup(host, addr)) {
		errno = ENOENT;
		return 0;
	}

	c = malloc(sizeof(*c));
	if(c) {
		c->link = link_connect(addr, port, stoptime);
		c->broken = 0;
		c->serial = global_serial++;
		strcpy(c->hostport, hostport);
		if(c->link) {
			link_tune(c->link, LINK_TUNE_INTERACTIVE);
			if(negotiate_auth) {
				char *type, *subject;

				int result = auth_assert(c->link, &type, &subject, stoptime);

				if(result) {
					free(type);
					free(subject);
					return c;
				} else {
					int save = errno;
					chirp_client_disconnect(c);
					errno = save;
					return 0;
				}
			} else {
				return c;
			}
		}
		save_errno = errno;
		free(c);
		errno = save_errno;
	}

	return 0;
}

void chirp_client_disconnect(struct chirp_client *c)
{
	link_close(c->link);
	free(c);
}

INT64_T chirp_client_serial(struct chirp_client *c)
{
	return c->serial;
}

INT64_T chirp_client_cookie(struct chirp_client * c, const char *cookie, time_t stoptime)
{
	/* chirp_client_cookie is an artifact from Condor's version of Chirp. They
	 * use this for authentication with the Chirp server. We still support this
	 * in our client as we want to be able to connect to Condor's Chirp server
	 * (e.g. with Parrot + Condor RemoteIO). See also issue #582.
	 */

	return simple_command(c, stoptime, "cookie %s\n", cookie);
}

INT64_T chirp_client_login(struct chirp_client * c, const char *name, const char *password, time_t stoptime)
{
	return simple_command(c, stoptime, "login %s %s\n", name, password);
}

INT64_T chirp_client_getlongdir(struct chirp_client * c, const char *path, chirp_longdir_t callback, void *arg, time_t stoptime)
{
	char name[CHIRP_LINE_MAX];
	struct chirp_stat info;
	int result;

	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));

	result = simple_command(c, stoptime, "getlongdir %s\n", safepath);
	if(result < 0)
		return result;

	while(link_readline(c->link, name, sizeof(name), stoptime)) {

		if(!name[0])
			return 0;

		if(get_stat_result(c, name, &info, stoptime) >= 0) {
			callback(name, &info, arg);
		} else {
			break;
		}
	}

	c->broken = 1;
	errno = ECONNRESET;
	return -1;
}

INT64_T chirp_client_getdir(struct chirp_client * c, const char *path, chirp_dir_t callback, void *arg, time_t stoptime)
{
	INT64_T result;
	const char *name;

	result = chirp_client_opendir(c, path, stoptime);
	if(result == 0) {
		while((name = chirp_client_readdir(c, stoptime))) {
			callback(name, arg);
		}
	}

	return result;
}

INT64_T chirp_client_opendir(struct chirp_client * c, const char *path, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));

	return simple_command(c, stoptime, "getdir %s\n", safepath);
}

const char *chirp_client_readdir(struct chirp_client *c, time_t stoptime)
{
	static char name[CHIRP_PATH_MAX];

	if(link_readline(c->link, name, sizeof(name), stoptime)) {
		if(name[0]) {
			return name;
		} else {
			return 0;
		}
	} else {
		c->broken = 1;
		errno = ECONNRESET;
		return 0;
	}

}

INT64_T chirp_client_getacl(struct chirp_client * c, const char *path, chirp_dir_t callback, void *arg, time_t stoptime)
{
	INT64_T result;
	const char *name;

	result = chirp_client_openacl(c, path, stoptime);
	if(result == 0) {
		while((name = chirp_client_readacl(c, stoptime))) {
			callback(name, arg);
		}
	}

	return result;
}

INT64_T chirp_client_openacl(struct chirp_client * c, const char *path, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));

	return simple_command(c, stoptime, "getacl %s\n", safepath);
}

const char *chirp_client_readacl(struct chirp_client *c, time_t stoptime)
{
	static char acl[CHIRP_PATH_MAX];

	if(link_readline(c->link, acl, sizeof(acl), stoptime)) {
		if(acl[0]) {
			return acl;
		} else {
			return 0;
		}
	} else {
		c->broken = 1;
		errno = ECONNRESET;
		return 0;
	}

}

static int ticket_translate(const char *name, char *ticket_subject)
{
	static const char command[] =
		"set -e\n"
		OPENSSL_RANDFILE
		"sed '/^\\s*#/d' < \"$CHIRP_TICKET_NAME\" | openssl rsa -pubout\n"
		;

	INT64_T result = 0;
	int status;
	buffer_t Bout[1];
	buffer_t Berr[1];
	buffer_t Benv[1];
	const char *env[] = {NULL, NULL, NULL};

	const char *dummy;
	if(chirp_ticket_isticketsubject(name, &dummy)) {
		strcpy(ticket_subject, name);
		return 0;
	}
	if (access(name, R_OK) == -1)
		return -1;

	buffer_init(Bout);
	buffer_abortonfailure(Bout, 1);
	buffer_init(Berr);
	buffer_abortonfailure(Berr, 1);
	buffer_init(Benv);
	buffer_abortonfailure(Benv, 1);

	buffer_putfstring(Benv, "CHIRP_TICKET_NAME=%s", name);
	env[0] = buffer_tostring(Benv);

	result = shellcode(command, env, NULL, 0, Bout, Berr, &status);

	if (result == 0) {
		debug(D_DEBUG, "shellcode exit status %d; stderr:\n%s", status, buffer_tostring(Berr));

		if (status == 0) {
			/* load the digest */
			const char *digest = chirp_ticket_digest(buffer_tostring(Bout));
			string_nformat(ticket_subject, CHIRP_LINE_MAX, "ticket:%s", digest);
		} else {
			debug(D_CHIRP, "could not create ticket, do you have openssl installed?");
			errno = ENOSYS;
			result = -1;
		}
	}

	buffer_free(Bout);
	buffer_free(Berr);
	buffer_free(Benv);

	return result;
}

/* Some versions of gcc emit a silly error about the use of %c.  This suppresses that error. */

static size_t my_strftime(char *str, int len, const char *fmt, struct tm *t)
{
	return strftime(str, len, fmt, t);
}


INT64_T chirp_client_ticket_register(struct chirp_client * c, const char *name, const char *subject, time_t duration, time_t stoptime)
{
	static const char command[] =
		"set -e\n"
		OPENSSL_RANDFILE
		"if [ -r \"$CHIRP_TICKET_NAME\" ]; then\n"
		"	sed '/^\\s*#/d' < \"$CHIRP_TICKET_NAME\" | openssl rsa -pubout\n"
		"	exit 0\n"
		"else\n"
		"	exit 1\n"
		"fi\n"
		;

	INT64_T result = 0;
	int status;
	buffer_t Bout[1];
	buffer_t Berr[1];
	buffer_t Benv[1];
	const char *env[] = {NULL, NULL};

	if(subject == NULL)
		subject = "self";

	char ticket_subject[CHIRP_LINE_MAX];
	if(access(name, R_OK) == -1)
		return -1;	/* the 'name' argument must be a client ticket filename */
	if (ticket_translate(name, ticket_subject) == -1)
		return -1;

	buffer_init(Bout);
	buffer_abortonfailure(Bout, 1);
	buffer_init(Berr);
	buffer_abortonfailure(Berr, 1);
	buffer_init(Benv);
	buffer_abortonfailure(Benv, 1);

	buffer_putfstring(Benv, "CHIRP_TICKET_NAME=%s", name);
	env[0] = buffer_tostring(Benv);

	result = shellcode(command, env, NULL, 0, Bout, Berr, &status);

	if (result == 0) {
		debug(D_DEBUG, "shellcode exit status %d; stderr:\n%s", status, buffer_tostring(Berr));

		if (status != 0) {
			debug(D_CHIRP, "could not create ticket, do you have openssl installed?");
			errno = ENOSYS;
			result = -1;
			goto out;
		}

		result = send_command(c, stoptime, "ticket_register %s %llu %zu\n", subject, (unsigned long long) duration, buffer_pos(Bout));
		if (result < 0)
			goto out;

		result = link_write(c->link, buffer_tostring(Bout), buffer_pos(Bout), stoptime);
		if ((size_t)result != buffer_pos(Bout)) {
			c->broken = 1;
			errno = ECONNRESET;
			result = -1;
			goto out;
		}

		result = get_result(c, stoptime);

		if(result == 0) {
			time_t t;
			struct tm tm;
			char now[1024];
			char expiration[1024];

			time(&t);
			localtime_r(&t, &tm);
			my_strftime(now, sizeof(now) / sizeof(char), "%c", &tm);
			t += duration;
			localtime_r(&t, &tm);
			my_strftime(expiration, sizeof(expiration) / sizeof(char), "%c", &tm);

			FILE *file = fopen(name, "a");
			if(file == NULL) {
				result = -1;
				goto out;
			}
			fprintf(file, "# %s: Registered with %s as \"%s\". Expires on %s\n", now, c->hostport, subject, expiration);
			fclose(file);
		}
	}

out:
	buffer_free(Bout);
	buffer_free(Berr);
	buffer_free(Benv);

	return result;
}

INT64_T chirp_client_ticket_create(struct chirp_client * c, char name[CHIRP_PATH_MAX], unsigned bits, time_t stoptime)
{
	static const char command[] =
		"set -e\n"
		OPENSSL_RANDFILE
		"umask 0177\n" /* files only readable/writable by owner */
		"T=`mktemp /tmp/ticket.XXXXXX`\n"
		"P=`mktemp /tmp/ticket.pub.XXXXXX`\n"
		"MD5=`mktemp /tmp/ticket.md5.XXXXXX`\n"
		"echo \"# Chirp Ticket\" > \"$T\"\n"
		"echo \"# `date`: Ticket Created.\" >> \"$T\"\n"
		"openssl genrsa \"$CHIRP_TICKET_BITS\" >> \"$T\"\n"
		"sed '/^\\s*#/d' < \"$T\" | openssl rsa -pubout > \"$P\"\n"
		/* WARNING: openssl is *very* bad at giving sensible output. Use the last
		 * 32 non-space characters as the MD5 sum.
		 */
		"openssl md5 < \"$P\" | tr -d '[:space:]' | tail -c 32 > \"$MD5\"\n"
		"if [ -z \"$CHIRP_TICKET_NAME\" ]; then\n"
		"  CHIRP_TICKET_NAME=\"ticket.`cat $MD5`\"\n"
		"fi\n"
		"cat > \"$CHIRP_TICKET_NAME\" < \"$T\"\n"
		"rm -f \"$T\" \"$P\" \"$MD5\"\n"
		"echo \"Generated ticket $CHIRP_TICKET_NAME.\" 1>&2\n"
		"printf '%s' \"$CHIRP_TICKET_NAME\"\n"
		;

	INT64_T result = 0;
	int status;
	buffer_t Bout[1];
	buffer_t Berr[1];
	buffer_t Benv[1];
	const char *env[] = {NULL, NULL, NULL};

	buffer_init(Bout);
	buffer_abortonfailure(Bout, 1);
	buffer_init(Berr);
	buffer_abortonfailure(Berr, 1);
	buffer_init(Benv);
	buffer_abortonfailure(Benv, 1);

	buffer_putfstring(Benv, "CHIRP_TICKET_BITS=%u", bits);
	buffer_putliteral(Benv, "\0");
	buffer_putfstring(Benv, "CHIRP_TICKET_NAME=%s", name);
	env[0] = buffer_tostring(Benv);
	env[1] = strchr(env[0], '\0')+1;

	result = shellcode(command, env, NULL, 0, Bout, Berr, &status);

	if (result == 0) {
		debug(D_DEBUG, "shellcode exit status %d; stderr:\n%s", status, buffer_tostring(Berr));

		if (status == 0) {
			string_nformat(name, CHIRP_PATH_MAX, "%s", buffer_tostring(Bout));
		} else {
			debug(D_CHIRP, "could not create ticket, do you have openssl installed?");
			errno = ENOSYS;
			result = -1;
		}
	}

	buffer_free(Bout);
	buffer_free(Berr);
	buffer_free(Benv);

	return result;
}

INT64_T chirp_client_ticket_delete(struct chirp_client * c, const char *name, time_t stoptime)
{
	char ticket_subject[CHIRP_LINE_MAX];

	if (ticket_translate(name, ticket_subject) == -1)
		return -1;

	INT64_T result = simple_command(c, stoptime, "ticket_delete %s\n", ticket_subject);

	if(result == 0) {
		unlink(name);
	}
	return result;
}

INT64_T chirp_client_ticket_get(struct chirp_client * c, const char *name, char **subject, char **ticket, time_t * duration, char ***rights, time_t stoptime)
{
	INT64_T result;
	char ticket_subject[CHIRP_LINE_MAX];

	*subject = *ticket = NULL;
	*rights = NULL;

	if (ticket_translate(name, ticket_subject) == -1)
		return -1;

	result = simple_command(c, stoptime, "ticket_get %s\n", ticket_subject);

	if(result >= 0) {
		char line[CHIRP_LINE_MAX];
		size_t length;
		size_t nrights = 0;

		if(!link_readline(c->link, line, CHIRP_LINE_MAX, stoptime))
			goto failure;
		if(sscanf(line, "%zu", &length) != 1)
			goto failure;
		*subject = xxmalloc((length + 1) * sizeof(char));
		if(!link_read(c->link, *subject, length, stoptime))
			goto failure;
		(*subject)[length] = '\0';

		if(!link_readline(c->link, line, CHIRP_LINE_MAX, stoptime))
			goto failure;
		if(sscanf(line, "%zu", &length) != 1)
			goto failure;
		*ticket = xxmalloc((length + 1) * sizeof(char));
		if(!link_read(c->link, *ticket, length, stoptime))
			goto failure;
		(*ticket)[length] = '\0';

		if(!link_readline(c->link, line, CHIRP_LINE_MAX, stoptime))
			goto failure;
		unsigned long long tmp;
		if(sscanf(line, "%llu", &tmp) != 1)
			goto failure;
		*duration = (time_t) tmp;

		while(1) {
			char path[CHIRP_PATH_MAX];
			char acl[CHIRP_LINE_MAX];
			if(!link_readline(c->link, line, CHIRP_LINE_MAX, stoptime))
				goto failure;
			if(sscanf(line, "%s %s", path, acl) == 2) {
				*rights = xxrealloc(*rights, sizeof(char *) * 2 * (nrights + 2));
				(*rights)[nrights * 2 + 0] = xxstrdup(path);
				(*rights)[nrights * 2 + 1] = xxstrdup(acl);
				(*rights)[nrights * 2 + 2] = NULL;
				(*rights)[nrights * 2 + 3] = NULL;
				nrights++;
			} else if(sscanf(line, "%" SCNd64, &result) == 1 && result == 0) {
				break;
			} else
				goto failure;
		}

		return 0;
failure:
		free(*subject);
		free(*ticket);
		if(*rights != NULL) {
			char **tmp = *rights;
			while(tmp[0] && tmp[1]) {
				free(tmp[0]);
				free(tmp[1]);
			}
			free(*rights);
		}
		*subject = *ticket = NULL;
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}

	return result;
}

INT64_T chirp_client_ticket_list(struct chirp_client * c, const char *subject, char ***list, time_t stoptime)
{
	INT64_T result;

	size_t size = 0;
	*list = NULL;

	result = simple_command(c, stoptime, "ticket_list %s\n", subject);

	if(result == 0) {
		while(1) {
			char line[CHIRP_LINE_MAX];
			size_t length;

			if(!link_readline(c->link, line, CHIRP_LINE_MAX, stoptime))
				goto failure;
			if(sscanf(line, "%zu", &length) != 1)
				goto failure;
			if(length == 0)
				break;

			size++;
			*list = xxrealloc(*list, sizeof(char *) * (size + 1));
			(*list)[size - 1] = xxmalloc(sizeof(char) * (length + 1));
			if(!link_read(c->link, (*list)[size - 1], length, stoptime))
				goto failure;
			(*list)[size - 1][length] = '\0';
			(*list)[size] = NULL;
		}
	}

	return 0;
failure:
	if(*list != NULL) {
		char **tmp = *list;
		while(tmp[0]) {
			free(tmp[0]);
		}
		free(*list);
	}
	c->broken = 1;
	errno = ECONNRESET;
	return -1;
}

INT64_T chirp_client_ticket_modify(struct chirp_client * c, const char *name, const char *path, const char *aclmask, time_t stoptime)
{
	char ticket_subject[CHIRP_LINE_MAX];
	char safepath[CHIRP_LINE_MAX];
	if (ticket_translate(name, ticket_subject) == -1)
		return -1;
	url_encode(path, safepath, sizeof(safepath));
	INT64_T result = simple_command(c, stoptime, "ticket_modify %s %s %s\n", ticket_subject, safepath, aclmask);
	if(result == 0) {
		time_t t;
		struct tm tm;
		char now[1024];

		time(&t);
		localtime_r(&t, &tm);
		my_strftime(now, sizeof(now) / sizeof(char), "%c", &tm);

		FILE *file = fopen(name, "a");
		if(file == NULL)
			return -1;
		fprintf(file, "# %s: Set ACL Mask on %s directory = '%s' mask = '%s'.\n", now, c->hostport, path, aclmask);
		fclose(file);
	}
	return result;
}

INT64_T chirp_client_setacl(struct chirp_client * c, const char *path, const char *user, const char *acl, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	return simple_command(c, stoptime, "setacl %s %s %s\n", safepath, user, acl);
}

INT64_T chirp_client_resetacl(struct chirp_client * c, const char *path, const char *acl, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	return simple_command(c, stoptime, "resetacl %s %s\n", safepath, acl);
}

INT64_T chirp_client_locate(struct chirp_client * c, const char *path, chirp_loc_t callback, void *arg, time_t stoptime)
{
	char location[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];
	sscanf(c->hostport, "%[^:]%*s", host);
	string_nformat(location, sizeof(location), "%s:%s", host, path);
	callback(location, arg);
	return 1;
}

INT64_T chirp_client_open(struct chirp_client * c, const char *path, INT64_T flags, INT64_T mode, struct chirp_stat * info, time_t stoptime)
{
	INT64_T result;
	char fstr[256];

	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));

	fstr[0] = 0;

	if(flags & O_WRONLY) {
		strcat(fstr, "w");
	} else if(flags & O_RDWR) {
		strcat(fstr, "rw");
	} else {
		strcat(fstr, "r");
	}

	if(flags & O_CREAT)
		strcat(fstr, "c");
	if(flags & O_TRUNC)
		strcat(fstr, "t");
	if(flags & O_APPEND)
		strcat(fstr, "a");
	if(flags & O_EXCL)
		strcat(fstr, "x");
#ifdef O_SYNC
	if(flags & O_SYNC)
		strcat(fstr, "s");
#endif

	result = simple_command(c, stoptime, "open %s %s %lld\n", safepath, fstr, mode);
	if(result >= 0) {
		if(get_stat_result(c, path, info, stoptime) >= 0) {
			return result;
		} else {
			c->broken = 1;
			errno = ECONNRESET;
			return -1;
		}
	} else {
		return result;
	}
}

INT64_T chirp_client_close(struct chirp_client * c, INT64_T fd, time_t stoptime)
{
	return simple_command(c, stoptime, "close %lld\n", fd);
}

INT64_T chirp_client_pread_begin(struct chirp_client * c, INT64_T fd, void *buffer, INT64_T length, INT64_T offset, time_t stoptime)
{
	return send_command(c, stoptime, "pread %lld %lld %lld\n", fd, length, offset);
}

INT64_T chirp_client_pread_finish(struct chirp_client * c, INT64_T fd, void *buffer, INT64_T length, INT64_T offset, time_t stoptime)
{
	INT64_T result;
	INT64_T actual;

	result = get_result(c, stoptime);
	if(result > 0) {
		actual = link_read(c->link, buffer, result, stoptime);
		if(actual != result) {
			errno = ECONNRESET;
			return -1;
		}
	}

	return result;
}

INT64_T chirp_client_pread(struct chirp_client * c, INT64_T fd, void *buffer, INT64_T length, INT64_T offset, time_t stoptime)
{
	INT64_T result = chirp_client_pread_begin(c, fd, buffer, length, offset, stoptime);
	if(result < 0)
		return result;
	return chirp_client_pread_finish(c, fd, buffer, length, offset, stoptime);
}

INT64_T chirp_client_sread_begin(struct chirp_client * c, INT64_T fd, void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime)
{
	return send_command(c, stoptime, "sread %lld %lld %lld %lld %lld\n", fd, length, stride_length, stride_skip, offset);
}

INT64_T chirp_client_sread_finish(struct chirp_client * c, INT64_T fd, void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime)
{
	INT64_T result;
	INT64_T actual;

	result = get_result(c, stoptime);
	if(result > 0) {
		actual = link_read(c->link, buffer, result, stoptime);
		if(actual != result) {
			errno = ECONNRESET;
			return -1;
		}
	}

	return result;
}

INT64_T chirp_client_sread(struct chirp_client * c, INT64_T fd, void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime)
{
	INT64_T result = chirp_client_sread_begin(c, fd, buffer, length, stride_length, stride_skip, offset, stoptime);
	if(result < 0)
		return result;
	return chirp_client_sread_finish(c, fd, buffer, length, stride_length, stride_skip, offset, stoptime);
}

INT64_T chirp_client_getfile(struct chirp_client * c, const char *path, FILE * stream, time_t stoptime)
{
	INT64_T length;

	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));

	length = simple_command(c, stoptime, "getfile %s\n", safepath);

	if(length >= 0) {
		if(link_stream_to_file(c->link, stream, length, stoptime) == length) {
			return length;
		} else {
			c->broken = 1;
			errno = ECONNRESET;
		}
	}

	return -1;
}

INT64_T chirp_client_getfile_buffer(struct chirp_client * c, const char *path, char **buffer, time_t stoptime)
{
	INT64_T length;
	INT64_T result;

	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));

	*buffer = 0;

	length = simple_command(c, stoptime, "getfile %s\n", safepath);
	if(length <= 0)
		return length;

	*buffer = malloc(length + 1);
	if(!*buffer) {
		c->broken = 1;
		errno = ENOMEM;
		return -1;
	}

	result = link_read(c->link, *buffer, length, stoptime);
	if(result < 0) {
		*buffer = realloc(*buffer, 0);
		c->broken = 1;
		return -1;
	}

	(*buffer)[length] = 0;

	return result;
}

INT64_T chirp_client_readlink(struct chirp_client * c, const char *path, char *buffer, INT64_T length, time_t stoptime)
{
	INT64_T result;
	INT64_T actual;

	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));

	result = simple_command(c, stoptime, "readlink %s %lld\n", safepath, length);

	if(result > 0) {
		actual = link_read(c->link, buffer, result, stoptime);
		if(actual != result) {
			c->broken = 1;
			errno = ECONNRESET;
			return -1;
		}
	}

	return result;
}

INT64_T chirp_client_localpath(struct chirp_client * c, const char *path, char *localpath, int length, time_t stoptime)
{
	INT64_T result;
	INT64_T actual;

	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));

	result = simple_command(c, stoptime, "localpath %s\n", path);

	if(result > 0) {
		actual = link_read(c->link, localpath, result, stoptime);
		if(actual != result) {
			c->broken = 1;
			errno = ECONNRESET;
			return -1;
		}
	}

	return result;
}

INT64_T chirp_client_whoami(struct chirp_client * c, char *buffer, INT64_T length, time_t stoptime)
{
	INT64_T result;
	INT64_T actual;

	result = simple_command(c, stoptime, "whoami %lld\n", length);

	if(result > 0) {
		actual = link_read(c->link, buffer, result, stoptime);
		if(actual != result) {
			c->broken = 1;
			errno = ECONNRESET;
			return -1;
		}
		buffer[actual] = 0;
	}

	return result;
}

INT64_T chirp_client_whoareyou(struct chirp_client * c, const char *rhost, char *buffer, INT64_T length, time_t stoptime)
{
	INT64_T result;
	INT64_T actual;

	result = simple_command(c, stoptime, "whoareyou %s %lld\n", rhost, length);

	if(result > 0) {
		actual = link_read(c->link, buffer, result, stoptime);
		if(actual != result) {
			c->broken = 1;
			errno = ECONNRESET;
			return -1;
		}
	}

	return result;
}

INT64_T chirp_client_pwrite_begin(struct chirp_client * c, INT64_T fd, const void *buffer, INT64_T length, INT64_T offset, time_t stoptime)
{
	INT64_T result;

	if(length > MAX_BUFFER_SIZE)
		length = MAX_BUFFER_SIZE;

	result = send_command(c, stoptime, "pwrite %lld %lld %lld\n", fd, length, offset);
	if(result < 0)
		return result;

	result = link_putlstring(c->link, buffer, length, stoptime);
	if(result != length) {
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}

	return result;
}

INT64_T chirp_client_pwrite_finish(struct chirp_client * c, INT64_T fd, const void *buffer, INT64_T length, INT64_T offset, time_t stoptime)
{
	return get_result(c, stoptime);
}

INT64_T chirp_client_pwrite(struct chirp_client * c, INT64_T fd, const void *buffer, INT64_T length, INT64_T offset, time_t stoptime)
{
	INT64_T result = chirp_client_pwrite_begin(c, fd, buffer, length, offset, stoptime);
	if(result < 0)
		return result;
	return chirp_client_pwrite_finish(c, fd, buffer, length, offset, stoptime);
}

INT64_T chirp_client_swrite_begin(struct chirp_client * c, INT64_T fd, const void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime)
{
	INT64_T result;

	if(length > MAX_BUFFER_SIZE)
		length = MAX_BUFFER_SIZE;

	result = send_command(c, stoptime, "swrite %lld %lld %lld %lld %lld\n", fd, length, stride_length, stride_skip, offset);
	if(result < 0)
		return result;

	result = link_putlstring(c->link, buffer, length, stoptime);
	if(result != length) {
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}

	return result;
}

INT64_T chirp_client_swrite_finish(struct chirp_client * c, INT64_T fd, const void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime)
{
	return get_result(c, stoptime);
}

INT64_T chirp_client_swrite(struct chirp_client * c, INT64_T fd, const void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime)
{
	INT64_T result = chirp_client_swrite_begin(c, fd, buffer, length, stride_length, stride_skip, offset, stoptime);
	if(result < 0)
		return result;
	return chirp_client_swrite_finish(c, fd, buffer, length, stride_length, stride_skip, offset, stoptime);
}

INT64_T chirp_client_putfile(struct chirp_client * c, const char *path, FILE * stream, INT64_T mode, INT64_T length, time_t stoptime)
{
	INT64_T result;

	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));

	result = simple_command(c, stoptime, "putfile %s %lld %lld\n", safepath, mode, length);
	if(result < 0)
		return result;

	result = link_stream_from_file(c->link, stream, length, stoptime);
	if(result != length) {
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}

	return get_result(c, stoptime);
}

INT64_T chirp_client_putfile_buffer(struct chirp_client * c, const char *path, const void *buffer, INT64_T mode, size_t length, time_t stoptime)
{
	INT64_T result;

	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));

	result = simple_command(c, stoptime, "putfile %s %lld %lld\n", safepath, mode, length);
	if(result < 0)
		return result;

	result = link_putlstring(c->link, buffer, length, stoptime);
	if((size_t)result != length) {
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}

	return get_result(c, stoptime);
}

INT64_T chirp_client_getstream(struct chirp_client * c, const char *path, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));

	return simple_command(c, stoptime, "getstream %s\n", path);
}

INT64_T chirp_client_getstream_read(struct chirp_client * c, void *buffer, INT64_T length, time_t stoptime)
{
	return link_read_avail(c->link, buffer, length, stoptime);
}

INT64_T chirp_client_putstream(struct chirp_client * c, const char *path, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	return simple_command(c, stoptime, "putstream %s\n", path);
}

INT64_T chirp_client_putstream_write(struct chirp_client * c, const char *data, INT64_T length, time_t stoptime)
{
	return link_putlstring(c->link, data, length, stoptime);
}

INT64_T chirp_client_thirdput(struct chirp_client * c, const char *path, const char *hostname, const char *newpath, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	char safenewpath[CHIRP_LINE_MAX];

	url_encode(path, safepath, sizeof(safepath));
	url_encode(newpath, safenewpath, sizeof(safenewpath));

	return simple_command(c, stoptime, "thirdput %s %s %s\n", safepath, hostname, safenewpath);
}

INT64_T chirp_client_fchmod(struct chirp_client * c, INT64_T fd, INT64_T mode, time_t stoptime)
{
	return simple_command(c, stoptime, "fchmod %lld %lld\n", fd, mode);
}

INT64_T chirp_client_fchown(struct chirp_client * c, INT64_T fd, INT64_T uid, INT64_T gid, time_t stoptime)
{
	return simple_command(c, stoptime, "fchown %lld %lld %lld\n", fd, uid, gid);
}

INT64_T chirp_client_ftruncate(struct chirp_client * c, INT64_T fd, INT64_T length, time_t stoptime)
{
	return simple_command(c, stoptime, "ftruncate %lld %lld\n", fd, length);
}

INT64_T chirp_client_fstat_begin(struct chirp_client * c, INT64_T fd, struct chirp_stat * info, time_t stoptime)
{
	return send_command(c, stoptime, "fstat %lld\n", fd);
}

INT64_T chirp_client_fstat_finish(struct chirp_client * c, INT64_T fd, struct chirp_stat * info, time_t stoptime)
{
	INT64_T result = get_result(c, stoptime);
	if(result >= 0)
		return get_stat_result(c, NULL, info, stoptime);
	return result;
}

INT64_T chirp_client_fstat(struct chirp_client * c, INT64_T fd, struct chirp_stat * info, time_t stoptime)
{
	INT64_T result = chirp_client_fstat_begin(c, fd, info, stoptime);
	if(result >= 0)
		return chirp_client_fstat_finish(c, fd, info, stoptime);
	return result;
}

INT64_T chirp_client_stat(struct chirp_client * c, const char *path, struct chirp_stat * info, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	INT64_T result = simple_command(c, stoptime, "stat %s\n", safepath);
	if(result >= 0)
		result = get_stat_result(c, path, info, stoptime);
	return result;
}

INT64_T chirp_client_lstat(struct chirp_client * c, const char *path, struct chirp_stat * info, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	INT64_T result = simple_command(c, stoptime, "lstat %s\n", safepath);
	if(result >= 0)
		result = get_stat_result(c, path, info, stoptime);
	return result;
}

INT64_T chirp_client_fstatfs(struct chirp_client * c, INT64_T fd, struct chirp_statfs * info, time_t stoptime)
{
	INT64_T result = simple_command(c, stoptime, "fstatfs %lld\n", fd);
	if(result >= 0)
		result = get_statfs_result(c, info, stoptime);
	return result;
}

INT64_T chirp_client_statfs(struct chirp_client * c, const char *path, struct chirp_statfs * info, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	INT64_T result = simple_command(c, stoptime, "statfs %s\n", safepath);
	if(result >= 0)
		result = get_statfs_result(c, info, stoptime);
	return result;
}

INT64_T chirp_client_unlink(struct chirp_client * c, const char *path, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	return simple_command(c, stoptime, "unlink %s\n", safepath);
}

INT64_T chirp_client_rename(struct chirp_client * c, const char *oldpath, const char *newpath, time_t stoptime)
{
	char safeoldpath[CHIRP_LINE_MAX];
	char safenewpath[CHIRP_LINE_MAX];

	url_encode(oldpath, safeoldpath, sizeof(safeoldpath));
	url_encode(newpath, safenewpath, sizeof(safenewpath));

	return simple_command(c, stoptime, "rename %s %s\n", safeoldpath, safenewpath);
}

INT64_T chirp_client_link(struct chirp_client * c, const char *oldpath, const char *newpath, time_t stoptime)
{
	char safeoldpath[CHIRP_LINE_MAX];
	char safenewpath[CHIRP_LINE_MAX];

	url_encode(oldpath, safeoldpath, sizeof(safeoldpath));
	url_encode(newpath, safenewpath, sizeof(safenewpath));

	return simple_command(c, stoptime, "link %s %s\n", safeoldpath, safenewpath);
}

INT64_T chirp_client_symlink(struct chirp_client * c, const char *oldpath, const char *newpath, time_t stoptime)
{
	char safeoldpath[CHIRP_LINE_MAX];
	char safenewpath[CHIRP_LINE_MAX];

	url_encode(oldpath, safeoldpath, sizeof(safeoldpath));
	url_encode(newpath, safenewpath, sizeof(safenewpath));

	debug(D_CHIRP, "symlink %s %s", safeoldpath, safenewpath);
	return simple_command(c, stoptime, "symlink %s %s\n", safeoldpath, safenewpath);
}

INT64_T chirp_client_fsync_begin(struct chirp_client * c, INT64_T fd, time_t stoptime)
{
	return send_command(c, stoptime, "fsync %lld\n", fd);
}

INT64_T chirp_client_fsync_finish(struct chirp_client * c, INT64_T fd, time_t stoptime)
{
	return get_result(c, stoptime);
}

INT64_T chirp_client_fsync(struct chirp_client * c, INT64_T fd, time_t stoptime)
{
	INT64_T result = chirp_client_fsync_begin(c, fd, stoptime);
	if(result >= 0)
		return get_result(c, stoptime);
	return result;
}

INT64_T chirp_client_mkdir(struct chirp_client * c, char const *path, INT64_T mode, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	return simple_command(c, stoptime, "mkdir %s %lld\n", safepath, mode);
}

INT64_T chirp_client_rmdir(struct chirp_client * c, char const *path, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	return simple_command(c, stoptime, "rmdir %s\n", safepath);
}

INT64_T chirp_client_rmall(struct chirp_client * c, char const *path, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	return simple_command(c, stoptime, "rmall %s\n", safepath);
}

INT64_T chirp_client_truncate(struct chirp_client * c, char const *path, INT64_T length, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	return simple_command(c, stoptime, "truncate %s %lld\n", safepath, length);
}

INT64_T chirp_client_utime(struct chirp_client * c, char const *path, time_t actime, time_t modtime, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	return simple_command(c, stoptime, "utime %s %u %u\n", safepath, actime, modtime);
}

INT64_T chirp_client_access(struct chirp_client * c, char const *path, INT64_T mode, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	return simple_command(c, stoptime, "access %s %lld\n", safepath, mode);
}

INT64_T chirp_client_chmod(struct chirp_client * c, char const *path, INT64_T mode, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	return simple_command(c, stoptime, "chmod %s %lld\n", safepath, mode);
}

INT64_T chirp_client_chown(struct chirp_client * c, char const *path, INT64_T uid, INT64_T gid, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	return simple_command(c, stoptime, "chown %s %lld %lld\n", safepath, uid, gid);
}

INT64_T chirp_client_lchown(struct chirp_client * c, char const *path, INT64_T uid, INT64_T gid, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	return simple_command(c, stoptime, "lchown %s %lld %lld\n", safepath, uid, gid);
}

INT64_T chirp_client_hash(struct chirp_client * c, const char *path, const char *algorithm, unsigned char digest[CHIRP_DIGEST_MAX], time_t stoptime)
{
	INT64_T result;
	INT64_T actual;

	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));

	result = simple_command(c, stoptime, "hash %s %s\n", algorithm, path);

	if(result > 0) {
		actual = link_read(c->link, (char *) digest, result, stoptime);
		if(actual != result) {
			errno = ECONNRESET;
			result = -1;
		}
	} else if(result >= 0) {
		result = -1;
		errno = ECONNRESET;
	}
	return result;
}

INT64_T chirp_client_md5(struct chirp_client * c, const char *path, unsigned char digest[16], time_t stoptime)
{
	return chirp_client_hash(c, path, "md5", digest, stoptime); /* digest has wrong length, but it is okay for md5 */
}

INT64_T chirp_client_setrep(struct chirp_client * c, char const *path, int nreps, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	return simple_command(c, stoptime, "setrep %s %d\n", safepath, nreps);
}

INT64_T chirp_client_remote_debug(struct chirp_client * c, const char *flag, time_t stoptime)
{
	if(flag == NULL)
		flag = "*";
	return simple_command(c, stoptime, "debug %s\n", flag);
}

INT64_T chirp_client_audit(struct chirp_client * c, const char *path, struct chirp_audit ** list, time_t stoptime)
{
	INT64_T result;
	struct chirp_audit *entry;
	int i, actual;
	char line[CHIRP_LINE_MAX];

	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));

	result = simple_command(c, stoptime, "audit %s\n", safepath);
	if(result <= 0)
		return result;

	*list = malloc(sizeof(struct chirp_audit) * result);
	entry = *list;

	for(i = 0; i < result; i++) {
		actual = link_readline(c->link, line, sizeof(line), stoptime);
		if(actual <= 0) {
			free(*list);
			result = -1;
			errno = ECONNRESET;
			break;
		} else {
			sscanf(line, "%s %" SCNd64 " %" SCNd64 " %" SCNd64, entry->name, &entry->nfiles, &entry->ndirs, &entry->nbytes);
		}
		entry++;
	}

	return result;
}

INT64_T chirp_client_mkalloc(struct chirp_client * c, char const *path, INT64_T size, INT64_T mode, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	return simple_command(c, stoptime, "mkalloc %s %lld %lld\n", safepath, size, mode);
}

INT64_T chirp_client_lsalloc(struct chirp_client * c, char const *path, char *allocpath, INT64_T * total, INT64_T * inuse, time_t stoptime)
{
	int result;
	char line[CHIRP_LINE_MAX];

	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));

	result = simple_command(c, stoptime, "lsalloc %s\n", safepath);
	if(result == 0) {
		if(link_readline(c->link, line, sizeof(line), stoptime)) {
			sscanf(line, "%s %" SCNd64 " %" SCNd64, allocpath, total, inuse);
		} else {
			c->broken = 1;
			errno = ECONNRESET;
			result = -1;
		}
	}
	return result;
}

CHIRP_SEARCH *chirp_client_opensearch(struct chirp_client * c, const char *path, const char *pattern, int flags, time_t stoptime)
{
	INT64_T result = simple_command(c, stoptime, "search %s %s %d\n", pattern, path, flags);

	if(result == 0) {
		char line[CHIRP_LINE_MAX];
		size_t n = 0;

		CHIRP_SEARCH *result = malloc(sizeof(CHIRP_SEARCH));
		if (!result) return NULL;

		buffer_init(&result->B);
		buffer_abortonfailure(&result->B, 1);
		while(link_readline(c->link, line, sizeof(line), stoptime) && line[0]) {
			buffer_putstring(&result->B, line);
			n += strlen(line);
		}
		if(n == 0) {
			buffer_putliteral(&result->B, "");
		}

		result->current = buffer_tostring(&result->B);
		return result;
	} else {
		return NULL;
	}
}

static const char *search_readnext(const char *current, char **result)
{
	*result = NULL;
	if(strlen(current)) {
		ptrdiff_t length;
		const char *tail = strchr(current, ':');

		if(tail)
			length = tail - current;
		else
			length = strlen(current);

		*result = xxmalloc(length + 1);
		strncpy(*result, current, length);
		(*result)[length] = '\0';
		return current + length + 1;
	}
	return NULL;
}

static void search_unpackstat(const char *str, struct chirp_stat *info)
{
	sscanf(str, "%" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " " "%" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " " "%" PRId64 " %" PRId64 " %" PRId64, &info->cst_dev, &info->cst_ino, &info->cst_mode, &info->cst_nlink,
		   &info->cst_uid, &info->cst_gid, &info->cst_rdev, &info->cst_size, &info->cst_atime, &info->cst_mtime, &info->cst_ctime, &info->cst_blksize, &info->cst_blocks);
}

struct chirp_searchent *chirp_client_readsearch(CHIRP_SEARCH * search)
{
	char *result;
	const char *current = search_readnext(search->current, &result);

	if(current && result) {
		search->entry.err = atoi(result);
		free(result);
		if(search->entry.err) {
			current = search_readnext(current, &result);
			assert(current && result);
			search->entry.errsource = atoi(result);
			free(result);

			current = search_readnext(current, &result);
			assert(current && result);
			memset(search->entry.path, 0, CHIRP_PATH_MAX);
			strncpy(search->entry.path, result, CHIRP_PATH_MAX - 1);
			free(result);

			memset(&search->entry.info, 0, sizeof(search->entry.info));
		} else {
			search->entry.errsource = 0;

			current = search_readnext(current, &result);
			assert(current && result);
			memset(search->entry.path, 0, CHIRP_PATH_MAX);
			strncpy(search->entry.path, result, CHIRP_PATH_MAX - 1);
			free(result);

			current = search_readnext(current, &result);
			assert(current && result);
			memset(&search->entry.info, 0, sizeof(search->entry.info));
			search_unpackstat(current, &search->entry.info);
			free(result);
		}
		search->current = current;
		return &search->entry;
	}
	return NULL;
}

int chirp_client_closesearch(CHIRP_SEARCH * search)
{
	buffer_free(&search->B);
	free(search);
	return 0;
}

INT64_T chirp_client_getxattr(struct chirp_client * c, const char *path, const char *name, void *data, size_t size, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	INT64_T result = send_command(c, stoptime, "getxattr %s %s\n", safepath, name);
	if(result < 0)
		return result;
	result = get_result(c, stoptime);
	if(result < 0) {
		if(errno == EINVAL)
			errno = ENOATTR;
		return result;
	} else if(result > (int) size) {
		link_soak(c->link, result, stoptime);
		errno = ERANGE;
		return result;
	}
	if(!link_read(c->link, data, result, stoptime)) {
		return -1;
	}
	return result;
}

INT64_T chirp_client_fgetxattr(struct chirp_client * c, INT64_T fd, const char *name, void *data, size_t size, time_t stoptime)
{
	INT64_T result = send_command(c, stoptime, "fgetxattr %lld %s\n", fd, name);
	if(result < 0)
		return result;

	result = get_result(c, stoptime);
	if(result < 0) {
		if(errno == EINVAL)
			errno = ENOATTR;
		return result;
	} else if(result > (int) size) {
		link_soak(c->link, result, stoptime);
		errno = ERANGE;
		return result;
	}
	if(!link_read(c->link, data, result, stoptime)) {
		return -1;
	}
	return result;
}

INT64_T chirp_client_lgetxattr(struct chirp_client * c, const char *path, const char *name, void *data, size_t size, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	INT64_T result = send_command(c, stoptime, "lgetxattr %s %s\n", safepath, name);
	if(result < 0)
		return result;
	result = get_result(c, stoptime);
	if(result < 0) {
		if(errno == EINVAL)
			errno = ENOATTR;
		return result;
	} else if(result > (int) size) {
		link_soak(c->link, result, stoptime);
		errno = ERANGE;
		return result;
	}
	if(!link_read(c->link, data, result, stoptime)) {
		return -1;
	}
	return result;
}

INT64_T chirp_client_listxattr(struct chirp_client * c, const char *path, char *list, size_t size, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	INT64_T result = send_command(c, stoptime, "listxattr %s\n", safepath);
	if(result < 0)
		return result;
	result = get_result(c, stoptime);
	if(result < 0)
		return result;
	if(result > (int) size) {
		link_soak(c->link, result, stoptime);
		errno = ERANGE;
		return result;
	}
	if(!link_read(c->link, list, result, stoptime)) {
		return -1;
	}
	return result;
}

INT64_T chirp_client_flistxattr(struct chirp_client * c, INT64_T fd, char *list, size_t size, time_t stoptime)
{
	INT64_T result = send_command(c, stoptime, "flistxattr %lld\n", fd);
	if(result < 0)
		return result;
	result = get_result(c, stoptime);
	if(result < 0)
		return result;
	if(result > (int) size) {
		link_soak(c->link, result, stoptime);
		errno = ERANGE;
		return result;
	}
	if(!link_read(c->link, list, result, stoptime)) {
		return -1;
	}
	return result;
}

INT64_T chirp_client_llistxattr(struct chirp_client * c, const char *path, char *list, size_t size, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	INT64_T result = send_command(c, stoptime, "llistxattr %s\n", safepath);
	if(result < 0)
		return result;
	result = get_result(c, stoptime);
	if(result < 0)
		return result;
	if(result > (int) size) {
		link_soak(c->link, result, stoptime);
		errno = ERANGE;
		return result;
	}
	if(!link_read(c->link, list, result, stoptime)) {
		return -1;
	}
	return result;
}

INT64_T chirp_client_setxattr(struct chirp_client * c, const char *path, const char *name, const void *data, size_t size, int flags, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	INT64_T result = send_command(c, stoptime, "setxattr %s %s %zu %d\n", safepath, name, size, flags);
	if(result < 0)
		return result;

	result = link_putlstring(c->link, data, size, stoptime);
	if(result != (int) size) {
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}

	result = get_result(c, stoptime);
	if(result < 0) {
		if(errno == EINVAL)
			errno = ENOATTR;
		return result;
	}

	return 0;
}

INT64_T chirp_client_fsetxattr(struct chirp_client * c, INT64_T fd, const char *name, const void *data, size_t size, int flags, time_t stoptime)
{
	INT64_T result = send_command(c, stoptime, "fsetxattr %s %s %zu %d\n", fd, name, size, flags);
	if(result < 0)
		return result;

	result = link_putlstring(c->link, data, size, stoptime);
	if(result != (int) size) {
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}

	result = get_result(c, stoptime);
	if(result < 0) {
		if(errno == EINVAL)
			errno = ENOATTR;
		return result;
	}

	return 0;
}

INT64_T chirp_client_lsetxattr(struct chirp_client * c, const char *path, const char *name, const void *data, size_t size, int flags, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	INT64_T result = send_command(c, stoptime, "lsetxattr %s %s %zu %d\n", safepath, name, size, flags);
	if(result < 0)
		return result;

	result = link_putlstring(c->link, data, size, stoptime);
	if(result != (int) size) {
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}

	result = get_result(c, stoptime);
	if(result < 0) {
		if(errno == EINVAL)
			errno = ENOATTR;
		return result;
	}

	return 0;
}

INT64_T chirp_client_removexattr(struct chirp_client * c, const char *path, const char *name, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	INT64_T result = simple_command(c, stoptime, "removexattr %s %s\n", safepath, name);
	if(result == -1 && errno == EINVAL)
		errno = ENOATTR;
	return result;
}

INT64_T chirp_client_fremovexattr(struct chirp_client * c, INT64_T fd, const char *name, time_t stoptime)
{
	INT64_T result = simple_command(c, stoptime, "fremovexattr %lld %s\n", fd, name);
	if(result == -1 && errno == EINVAL)
		errno = ENOATTR;
	return result;
}

INT64_T chirp_client_lremovexattr(struct chirp_client * c, const char *path, const char *name, time_t stoptime)
{
	char safepath[CHIRP_LINE_MAX];
	url_encode(path, safepath, sizeof(safepath));
	INT64_T result = simple_command(c, stoptime, "lremovexattr %s %s\n", safepath, name);
	if(result == -1 && errno == EINVAL)
		errno = ENOATTR;
	return result;
}

INT64_T chirp_client_job_create (struct chirp_client *c, const char *json, chirp_jobid_t *id, time_t stoptime)
{
	INT64_T result;
	size_t len = strlen(json);

	if(len >= MAX_BUFFER_SIZE) {
		errno = ENOMEM;
		return -1;
	}

	result = send_command(c, stoptime, "job_create %zu\n", len);
	if(result < 0)
		return result;

	result = link_putlstring(c->link, json, len, stoptime);
	if(result != (INT64_T)len) {
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}
	result = get_result(c, stoptime);
	if (result > 0) {
		*id = result;
		result = 0;
	}
	return result;
}

INT64_T chirp_client_job_commit (struct chirp_client *c, const char *json, time_t stoptime)
{
	INT64_T result;
	size_t len = strlen(json);

	if(len >= MAX_BUFFER_SIZE) {
		errno = ENOMEM;
		return -1;
	}

	result = send_command(c, stoptime, "job_commit %zu\n", len);
	if(result < 0)
		return result;

	result = link_putlstring(c->link, json, len, stoptime);
	if(result != (INT64_T)len) {
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}
	return get_result(c, stoptime);
}

INT64_T chirp_client_job_kill (struct chirp_client *c, const char *json, time_t stoptime)
{
	INT64_T result;
	size_t len = strlen(json);

	if(len >= MAX_BUFFER_SIZE) {
		errno = ENOMEM;
		return -1;
	}

	result = send_command(c, stoptime, "job_kill %zu\n", len);
	if(result < 0)
		return result;

	result = link_putlstring(c->link, json, len, stoptime);
	if(result != (INT64_T)len) {
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}
	return get_result(c, stoptime);
}

INT64_T chirp_client_job_status (struct chirp_client *c, const char *json, char **status, time_t stoptime)
{
	INT64_T result;
	size_t len = strlen(json);

	if(len >= MAX_BUFFER_SIZE) {
		errno = ENOMEM;
		return -1;
	}

	result = send_command(c, stoptime, "job_status %zu\n", len);
	if(result < 0)
		return result;

	result = link_putlstring(c->link, json, len, stoptime);
	if(result != (INT64_T)len) {
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}
	result = get_result(c, stoptime);

	if(result > 0) {
		INT64_T actual;

		if(result >= MAX_BUFFER_SIZE || (*status = realloc(NULL, result+1)) == NULL) {
			errno = ENOMEM;
			return -1;
		}

		memset(*status, 0, result+1);
		actual = link_read(c->link, *status, result, stoptime);
		if(actual != result) {
			*status = realloc(*status, 0);
			errno = ECONNRESET;
			return -1;
		}
	}

	return result;
}

INT64_T chirp_client_job_wait (struct chirp_client *c, chirp_jobid_t id, INT64_T timeout, char **status, time_t stoptime)
{
	INT64_T result;

	result = simple_command(c, stoptime, "job_wait %" PRICHIRP_JOBID_T " %" PRId64 "\n", id, timeout);
	if(result > 0) {
		INT64_T actual;

		if(result >= MAX_BUFFER_SIZE || (*status = realloc(NULL, result+1)) == NULL) {
			errno = ENOMEM;
			return -1;
		}

		memset(*status, 0, result+1);
		actual = link_read(c->link, *status, result, stoptime);
		if(actual != result) {
			*status = realloc(*status, 0);
			errno = ECONNRESET;
			return -1;
		}
	}

	return result;
}

INT64_T chirp_client_job_reap (struct chirp_client *c, const char *json, time_t stoptime)
{
	INT64_T result;
	size_t len = strlen(json);

	if(len >= MAX_BUFFER_SIZE) {
		errno = ENOMEM;
		return -1;
	}

	result = send_command(c, stoptime, "job_reap %zu\n", len);
	if(result < 0)
		return result;

	result = link_putlstring(c->link, json, len, stoptime);
	if(result != (INT64_T)len) {
		c->broken = 1;
		errno = ECONNRESET;
		return -1;
	}
	return get_result(c, stoptime);
}

/* vim: set noexpandtab tabstop=4: */
