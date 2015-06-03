/*
 * Copyright (C) 2013- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
*/

/* TODO
 *
 * o Job time limits.
 * o bind outputs always in some .__sandbox.jobid directory?
 * o more states to reduces SQL locks: STARTED, WAITED, BOUND, FINISHED
 * o do not hold database locks during file transfer (use intermediate files with rename)?
 * o binding of symlink outputs does not follow transaction semantics (also use intermediate file with rename?)
 */

#include "catch.h"
#include "chirp_acl.h"
#include "chirp_client.h"
#include "chirp_job.h"
#include "chirp_fs_local.h"
#include "chirp_reli.h"
#include "chirp_sqlite.h"

#include "auth_all.h"
#include "buffer.h"
#include "cctools.h"
#include "copy_stream.h"
#include "create_dir.h"
#include "debug.h"
#include "domain_name.h"
#include "fd.h"
#include "link.h"
#include "md5.h"
#include "path.h"
#include "pattern.h"
#include "sha1.h"
#include "string_array.h"
#include "sigdef.h"
#include "unlink_recursive.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>

#if defined(__linux__)
#	include <sys/syscall.h>
#	include "ioprio.h"
#endif

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>


extern char chirp_hostname[DOMAIN_NAME_MAX];
extern int chirp_port;

struct url_binding {
	char path[PATH_MAX]; /* task_path */
	char *url;
	struct url_binding *next;
};

int chirp_fs_local_job_dbinit (sqlite3 *db)
{
	static const char SQL[] =
		"CREATE TABLE IF NOT EXISTS LocalJob ("
		"	id INTEGER PRIMARY KEY REFERENCES Job (id),"
		"	pid INTEGER NOT NULL,"
		"	ppid INTEGER NOT NULL,"
		"	sandbox TEXT NOT NULL);";

	int rc;

	sqlcatchexec(db, SQL);

	rc = 0;
	goto out;
out:
	return rc;
}

static int jerror (sqlite3 *db, chirp_jobid_t id, const char *errmsg)
{
	static const char SQL[] =
		"UPDATE Job"
		"	SET"
		"		status = 'ERRORED',"
		"		time_error = strftime('%s', 'now'),"
		"		error = ?"
		"	WHERE id = ?;";

	int rc;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_text(stmt, 1, errmsg, -1, SQLITE_STATIC));
	sqlcatch(sqlite3_bind_int64(stmt, 2, (sqlite3_int64)id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " entered error state: `%s'", id, errmsg);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int handle_error (sqlite3 *db, chirp_jobid_t id, int rc)
{
	switch (rc) {
		/* no error */
		case 0:
			break;
		/* temporary error. */
		case EAGAIN:
		case EINTR:
			break;
		default:
			jerror(db, id, strerror(rc));
			break;
	}
	return rc;
}

static int kill_kindly (pid_t pid)
{
	int rc;

	/* N.B. There is no point in waiting for `pid' because one of its children
	 * may be ignoring the kinder termination signals. We have to go through
	 * the whole thing. However, if we get ESRCH (or any other error), this is
	 * indication no process matches the group -pid. */
	CATCHUNIX(kill(-pid, SIGTERM));
	usleep(50);
	CATCHUNIX(kill(-pid, SIGQUIT));
	usleep(50);
	CATCHUNIX(kill(-pid, SIGKILL));
	usleep(50);

	rc = 0;
	goto out;
out:
	return rc;
}

static int sandbox_create (char sandbox[PATH_MAX], chirp_jobid_t id)
{
	int rc;
	char path[PATH_MAX] = "";

	CATCHUNIX(snprintf(path, PATH_MAX, "/.__job.%" PRICHIRP_JOBID_T ".XXXXXX", id));
	if (rc >= PATH_MAX)
		CATCH(ENAMETOOLONG);
	CATCHUNIX(chirp_fs_local_resolve(path, sandbox));
	CATCHUNIX(mkdtemp(sandbox) ? 0 : -1);
	debug(D_DEBUG, "created new sandbox `%s'", sandbox);

	rc = 0;
	goto out;
out:
	return rc;
}

static int sandbox_delete (const char *sandbox)
{
	int rc;

	CATCHUNIX(unlink_recursive(sandbox));

	rc = 0;
	goto out;
out:
	return rc;
}

#define MAX_SIZE_HASH  (1<<24)
static int interpolate (chirp_jobid_t id, char task_path[CHIRP_PATH_MAX], char serv_path[CHIRP_PATH_MAX])
{
	int rc;
	char *mark;

	for (mark = serv_path; (mark = strchr(mark, '%')); ) {
		switch (mark[1]) {
			case 'g':
			case 'h':
			case 's':
			{
				/* replace with hash of task_path */
				unsigned char digest[SHA1_DIGEST_LENGTH];
				if (mark[1] == 'h')
					CATCHUNIX(sha1_file(task_path, digest) ? 0 : -1);
				else if (mark[1] == 'g')
					sqlite3_randomness(SHA1_DIGEST_LENGTH, digest);
				else if (mark[1] == 's') {
					struct stat64 buf;
					CATCHUNIX(stat64(task_path, &buf));
					if (buf.st_size <= MAX_SIZE_HASH) {
						CATCHUNIX(sha1_file(task_path, digest) ? 0 : -1);
					} else {
						sqlite3_randomness(SHA1_DIGEST_LENGTH, digest);
					}
				}

				if (strlen(serv_path)+sizeof(digest)*2 < CHIRP_PATH_MAX) {
					size_t i;
					memmove(mark+sizeof(digest)*2, mark+2, strlen(mark+2)+1);
					for (i = 0; i < sizeof(digest); i++) {
						char hex[3];
						CATCHUNIX(snprintf(hex, sizeof(hex), "%02X", (unsigned int)digest[i]));
						assert(rc == 2);
						mark[i*2] = hex[0];
						mark[i*2+1] = hex[1];
					}
					mark += sizeof(digest)*2;
				} else {
					CATCH(ENAMETOOLONG);
				}
				break;
			}
			case 'j':
			{
				char str[64];
				CATCHUNIX(snprintf(str, sizeof(str), "%" PRICHIRP_JOBID_T, id));
				assert((size_t)rc < sizeof(str));
				if (strlen(serv_path)+strlen(str) < CHIRP_PATH_MAX) {
					memmove(mark+strlen(str), mark+2, strlen(mark+2)+1);
					memcpy(mark, str, strlen(str));
					mark += strlen(str);
				} else {
					CATCH(ENAMETOOLONG);
				}
				break;
			}
			default:
				mark += 1; /* ignore */
				break;
		}
	}

	rc = 0;
	goto out;
out:
	return rc;
}

enum BINDSTATE { BOOTSTRAP, STRAPBOOT };
static int bindfile (sqlite3 *db, chirp_jobid_t id, const char *subject, const char *sandbox, struct url_binding **urlsp, const char *task_path, const char *serv_path, const char *binding, const char *type, enum BINDSTATE mode)
{
	static const char SQL[] =
		"UPDATE JobFile"
		"	SET serv_path = ?, size = ?"
		"	WHERE id = ? AND task_path = ? AND type = 'OUTPUT'";

	int rc;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	char task_path_resolved[CHIRP_PATH_MAX] = "";
	char serv_path_interpolated[CHIRP_PATH_MAX] = "";
	char serv_path_resolved[CHIRP_PATH_MAX] = "";
	char task_path_dir[CHIRP_PATH_MAX] = ""; /* directory containing task_path_resolved */

	CATCHUNIX(snprintf(task_path_resolved, sizeof(task_path_resolved), "%s/%s", sandbox, task_path));
	if ((size_t)rc >= sizeof(task_path_resolved))
		CATCH(ENAMETOOLONG);
	path_dirname(task_path_resolved, task_path_dir);

	if (strcmp(binding, "URL") == 0) {
		if (strcmp(type, "INPUT") != 0)
			CATCH(EINVAL);
		if (mode == BOOTSTRAP) {
			debug(D_DEBUG, "binding `%s' for future URL fetch `%s'", task_path, serv_path);
			CATCHUNIX(create_dir(task_path_dir, S_IRWXU) ? 0 : -1);
			struct url_binding *url = malloc(sizeof(struct url_binding));
			CATCHUNIX(url ? 0 : -1);
			memset(url, 0, sizeof(struct url_binding));
			strncpy(url->path, "", sizeof(url->path));
			url->url = NULL;
			url->next = *urlsp;
			*urlsp = url;
			CATCHUNIX(snprintf(url->path, sizeof(url->path), "%s", task_path));
			CATCHUNIX((url->url = strdup(serv_path)) ? 0 : -1);
		}
		rc = 0;
		goto out;
	}

	strncpy(serv_path_interpolated, serv_path, sizeof(serv_path_interpolated)-1);
	if (mode == STRAPBOOT && strcmp(type, "OUTPUT") == 0)
		CATCH(interpolate(id, task_path_resolved, serv_path_interpolated));
	serv_path = serv_path_interpolated;
	CATCH(chirp_fs_local_resolve(serv_path, serv_path_resolved));

	if (mode == BOOTSTRAP) {
		debug(D_DEBUG, "binding `%s' as `%s'", task_path, serv_path);

		CATCHUNIX(create_dir(task_path_dir, S_IRWXU) ? 0 : -1);

		if (strcmp(type, "INPUT") == 0) {
			if (strcmp(binding, "LINK") == 0) {
				CATCHUNIX(link(serv_path_resolved, task_path_resolved));
			} else if (strcmp(binding, "COPY") == 0) {
				CATCHUNIX(copy_file_to_file(serv_path_resolved, task_path_resolved));
			} else assert(0);
			CATCHUNIX(chmod(serv_path_resolved, S_IRWXU));
		}
	} else if (mode == STRAPBOOT) {
		if (strcmp(type, "OUTPUT") == 0) {
			struct stat info;

			debug(D_DEBUG, "binding output file `%s' as `%s'", task_path, serv_path);
			if (strcmp(binding, "LINK") == 0) {
				CATCHUNIXIGNORE(unlink(serv_path_resolved), ENOENT); /* ignore error/success */
				CATCHUNIX(link(task_path_resolved, serv_path_resolved));
			} else if (strcmp(binding, "COPY") == 0) {
				CATCHUNIXIGNORE(unlink(serv_path_resolved), ENOENT);
				CATCHUNIX(copy_file_to_file(task_path_resolved, serv_path_resolved));
			}

			if (stat(serv_path_resolved, &info) == 0) {
				sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
				sqlcatch(sqlite3_bind_text(stmt, 1, serv_path, -1, SQLITE_STATIC));
				sqlcatch(sqlite3_bind_int64(stmt, 2, info.st_size));
				sqlcatch(sqlite3_bind_int64(stmt, 3, (sqlite3_int64)id));
				sqlcatch(sqlite3_bind_text(stmt, 4, task_path, -1, SQLITE_STATIC));
				sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
				sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
			}
		}
	} else assert(0);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int jbindfiles (sqlite3 *db, chirp_jobid_t id, const char *subject, const char *sandbox, struct url_binding **urlsp, enum BINDSTATE mode)
{
	static const char SQL[] =
		"SELECT task_path, serv_path, binding, type"
		"	FROM JobFile"
		"	WHERE id = ?"
		"	ORDER BY task_path;";

	int rc;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		rc = bindfile(db, id, subject, sandbox, urlsp, (const char *)sqlite3_column_text(stmt, 0), (const char *)sqlite3_column_text(stmt, 1), (const char *)sqlite3_column_text(stmt, 2), (const char *)sqlite3_column_text(stmt, 3), mode);
		if (mode == BOOTSTRAP) {
			CATCH(rc);
		} else if (mode == STRAPBOOT) {
			switch (rc) {
				case 0:
				case ENOENT:
					break;
				default:
					CATCH(rc);
					break;
			}
		} else assert(0);
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int jgetargs (sqlite3 *db, chirp_jobid_t id, char ***args)
{
	static const char SQL[] =
		"SELECT n, arg"
		"	FROM JobArgument"
		"	WHERE id = ?"
		"	ORDER BY n;";

	int rc;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	*args = string_array_new();
	uint64_t n;
	BUFFER_STACK(B, 4096);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	for (n = 1; (rc = sqlite3_step(stmt)) == SQLITE_ROW; n++)
	{
		assert(((uint64_t)sqlite3_column_int64(stmt, 0)) == n);
		const char *arg = (const char *) sqlite3_column_text(stmt, 1);
		*args = string_array_append(*args, arg);
		if (n == 1)
			buffer_putfstring(B, "`%s'", arg);
		else
			buffer_putfstring(B, ", `%s'", arg);
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	debug(D_DEBUG, "jobs[%" PRICHIRP_JOBID_T "].args = {%s}", id, buffer_tostring(B));

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static const char *readenv (char *const *env, const char *name)
{
	for (; *env; env++)
		if (strncmp(*env, name, strlen(name)) == 0 && (*env)[strlen(name)] == '=')
			return &(*env)[strlen(name)+1];
	return NULL;
}

static int envinsert (char ***env, const char *name, const char *fmt, ...)
{
	int rc;
	buffer_t B[1];
	buffer_init(B);

	if (!readenv(*env, name)) {
		va_list ap;

		CATCHUNIX(buffer_putfstring(B, "%s=", name));

		va_start(ap, fmt);
		rc = buffer_putvfstring(B, fmt, ap);
		va_end(ap);
		CATCHUNIX(rc);
		*env = string_array_append(*env, buffer_tostring(B));
	}

	rc = 0;
	goto out;
out:
	buffer_free(B);
	return rc;
}

static int envdefaults (char ***env, chirp_jobid_t id, const char *subject, const char *sandbox)
{
	int rc;

	CATCH(envinsert(env, "CHIRP_SUBJECT", "%s", subject));
	CATCH(envinsert(env, "CHIRP_HOST", "%s:%d", chirp_hostname, chirp_port));
	CATCH(envinsert(env, "HOME", "%s", sandbox));
	CATCH(envinsert(env, "LANG", "C"));
	CATCH(envinsert(env, "LC_ALL", "C"));
	CATCH(envinsert(env, "PATH", "/bin:/usr/bin:/usr/local/bin"));
	CATCH(envinsert(env, "PWD", "%s", sandbox));
	CATCH(envinsert(env, "TMPDIR", "%s", sandbox));
	CATCH(envinsert(env, "USER", "chirp"));

	rc = 0;
	goto out;
out:
	return rc;
}

static int jgetenv (sqlite3 *db, chirp_jobid_t id, const char *subject, const char *sandbox, char ***env)
{
	static const char SQL[] =
		"SELECT name, value"
		"	FROM JobEnvironment"
		"	WHERE id = ?"
		"	ORDER BY name;";

	int rc;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	*env = string_array_new();
	buffer_t B[1];
	buffer_init(B);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
	{
		assert(sqlite3_column_count(stmt) == 2);
		assert(sqlite3_column_type(stmt, 0) == SQLITE_TEXT);
		assert(sqlite3_column_type(stmt, 1) == SQLITE_TEXT);
		const char *name = (const char *) sqlite3_column_text(stmt, 0);
		const char *value = (const char *) sqlite3_column_text(stmt, 1);
		debug(D_DEBUG, "jobs[%" PRICHIRP_JOBID_T "].environment[`%s'] = `%s'", id, name, value);
		buffer_rewind(B, 0);
		CATCHUNIX(buffer_putfstring(B, "%s=%s", name, value));
		*env = string_array_append(*env, buffer_tostring(B));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	CATCH(envdefaults(env, id, subject, sandbox)); /* ideally these would be set at the start and then replaced but it's awkward to replace entries in the char *[] */

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	buffer_free(B);
	return rc;
}

static void do_put (char *const argv[], char *const envp[])
{
	int rc;
	FILE *stream;
	struct stat info;
	time_t stoptime;

	debug(D_CHIRP, "do_put('%s', '%s', '%s')", argv[1], argv[2], argv[3]);

	stream = fopen(argv[2], "r");
	CATCHUNIX(stream == NULL ? -1 : 0);
	CATCHUNIX(fstat(fileno(stream), &info));
	stoptime = time(NULL) + 120 + info.st_size/1024; /* anything less than 1KB/s is unacceptable */

	CATCHUNIX(chirp_reli_putfile(argv[1], argv[3], stream, S_IRUSR, info.st_size, stoptime));

	rc = 0;
	goto out;
out:
	exit(rc);
}

static void do_hash (char *const argv[], char *const envp[])
{
	int rc;

	debug(D_CHIRP, "do_hash('%s', '%s')", argv[1], argv[2]);

	if (strcmp(argv[1], "sha1") == 0) {
		unsigned char digest[SHA1_DIGEST_LENGTH];
		CATCHUNIX(sha1_file(argv[2], digest) ? 0 : -1);
	} else if (strcmp(argv[2], "md5") == 0) {
		unsigned char digest[MD5_DIGEST_LENGTH];
		CATCHUNIX(md5_file(argv[2], digest) ? 0 : -1);
	} else {
		CATCH(EXIT_FAILURE);
	}

	rc = 0;
	goto out;
out:
	exit(rc);
}

static int geturl (const struct url_binding *url)
{
	int rc;
	char *hostport = NULL;
	char *rest = NULL;
	const char *urls = url->url;
	FILE *output = NULL;

	/* urls may point to multiple sources, delimited by whitespace */
	debug(D_DEBUG, "url '%s'", url->url);

	output = fopen(url->path, "w");
	CATCHUNIX(output ? 0 : -1);
	CATCHUNIX(fchmod(fileno(output), S_IRUSR|S_IWUSR|S_IXUSR));

	do {
		size_t n;
		if (pattern_match(urls, "^chirp://([^/,%s]+)(%S*)%s*()", &hostport, &rest, &n) == 0) {
			debug(D_CHIRP, "sourcing '%s' via 'chirp://%s%s'", url->path, hostport, rest);
			struct chirp_stat buf;
			time_t timeout = time(NULL) + 120;
			rc = chirp_reli_stat(hostport, rest, &buf, timeout);
			if (rc == 0) {
				timeout = time(NULL) + 120 + buf.cst_size/1024; /* anything less than 1KB/s is unacceptable */
				rewind(output);
				int64_t rc64 = chirp_reli_getfile(hostport, rest, output, timeout);
				if (rc64 >= 0) {
					rc = 0;
					goto out;
				} else {
					rc = errno;
					debug(D_DEBUG, "getfile failed with rc %d: %s", rc, strerror(rc));
				}
			} else {
				rc = errno;
				debug(D_DEBUG, "stat failed with rc %d: %s", rc, strerror(rc));
			}
		} else if (pattern_match(url->url, "^https?://%S*") == 0) {
			assert(0); /* TODO */
		} else CATCH(EINVAL);
		urls += n;
	} while (*urls);

	rc = ENOENT;
	goto out;
out:
	if (output)
		fclose(output);
	free(hostport);
	free(rest);
	if (rc)
		debug(D_DEBUG, "failed to fetch file: %s", strerror(rc));
	return rc;
}

static void run (const char *sandbox, const char *path, char *const argv[], char *const envp[], const struct url_binding *urls)
{
	int rc;
	int fd;
	/* TODO don't propagate CHIRP_CLIENT_TICKETS across execve. */

	signal(SIGUSR1, SIG_DFL);

	/* reassign std files and close everything else */
	CATCH(fd_nonstd_close());
	CATCH(fd_null(STDIN_FILENO, O_RDONLY));
	CATCH(fd_null(STDOUT_FILENO, O_WRONLY));
	CATCH(fd_null(STDERR_FILENO, O_WRONLY));
	CATCHUNIX(chdir(sandbox));
	CATCHUNIX(setpgid(0, 0)); /* create new process group */

	/* write debug information to `debug': can be examined by the user by adding an OUTPUT file */
	debug_config("chirp@job");
	debug_flags_clear();
	debug_flags_set("all");
	debug_config_file(".chirp.debug");
	cctools_version_debug(D_DEBUG, "chirp@job");

	/* This is closed by exit(3) or via execve. */
	CATCHUNIX(fd = open(".chirp.stats", O_CREAT|O_TRUNC|O_WRONLY, S_IRUSR|S_IWUSR));
	CATCHUNIX(fcntl(fd, F_GETFD));
	CATCHUNIX(fcntl(fd, F_SETFD, rc|FD_CLOEXEC));
	FILE *log = fdopen(fd, "w");
	CATCHUNIX(log == NULL ? -1 : 0);
	link_stats(log);

#if defined(__linux__) && defined(SYS_ioprio_get)
	/* Reduce the iopriority of jobs below the scheduler. */
	CATCHUNIX(syscall(SYS_ioprio_get, IOPRIO_WHO_PROCESS, 0));
	debug(D_CHIRP, "iopriority: %d:%d", (int)IOPRIO_PRIO_CLASS(rc), (int)IOPRIO_PRIO_DATA(rc));
	CATCHUNIX(syscall(SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0, IOPRIO_PRIO_VALUE(IOPRIO_CLASS_BE, 1)));
	if (rc == 0)
		debug(D_CHIRP, "iopriority set: %d:%d", (int)IOPRIO_CLASS_BE, 1);
	else assert(0);
	CATCHUNIX(syscall(SYS_ioprio_get, IOPRIO_WHO_PROCESS, 0));
	assert(IOPRIO_PRIO_CLASS(rc) == IOPRIO_CLASS_BE && IOPRIO_PRIO_DATA(rc) == 1);
#endif /* defined(__linux__) && defined(SYS_ioprio_get) */

	/* order matters! */
	auth_clear();
	auth_ticket_register();
	auth_ticket_load(readenv(envp, CHIRP_CLIENT_TICKETS));
	auth_hostname_register();
	auth_address_register();

	{
		const struct url_binding *url = urls;
		while (url) {
			CATCH(geturl(url));
			url = url->next;
		}
	}

	if (strcmp(path, "@put") == 0) {
		do_put(argv, envp);
	} else if (strcmp(path, "@hash") == 0) {
		do_hash(argv, envp);
	} else {
		buffer_t B[1];
		buffer_init(B);
		int i;
		for (i = 0; argv[i]; i++) {
			if (i)
				buffer_putfstring(B, ", '%s'", argv[i]);
			else
				buffer_putfstring(B, "'%s'", argv[i]);
		}
		debug(D_CHIRP, "execve('%s', [%s], [...])", path, buffer_tostring(B));
		buffer_free(B);
		CATCHUNIX(execve(path, argv, envp));
	}

	rc = 0;
	goto out;
out:
	debug(D_FATAL, "execution failed: %s", strerror(rc));
	raise(SIGUSR1); /* signal abnormal termination before program starts */
	abort();
}

static int jexecute (pid_t *pid, chirp_jobid_t id, const char *sandbox, const char *path, char *const argv[], char *const envp[], const struct url_binding *urls)
{
	int rc = 0;
	*pid = fork();
	if (*pid == 0) { /* child */
		run(sandbox, path, argv, envp, urls);
	} else if (*pid > 0) { /* parent */
		setpgid(*pid, 0); /* handle race condition */
		debug(D_CHIRP, "job %" PRICHIRP_JOBID_T " started as pid %d", id, (int)*pid);
		rc = 0;
	} else {
		debug(D_NOTICE, "could not fork: %s", strerror(errno));
		rc = errno;
	}
	return rc;
}

static int jstart (sqlite3 *db, chirp_jobid_t id, const char *executable, const char *subject, int priority)
{
	static const char SQL[] =
		"BEGIN EXCLUSIVE TRANSACTION;"
		"UPDATE Job"
		"	SET"
		"		status = 'STARTED',"
		"		time_start = strftime('%s', 'now')"
		"	WHERE id = ?;"
		"INSERT OR REPLACE INTO LocalJob (id, pid, ppid, sandbox)"
		"	VALUES (?, ?, ?, ?);"
		"END TRANSACTION;";

	int rc;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	pid_t pid = 0;
	char sandbox[PATH_MAX] = "";
	char **arguments = NULL;
	char **environment = NULL;
	struct url_binding *urls = NULL;

	debug(D_DEBUG, "jstart j = %" PRICHIRP_JOBID_T " e = `%s' s = `%s' p = %d", id, executable, subject, priority);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	CATCH(sandbox_create(sandbox, id));
	CATCH(jgetargs(db, id, &arguments));
	CATCH(jgetenv(db, id, subject, sandbox, &environment));
	CATCH(jbindfiles(db, id, subject, sandbox, &urls, BOOTSTRAP));
	CATCH(jexecute(&pid, id, sandbox, executable, arguments, environment, urls));

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	sqlcatch(sqlite3_bind_int64(stmt, 2, (sqlite3_int64)pid));
	sqlcatch(sqlite3_bind_int64(stmt, 3, (sqlite3_int64)getpid()));
	sqlcatch(sqlite3_bind_text(stmt, 4, sandbox, -1, SQLITE_STATIC));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	if (rc) {
		if (pid)
			kill_kindly(pid);
		if (sandbox[0])
			sandbox_delete(sandbox);
	}
	free(arguments);
	free(environment);
	while (urls) {
		struct url_binding *next = urls->next;
		free(urls->url);
		free(urls);
		urls = next;
	}
	return rc;
}

static int jwait (sqlite3 *db, unsigned *count, chirp_jobid_t id, const char *subject, pid_t pid, const char *sandbox)
{
	static const char SQL[] =
		/* We need to establish an EXCLUSIVE lock so we can store the results of waitpid. */
		"BEGIN EXCLUSIVE TRANSACTION;"
		"UPDATE Job"
		"    SET exit_code = ?2,"
		"        exit_status = ?3,"
		"        exit_signal = ?4,"
		"        status = 'FINISHED',"
		"        time_finish = strftime('%s', 'now')"
		"    WHERE id = ?1 AND status = 'STARTED';"
		"DELETE FROM LocalJob WHERE id = ?1;"
		"END TRANSACTION;";

	int rc;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	int status;
	struct url_binding *urls = NULL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	pid_t wpid = waitpid(pid, &status, WNOHANG);
	CATCHUNIX(wpid);
	if (wpid == 0)
		THROW_QUIET(EAGAIN);

	assert(wpid == pid);
	if (WIFEXITED(status))
		debug(D_DEBUG, "%d exited normally status = %d", (int)pid, WEXITSTATUS(status));
	else if (WIFSIGNALED(status))
		debug(D_DEBUG, "%d exited abnormally due to signal %d", (int)pid, WTERMSIG(status));

	*count -= 1;
	CATCH(jbindfiles(db, id, subject, sandbox, &urls, STRAPBOOT));
	sandbox_delete(sandbox);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	if (WIFEXITED(status)) {
		sqlcatch(sqlite3_bind_int64(stmt, 2, (sqlite3_int64)WEXITSTATUS(status)));
		sqlcatch(sqlite3_bind_text(stmt, 3, "EXITED", -1, SQLITE_STATIC));
		sqlcatch(sqlite3_bind_null(stmt, 4));
	} else if (WIFSIGNALED(status)) {
		sqlcatch(sqlite3_bind_null(stmt, 2));
		sqlcatch(sqlite3_bind_text(stmt, 3, "SIGNALED", -1, SQLITE_STATIC));
		sqlcatch(sqlite3_bind_text(stmt, 4, sigdefstr(WTERMSIG(status)), -1, SQLITE_STATIC));
	} else {
		assert(0);
	}
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " entered finished state: %d", id, status);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	while (urls) {
		struct url_binding *next = urls->next;
		free(urls->url);
		free(urls);
		urls = next;
	}
	return rc;
}

static int job_wait (sqlite3 *db, unsigned *count)
{
	static const char SQL[] =
		"SELECT Job.id, subject, pid, sandbox"
		"	FROM Job NATURAL JOIN LocalJob"
		"	WHERE status = 'STARTED'"
		"	ORDER BY RANDOM();"; /* to prevent starvation due to constant transaction ROLLBACK */

	int rc;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		assert(sqlite3_column_count(stmt) == 4);
		assert(sqlite3_column_type(stmt, 0) == SQLITE_INTEGER);
		assert(sqlite3_column_type(stmt, 1) == SQLITE_TEXT);
		assert(sqlite3_column_type(stmt, 2) == SQLITE_INTEGER);
		assert(sqlite3_column_type(stmt, 3) == SQLITE_TEXT);
		rc = handle_error(db, sqlite3_column_int64(stmt, 0), jwait(db, count, sqlite3_column_int64(stmt, 0), (const char *)sqlite3_column_text(stmt, 1), sqlite3_column_int64(stmt, 2), (const char *)sqlite3_column_text(stmt, 3)));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int jkill (sqlite3 *db, unsigned *count, chirp_jobid_t id, pid_t pid, const char *sandbox)
{
	static const char SQL[] =
		"DELETE FROM LocalJob WHERE id = ?;";

	int rc;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	int status;

	debug(D_DEBUG, "killing job %" PRICHIRP_JOBID_T " with pid = %d with sandbox = `%s'", id, (int)pid, sandbox);

	kill_kindly(pid);
	sandbox_delete(sandbox);

	pid_t wpid = waitpid(pid, &status, WNOHANG); /* we use WNOHANG because the child might be in an unkillable state... we can't do anything about that. */
	debug(D_DEBUG, "[%s:%d] %d = waitpid(%d, %p, WNOHANG)", __FILE__, __LINE__, (int)wpid, (int)pid, &status);
	if (wpid == pid) {
		*count -= 1;
		debug(D_DEBUG, "status = %d; WIFEXITED(status) = %d; WEXITSTATUS(status) = %d; WIFSIGNALED(status) = %d; WTERMSIG(status) = %d", status, WIFEXITED(status), WEXITSTATUS(status), WIFSIGNALED(status), WTERMSIG(status));
		if (WIFSTOPPED(status) || WIFCONTINUED(status)) {
			/* ignore this, probably debugging of some kind; try again next time... */
			rc = 0;
			goto out;
		}
		/* else process ended... */
	} else if (wpid == -1) {
		if (errno == ECHILD) {
			/* This indicates a child from a previous Chirp instance.
			 * KILLED is a terminal state, cannot change to ERRORED. So we
			 * just remove it from LocalJob. */
		} else if (errno == EINTR) {
			rc = 0;
			goto out;
		} else {
			assert(0);
		}
	} else {
		/* else we killed it but we cannot "wait" for it yet, we'll try again next time... */
		rc = 0;
		goto out;
	}

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int job_kill (sqlite3 *db, unsigned *count)
{
	static const char SQL[] =
		"SELECT Job.id, LocalJob.pid, LocalJob.sandbox"
		"	FROM Job NATURAL JOIN LocalJob"
		"	WHERE Job.status = 'KILLED';";

	int rc;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		assert(sqlite3_column_count(stmt) == 3);
		assert(sqlite3_column_type(stmt, 0) == SQLITE_INTEGER);
		assert(sqlite3_column_type(stmt, 1) == SQLITE_INTEGER);
		assert(sqlite3_column_type(stmt, 2) == SQLITE_TEXT);
		jkill(db, count, sqlite3_column_int64(stmt, 0), sqlite3_column_int64(stmt, 1), (const char *)sqlite3_column_text(stmt, 2));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int job_schedule_fifo (sqlite3 *db, unsigned *count)
{
	static const char SQL[] =
		"SELECT id, executable, subject, priority"
		"	FROM Job"
		"	WHERE status = 'COMMITTED'"
		"	ORDER BY priority, time_commit;";

	int rc;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((chirp_job_concurrency == 0 || *count < chirp_job_concurrency) && (rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		assert(sqlite3_column_count(stmt) == 4);
		assert(sqlite3_column_type(stmt, 0) == SQLITE_INTEGER);
		assert(sqlite3_column_type(stmt, 1) == SQLITE_TEXT);
		assert(sqlite3_column_type(stmt, 2) == SQLITE_TEXT);
		assert(sqlite3_column_type(stmt, 3) == SQLITE_INTEGER);
		rc = handle_error(db, sqlite3_column_int64(stmt, 0), jstart(db, sqlite3_column_int64(stmt, 0), (const char *)sqlite3_column_text(stmt, 1), (const char *)sqlite3_column_text(stmt, 2), sqlite3_column_int(stmt, 3)));
		if (rc == 0)
			*count += 1;
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

int chirp_fs_local_job_schedule (sqlite3 *db)
{
	int rc;
	unsigned count = 0; /* FIXME get rid of this, do a query to see what's running? */
	time_t update = 0;
	struct chirp_statfs buf;

	CATCHUNIX(chirp_fs_local.statfs("/", &buf));
	/* check if root is on AFS */
	if (buf.f_type == 0x5346414F /* AFS_SUPER_MAGIC */) {
		debug(D_ERROR|D_CHIRP, "Chirp jobs with LINK file bindings will not work with --root on AFS.");
	}

	/* continue until parent dies */
	while (getppid() != 1) {
		time_t now = time(NULL);
		if (update+30 < now) {
			debug(D_DEBUG, "%u jobs running", count);
			update = now;
		}

		/* Look at jobs that are executing, try to wait. */
		job_wait(db, &count);

		/* Look at jobs that are in `KILLED' state, kill any running killed jobs */
		job_kill(db, &count);

		/* Look at jobs waiting in `COMMIT' state, schedule if we can */
		job_schedule_fifo(db, &count);

		usleep(50000);
	}

	rc = 0;
	goto out;
out:
	return rc;
}

/* vim: set noexpandtab tabstop=4: */
