#include "chirp_acl.h"
#include "chirp_job.h"
#include "chirp_fs_local.h"
#include "chirp_sqlite.h"

#include "copy_stream.h"
#include "create_dir.h"
#include "debug.h"
#include "delete_dir.h"
#include "domain_name.h"
#include "fd.h"
#include "path.h"
#include "string_array.h"
#include "xxmalloc.h"

#include <unistd.h>
#ifdef CCTOOLS_OPSYS_LINUX
#include <sched.h>
#endif
#include <sys/wait.h>

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>

#define CATCH(expr) \
	do {\
		rc = (expr);\
		if (rc) {\
			if (rc == -1) {\
				debug(D_DEBUG, "[%s:%d] generic error: %d `%s'", __FILE__, __LINE__, rc, strerror(errno));\
				rc = errno;\
			} else {\
				debug(D_DEBUG, "[%s:%d] generic error: %d `%s'", __FILE__, __LINE__, rc, strerror(rc));\
			}\
			goto out;\
		}\
	} while (0)

#define CATCHCODE(expr, code) \
	do {\
		rc = (expr);\
		if (rc == (code)) {\
			if (rc == -1) {\
				debug(D_DEBUG, "[%s:%d] generic error: %d `%s'", __FILE__, __LINE__, rc, strerror(errno));\
				rc = errno;\
			} else {\
				debug(D_DEBUG, "[%s:%d] generic error: %d `%s'", __FILE__, __LINE__, rc, strerror(rc));\
			}\
			goto out;\
		}\
	} while (0)

extern char chirp_hostname[DOMAIN_NAME_MAX];
extern int chirp_port;
extern char chirp_transient_path[PATH_MAX];

int chirp_fs_local_job_dbinit (sqlite3 *db)
{
	static const char Init[] =
		"CREATE TABLE LocalJobs ( id INTEGER PRIMARY KEY,"
		"                         pid INTEGER NOT NULL,"
		"                         ppid INTEGER NOT NULL,"
		"                         sandbox TEXT NOT NULL,"
		"                         waited INTEGER NOT NULL DEFAULT 0,"
		"                         FOREIGN KEY (id) REFERENCES Jobs (id));";

	int rc;
	sqlcatchexec(db, Init);

	rc = 0;
out:
	return rc;
}

static int kill_kindly (pid_t pid)
{
	int rc;

	/* N.B. There is no point in waiting for `pid' because one of its children
	 * may be ignoring the kinder termination signals. We have to go through
	 * the whole thing. However, if we get ESRCH (or any other error), this is
	 * indication no process matches the group -pid. */
	CATCHCODE(kill(-pid, SIGTERM), -1);
	usleep(50);
	CATCHCODE(kill(-pid, SIGQUIT), -1);
	usleep(50);
	CATCHCODE(kill(-pid, SIGKILL), -1);
out:
	return rc;
}

static int sandbox_create (char sandbox[PATH_MAX])
{
	int n = snprintf(sandbox, PATH_MAX, "%s/chirp.job.XXXXXX", chirp_transient_path);
	if (n == -1 || n >= PATH_MAX)
		return errno;
	if (mkdtemp(sandbox) == NULL)
		return errno;
	debug(D_DEBUG, "created new sandbox `%s'", sandbox);
	return 0;
}

static int sandbox_delete (const char *sandbox)
{
	int rc;
	CATCHCODE(delete_dir(sandbox), -1);

	rc = 0;
out:
	return rc;
}

enum BINDSTATE { BOOTSTRAP, STRAPBOOT };
static int bindfile (const char *subject, const char *sandbox, const char *task_path, const char *serv_path, const char *binding, const char *type, enum BINDSTATE mode)
{
	int rc;
	char task_path_resolved[CHIRP_PATH_MAX];
	char task_path_dir[CHIRP_PATH_MAX]; /* directory containing task_path_resolved */
	char serv_path_resolved[CHIRP_PATH_MAX];

	/* TODO in future work, this path resolution is done at the FS layer transparently. */
	CATCH(chirp_fs_local_resolve(serv_path, serv_path_resolved));
	debug(D_DEBUG, "`%s' --> `%s'", serv_path, serv_path_resolved);
	CATCHCODE(snprintf(task_path_resolved, sizeof(task_path_resolved), "%s/%s", sandbox, task_path), -1);
	if ((size_t)rc >= sizeof(task_path_resolved))
		return ENAMETOOLONG;
	path_dirname(task_path_resolved, task_path_dir);
	if (mode == BOOTSTRAP) {
		debug(D_DEBUG, "binding `%s' as `%s'", task_path, serv_path);
		CATCHCODE(create_dir(task_path_dir, 0700), 0);
		if (strcmp(type, "INPUT") == 0) {
			CATCH(chirp_acl_check(serv_path, subject, CHIRP_ACL_READ) ? 0 : -1);
			if (strcmp(binding, "SYMLINK") == 0) {
				CATCHCODE(symlink(serv_path_resolved, task_path_resolved), -1);
			} else if (strcmp(binding, "LINK") == 0) {
				CATCHCODE(link(serv_path_resolved, task_path_resolved), -1);
			} else if (strcmp(binding, "COPY") == 0) {
				CATCHCODE(copy_file_to_file(serv_path_resolved, task_path_resolved), -1);
			} else {
				assert(0);
			}
		} else if (strcmp(type, "OUTPUT") == 0) {
			CATCH(chirp_acl_check(serv_path, subject, CHIRP_ACL_WRITE) ? 0 : -1);
			if (strcmp(binding, "SYMLINK") == 0) {
				CATCHCODE(symlink(serv_path_resolved, task_path_resolved), -1);
			}
		} else {
			assert(0);
		}
	} else {
		if (strcmp(type, "OUTPUT") == 0) {
			debug(D_DEBUG, "binding output file `%s' as `%s'", task_path, serv_path);
			CATCH(chirp_acl_check(serv_path, subject, CHIRP_ACL_WRITE) ? 0 : -1);
			if (strcmp(binding, "LINK") == 0) {
				CATCHCODE(link(task_path_resolved, serv_path_resolved), -1);
			} else if (strcmp(binding, "COPY") == 0) {
				CATCHCODE(copy_file_to_file(task_path_resolved, serv_path_resolved), -1);
			}
		}
	}
	rc = 0;
out:
	return rc;
}

static int jbindfiles (sqlite3 *db, chirp_jobid_t id, const char *subject, const char *sandbox, enum BINDSTATE mode)
{
	static const char JobFiles[] =
		"SELECT task_path, serv_path, binding, type"
		"    FROM JobFiles"
		"    WHERE id = ?"
		"    ORDER BY task_path;";

	sqlite3_stmt *stmt = NULL;
	int rc;

	sqlcatch(sqlite3_prepare_v2(db, JobFiles, sizeof(JobFiles), &stmt, NULL));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
	{
		assert(sqlite3_column_count(stmt) == 4);
		assert(sqlite3_column_type(stmt, 0) == SQLITE_TEXT);
		assert(sqlite3_column_type(stmt, 1) == SQLITE_TEXT);
		assert(sqlite3_column_type(stmt, 2) == SQLITE_TEXT);
		assert(sqlite3_column_type(stmt, 3) == SQLITE_TEXT);
		CATCH(bindfile(subject, sandbox, (const char *)sqlite3_column_text(stmt, 0), (const char *)sqlite3_column_text(stmt, 1), (const char *)sqlite3_column_text(stmt, 2), (const char *)sqlite3_column_text(stmt, 3), mode));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	rc = 0;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int jgetargs (sqlite3 *db, chirp_jobid_t id, char ***args)
{
	static const char JobArguments[] =
		"SELECT n, arg"
		"    FROM JobArguments"
		"    WHERE id = ?"
		"    ORDER BY n;";

	*args = string_array_new();
	sqlite3_stmt *stmt = NULL;
	int rc;
	uint64_t n;

	sqlcatch(sqlite3_prepare_v2(db, JobArguments, sizeof(JobArguments), &stmt, NULL));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	for (n = 1; (rc = sqlite3_step(stmt)) == SQLITE_ROW; n++)
	{
		assert(sqlite3_column_count(stmt) == 2);
		assert(sqlite3_column_type(stmt, 0) == SQLITE_INTEGER);
		assert(sqlite3_column_type(stmt, 1) == SQLITE_TEXT);
		assert(((uint64_t)sqlite3_column_int64(stmt, 0)) == n);
		const char *arg = (const char *) sqlite3_column_text(stmt, 1);
		*args = string_array_append(*args, arg);
		debug(D_DEBUG, "jobs[%" PRICHIRP_JOBID_T "].args[%u] = `%s'", id, (unsigned)n-1, arg);
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	rc = 0;
out:
	sqlite3_finalize(stmt);
	if (rc) {
		free(*args);
		*args = NULL;
	}
	return rc;
}

static int inenv (char *const *env, const char *name)
{
	for (; *env; env++)
		if (strncmp(*env, name, strlen(name)) == 0 && (*env)[strlen(name)] == '=')
			return 1;
	return 0;
}

static int envinsert (char ***env, const char *name, const char *fmt, ...)
{
	int rc;
	if (!inenv(*env, name)) {
		va_list ap;
		char buf[1<<15];

		CATCHCODE(snprintf(buf, sizeof(buf), "%s=", name), -1);
		if ((size_t)rc >= sizeof(buf))
			return ENAMETOOLONG;

		va_start(ap, fmt);
		vsnprintf(buf+strlen(buf), sizeof(buf)-strlen(buf), fmt, ap);
		va_end(ap);
		CATCHCODE(rc, -1);
		if ((size_t)rc >= sizeof(buf))
			return ENAMETOOLONG;
		*env = string_array_append(*env, buf);
	}
	rc = 0;
out:
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
out:
	return rc;
}

static int jgetenv (sqlite3 *db, chirp_jobid_t id, const char *subject, const char *sandbox, char ***env)
{
	static const char JobEnvironment[] =
		"SELECT name, value"
		"    FROM JobEnvironment"
		"    WHERE id = ?"
		"    ORDER BY name;";

	*env = string_array_new();
	sqlite3_stmt *stmt = NULL;
	int rc;

	sqlcatch(sqlite3_prepare_v2(db, JobEnvironment, sizeof(JobEnvironment), &stmt, NULL));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
	{
		int n;
		char buf[1<<15];
		char *entry = buf;
		assert(sqlite3_column_count(stmt) == 2);
		assert(sqlite3_column_type(stmt, 0) == SQLITE_TEXT);
		assert(sqlite3_column_type(stmt, 1) == SQLITE_TEXT);
		const char *name = (const char *) sqlite3_column_text(stmt, 0);
		const char *value = (const char *) sqlite3_column_text(stmt, 1);
		debug(D_DEBUG, "jobs[%" PRICHIRP_JOBID_T "].environment[`%s'] = `%s'", id, name, value);
		n = snprintf(entry, sizeof(buf), "%s=%s", name, value);
		if (n == -1) {
			rc = errno; /* EOVERFLOW */
			goto out;
		} else if ((size_t)n >= sizeof(buf)) {
			entry = xxmalloc(n);
			sprintf(entry, "%s=%s", name, value);
		}
		*env = string_array_append(*env, entry);
		if (entry != buf) free(entry);
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	CATCH(envdefaults(env, id, subject, sandbox)); /* ideally these would be set at the start and then replaced but it's awkward to replace entries in the char *[] */
	rc = 0;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int jgetcontext (sqlite3 *db, chirp_jobid_t id, char **executable, char **subject, int *priority)
{
	static const char JobExecutable[] =
		"SELECT executable, subject, priority"
		"    FROM Jobs"
		"    WHERE id = ?;";

	sqlite3_stmt *stmt = NULL;
	int rc;

	sqlcatch(sqlite3_prepare_v2(db, JobExecutable, sizeof(JobExecutable), &stmt, NULL));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
	{
		assert(sqlite3_column_count(stmt) == 3);
		assert(sqlite3_column_type(stmt, 0) == SQLITE_TEXT);
		assert(sqlite3_column_type(stmt, 1) == SQLITE_TEXT);
		assert(sqlite3_column_type(stmt, 2) == SQLITE_INTEGER);
		*executable = xxstrdup((const char *)sqlite3_column_text(stmt, 0));
		*subject = xxstrdup((const char *)sqlite3_column_text(stmt, 1));
		*priority = sqlite3_column_int(stmt, 2);
		debug(D_DEBUG, "jobs[%" PRICHIRP_JOBID_T "].executable = `%s'", id, *executable);
		debug(D_DEBUG, "jobs[%" PRICHIRP_JOBID_T "].subject = `%s'", id, *subject);
		debug(D_DEBUG, "jobs[%" PRICHIRP_JOBID_T "].priority = %d", id, *priority);
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	rc = 0;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int setcore (void)
{
#ifdef CCTOOLS_OPSYS_LINUX
	cpu_set_t set;
	CPU_ZERO(&set);
	CPU_SET(0, &set);
	return sched_setaffinity(0, sizeof(set), &set) == -1 ? errno : 0;
#else
	return 0;
#endif
}

static void bootstrap (const char *sandbox, const char *path, char *const argv[], char *const envp[])
{
	int rc;

	/* reassign std files and close everything else */
	CATCH(fd_nonstd_close());
	CATCH(fd_null(STDIN_FILENO, O_RDONLY));
	CATCH(fd_null(STDOUT_FILENO, O_WRONLY));
	CATCH(fd_null(STDERR_FILENO, O_WRONLY));
	CATCHCODE(chdir(sandbox), -1);
	CATCHCODE(setpgid(0, 0), -1); /* create new process group */
	CATCH(setcore());
	CATCHCODE(execve(path, argv, envp), -1);
out:
	signal(SIGUSR1, SIG_DFL);
	raise(SIGUSR1);
	raise(SIGABRT);
	_exit(EXIT_FAILURE);
}

static int jexecute (pid_t *pid, chirp_jobid_t id, const char *sandbox, const char *path, char *const argv[], char *const envp[])
{
	int rc = 0;
	*pid = fork();
	if (*pid == 0) { /* child */
		bootstrap(sandbox, path, argv, envp);
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

static int jstarted (sqlite3 *db, chirp_jobid_t id, pid_t pid, const char *sandbox)
{
	static const char Started[] =
		"UPDATE OR ROLLBACK Jobs SET status = 'STARTED', time_start = CURRENT_TIMESTAMP WHERE id = ?;"
		"INSERT OR REPLACE INTO LocalJobs (id, pid, ppid, sandbox) VALUES ( ?, ?, ?, ? );";

	sqlite3_stmt *stmt = NULL;
	const char *current;
	int rc;

	sqlcatch(sqlite3_prepare_v2(db, Started, strlen(Started)+1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	sqlcatch(sqlite3_bind_int64(stmt, 2, (sqlite3_int64)pid));
	sqlcatch(sqlite3_bind_int64(stmt, 3, (sqlite3_int64)getpid()));
	sqlcatch(sqlite3_bind_text(stmt, 4, sandbox, -1, SQLITE_TRANSIENT));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	return rc;
}

static int jerrored (sqlite3 *db, chirp_jobid_t id, const char *errmsg)
{
	static const char Errored[] =
		"UPDATE OR ROLLBACK Jobs SET status = 'ERRORED', time_error = CURRENT_TIMESTAMP, error = ? WHERE id = ?;";

	sqlite3_stmt *stmt = NULL;
	int rc;

	sqlcatch(sqlite3_prepare_v2(db, Errored, sizeof(Errored), &stmt, NULL));
	sqlcatch(sqlite3_bind_text(stmt, 1, errmsg, -1, SQLITE_TRANSIENT));
	sqlcatch(sqlite3_bind_int64(stmt, 2, (sqlite3_int64)id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " entered error state: `%s'", id, errmsg);

	rc = 0;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int job_start (sqlite3 *db, chirp_jobid_t id)
{
	int rc;
	pid_t pid = 0;
	char sandbox[PATH_MAX] = "";
	char *executable = NULL;
	char *subject = NULL;
	int priority;
	char **arguments = NULL;
	char **environment = NULL;
	sqlite3_stmt *stmt = NULL;

	rc = sqlite3_exec(db, "BEGIN EXCLUSIVE TRANSACTION;", NULL, NULL, NULL);
	if (rc == SQLITE_BUSY)
		return SQLITE_BUSY; /* we do this so the job is not moved to ERRORED state */
	else
		sqlcatch(rc);

	debug(D_DEBUG, "job_start %" PRICHIRP_JOBID_T, id);

	CATCH(sandbox_create(sandbox));
	CATCH(jgetcontext(db, id, &executable, &subject, &priority));
	CATCH(jgetargs(db, id, &arguments));
	CATCH(jgetenv(db, id, subject, sandbox, &environment));
	CATCH(jbindfiles(db, id, subject, sandbox, BOOTSTRAP));

	CATCH(jexecute(&pid, id, sandbox, executable, arguments, environment));
	CATCH(jstarted(db, id, pid, sandbox));

	sqlcatchexec(db, "END TRANSACTION;");

	rc = 0;
	goto out;

out:
	if (rc) {
		jerrored(db, id, strerror(rc)); /* N.B. if this fails then job_start will be retried */
		sqlite3_exec(db, "END TRANSACTION;", NULL, NULL, NULL);
		if (pid)
			kill_kindly(pid);
		if (sandbox[0])
			sandbox_delete(sandbox);
	}
	free(executable);
	free(arguments);
	free(environment);
	return rc;
}

static int jwaited (sqlite3 *db, chirp_jobid_t id)
{
	static const char SetWait[] = "UPDATE LocalJobs SET waited = 1 WHERE id = ?;";

	int rc;
	sqlite3_stmt *stmt = NULL;

	sqlcatch(sqlite3_prepare_v2(db, SetWait, sizeof(SetWait), &stmt, NULL));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
out:
	sqlite3_finalize(stmt);
	return rc;
}

#define bind_literal(stmt,n,l)  sqlite3_bind_text((stmt),(n),(l ""),sizeof(l)-1,SQLITE_STATIC)
static int jfinished (sqlite3 *db, chirp_jobid_t id, int status)
{
	/* This is wrapped in a transaction in job_wait. */
	static const char Finished[] =
		"UPDATE OR ROLLBACK Jobs"
		"    SET exit_code = ?,"
		"        exit_status = ?,"
		"        exit_signal = ?,"
		"        status = 'FINISHED',"
		"        time_finish = CURRENT_TIMESTAMP"
		"    WHERE id = ? AND status = 'STARTED';"
		"DELETE FROM LocalJobs WHERE id = ?;";

	sqlite3_stmt *stmt = NULL;
	int rc;
	const char *current;

	sqlcatch(sqlite3_prepare_v2(db, Finished, strlen(Finished)+1, &stmt, &current));
	if (WIFEXITED(status)) {
		sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)WEXITSTATUS(status)));
		sqlcatch(bind_literal(stmt, 2, "EXITED"));
		sqlcatch(sqlite3_bind_null(stmt, 3));
	} else if (WIFSIGNALED(status)) {
		sqlcatch(sqlite3_bind_null(stmt, 1));
		sqlcatch(bind_literal(stmt, 2, "SIGNALED"));
		sqlcatch(sqlite3_bind_int64(stmt, 3, (sqlite3_int64)WTERMSIG(status)));
	} else {
		assert(0);
	}
	sqlcatch(sqlite3_bind_int64(stmt, 4, (sqlite3_int64)id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " entered finished state: %d", id, status);

	rc = 0;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int job_wait (sqlite3 *db, int *count)
{
	static const char Running[] =
		/* We need to establish an EXCLUSIVE lock so we can store the results of waitpid. */
		"BEGIN EXCLUSIVE TRANSACTION;"
		"SELECT Jobs.id, subject, pid, sandbox"
		"    FROM Jobs NATURAL JOIN LocalJobs"
		"    WHERE status = 'STARTED';"
		"END TRANSACTION;";

	int rc;
	sqlite3_stmt *stmt = NULL;
	const char *current;

	sqlcatch(sqlite3_prepare_v2(db, Running, strlen(Running)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		int status;
		pid_t pid, wpid;

		assert(sqlite3_column_count(stmt) == 4);
		assert(sqlite3_column_type(stmt, 0) == SQLITE_INTEGER);
		assert(sqlite3_column_type(stmt, 1) == SQLITE_TEXT);
		assert(sqlite3_column_type(stmt, 2) == SQLITE_INTEGER);
		assert(sqlite3_column_type(stmt, 3) == SQLITE_TEXT);

		const char *subject = (const char *)sqlite3_column_text(stmt, 1);
		const char *sandbox = (const char *)sqlite3_column_text(stmt, 3);

		chirp_jobid_t id = sqlite3_column_int64(stmt, 0);
		pid = sqlite3_column_int64(stmt, 2);
		wpid = waitpid(pid, &status, WNOHANG);
		if (wpid == pid) {
			debug(D_DEBUG, "[%s:%d] %d = waitpid(%d, %p, WNOHANG)", __FILE__, __LINE__, (int)wpid, (int)pid, &status);
			debug(D_DEBUG, "pid = %d; status = %d", (int)pid, status);
			debug(D_DEBUG, "WIFEXITED(status) = %d", WIFEXITED(status));
			debug(D_DEBUG, "WEXITSTATUS(status) = %d", WEXITSTATUS(status));
			debug(D_DEBUG, "WIFSIGNALED(status) = %d", WIFSIGNALED(status));
			debug(D_DEBUG, "WTERMSIG(status) = %d", WTERMSIG(status));
			if (WIFSTOPPED(status) || WIFCONTINUED(status)) {
				/* ignore this, probably debugging of some kind */
				continue;
			} else {
				/* process ended */
				*count -= 1;
				rc = jbindfiles(db, id, subject, sandbox, STRAPBOOT);
				sandbox_delete(sandbox);
				CATCH(jfinished(db, id, status)); /* TODO Handle error but always set state to FINISHED */
			}
		} else if (wpid == 0) {
			continue;
		} else if (wpid == -1) {
			debug(D_DEBUG, "[%s:%d] %d = waitpid(%d, %p, WNOHANG)", __FILE__, __LINE__, (int)wpid, (int)pid, &status);
			if (errno == ECHILD) {
				/* This indicates a child from a previous Chirp instance. Set it to ERRORED */
				sandbox_delete(sandbox);
				CATCH(jerrored(db, id, "Lost process"));
			} else if (errno == EINTR) {
				continue;
			} else {
				assert(0);
			}
		}
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	return rc;
}

static int job_kill (sqlite3 *db, int *count)
{
	static const char Killable[] =
		"BEGIN EXCLUSIVE TRANSACTION;"
		"SELECT Jobs.id, LocalJobs.pid, LocalJobs.sandbox"
		"    FROM Jobs NATURAL JOIN LocalJobs"
		"    WHERE Jobs.status = 'KILLED' AND NOT LocalJobs.waited;"
		"DELETE FROM LocalJobs"
		"    WHERE id IN (SELECT Jobs.id"
		"                 FROM Jobs NATURAL JOIN LocalJobs"
		"                 WHERE status = 'KILLED' AND waited);"
		"END TRANSACTION";

	int rc;
	sqlite3_stmt *stmt = NULL;
	const char *current;

	sqlcatch(sqlite3_prepare_v2(db, Killable, strlen(Killable)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		int status;
		chirp_jobid_t id;
		pid_t pid, wpid;
		const char *sandbox;

		assert(sqlite3_column_count(stmt) == 3);
		assert(sqlite3_column_type(stmt, 0) == SQLITE_INTEGER);
		assert(sqlite3_column_type(stmt, 1) == SQLITE_INTEGER);
		assert(sqlite3_column_type(stmt, 2) == SQLITE_TEXT);

		id = sqlite3_column_int64(stmt, 0);
		pid = sqlite3_column_int64(stmt, 1);
		sandbox = (const char *)sqlite3_column_text(stmt, 2);
		debug(D_DEBUG, "killing job %" PRICHIRP_JOBID_T " with pid = %d with sandbox = `%s'", id, (int)pid, sandbox);

		rc = kill_kindly(pid);
		if (rc == 0) {
			/* this indicates SIGKILL was sent, we need to do another short wait */
			usleep(50);
		}
		sandbox_delete(sandbox);
		/* we use WNOHANG because the child might be in an unkillable state... we can't do anything about that. */
		wpid = waitpid(pid, &status, WNOHANG);
		debug(D_DEBUG, "[%s:%d] %d = waitpid(%d, %p, WNOHANG)", __FILE__, __LINE__, (int)wpid, (int)pid, &status);
		if (wpid == pid) {
			debug(D_DEBUG, "pid = %d; status = %d", (int)pid, status);
			debug(D_DEBUG, "WIFEXITED(status) = %d", WIFEXITED(status));
			debug(D_DEBUG, "WEXITSTATUS(status) = %d", WEXITSTATUS(status));
			debug(D_DEBUG, "WIFSIGNALED(status) = %d", WIFSIGNALED(status));
			debug(D_DEBUG, "WTERMSIG(status) = %d", WTERMSIG(status));
			if (WIFSTOPPED(status) || WIFCONTINUED(status)) {
				/* ignore this, probably debugging of some kind */
				continue;
			} else {
				/* process ended */
				CATCH(jwaited(db, id));
				*count -= 1;
			}
		} else if (wpid == 0) {
			continue; /* we killed it but it isn't dead yet, we'll try again next time */
		} else if (wpid == -1) {
			if (errno == ECHILD) {
				/* This indicates a child from a previous Chirp instance.
				 * KILLED is a terminal state, cannot change to ERRORED. So we
				 * just remove it from LocalJobs. */
				CATCH(jwaited(db, id));
			} else if (errno == EINTR) {
				continue;
			} else {
				assert(0);
			}
		}
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	return rc;
}

static int job_schedule_fifo (sqlite3 *db, int *count)
{
	static const char FIFO[] = "SELECT id FROM Jobs WHERE status = 'COMMITTED' ORDER BY priority, time_commit LIMIT 1;";

	int rc;
	sqlite3_stmt *stmt = NULL;

	sqlcatch(sqlite3_prepare_v2(db, FIFO, sizeof(FIFO), &stmt, NULL));
	if ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
	{
		assert(sqlite3_column_count(stmt) == 1);
		assert(sqlite3_column_type(stmt, 0) == SQLITE_INTEGER);
		CATCH(job_start(db, (chirp_jobid_t)sqlite3_column_int64(stmt, 0)));
		*count += 1;
		rc = sqlite3_step(stmt);
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	rc = 0;
out:
	sqlite3_finalize(stmt);
	return rc;
}

int chirp_fs_local_job_schedule (sqlite3 *db)
{
	int rc = 0;
	int count = 0;

	/* TODO Job time limits */

	/* continue until parent dies */
	while (getppid() != 1) {
		/* Look at jobs that are executing, try to wait. */
		job_wait(db, &count);

		/* Look at jobs that are in `KILLED' state, kill any running killed jobs */
		job_kill(db, &count);

		/* Look at jobs waiting in `COMMIT' state, schedule if we can */
		if (count < chirp_job_concurrency) {
			rc = job_schedule_fifo(db, &count);
		}

		usleep(50000);
	}

	debug(D_DEBUG, "scheduler exit: rc = %d (`%s')", rc, strerror(rc));
	return rc;
}

/* vim: set noexpandtab tabstop=4: */
