/*
 * Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
*/

/* TODO:
 *
 * o Finer grained errors for exception handling in status/commit/kill/reap/wait.
 * o Tags/Job Group for jobs for wait?
 * o Maintain complete job database vs. garbage collecting old/lost jobs.
 * o Job execute ACL?
 * o Trace (log) all SQL commands: http://stackoverflow.com/questions/1607368/sql-query-logging-for-sqlite
 */

#include "catch.h"
#include "chirp_acl.h"
#include "chirp_filesystem.h"
#include "chirp_job.h"
#include "chirp_sqlite.h"
#include "chirp_types.h"

#include "buffer.h"
#include "debug.h"
#include "json.h"
#include "json_aux.h"
#include "macros.h"
#include "path.h"

#if defined(__linux__)
#	include <sys/syscall.h>
#	include "ioprio.h"
#endif

#include <assert.h>
#include <errno.h>
#include <float.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define IMMUTABLE_JOB_INSERT(T) \
		"CREATE TRIGGER " T "ImmutableJobI BEFORE INSERT ON " T " FOR EACH ROW" \
		"    BEGIN" \
		"        SELECT RAISE(ABORT, 'cannot update immutable job')" \
		"        FROM Job INNER JOIN JobStatus ON Job.status = JobStatus.status" \
		"        WHERE NEW.id = Job.id AND JobStatus.terminal;" \
		"    END;"

#define IMMUTABLE_JOB_UPDATE(T) \
		"CREATE TRIGGER " T "ImmutableJobU BEFORE UPDATE ON " T " FOR EACH ROW" \
		"    BEGIN" \
		"        SELECT RAISE(ABORT, 'cannot update immutable job')" \
		"        FROM Job INNER JOIN JobStatus ON Job.status = JobStatus.status" \
		"        WHERE OLD.id = Job.id AND JobStatus.terminal;" \
		"    END;"

#define IMMUTABLE_JOB_INSUPD(T) \
		IMMUTABLE_JOB_INSERT(T) \
		IMMUTABLE_JOB_UPDATE(T)


extern const char *chirp_super_user;
extern char        chirp_transient_path[PATH_MAX];

unsigned chirp_job_concurrency = 1;
int      chirp_job_enabled = 0;
pid_t    chirp_job_schedd = 0;
int      chirp_job_time_limit = 3600; /* 1 hour */

static int db_init (sqlite3 *db)
{
	static const char Initialize[] =
		/* Always goes through... */
		"PRAGMA foreign_keys = ON;"
		"PRAGMA journal_mode = WAL;"
		/* May cause errors (table already exists because the DB is already setup) */
		"BEGIN TRANSACTION;"
		"CREATE TABLE Job("
		"	id INTEGER PRIMARY KEY,"
		"	error TEXT,"
		"	executable TEXT NOT NULL,"
		"	exit_code INTEGER,"
		"	exit_signal TEXT,"
		"	exit_status TEXT REFERENCES ExitStatus (status),"
		"	priority INTEGER NOT NULL DEFAULT 1,"
		"	status TEXT NOT NULL DEFAULT 'CREATED' REFERENCES JobStatus (status),"
		"	subject TEXT NOT NULL,"
		"	tag TEXT NOT NULL,"
		"	time_commit DATETIME,"
		"	time_create DATETIME NOT NULL DEFAULT (strftime('%s', 'now')),"
		"	time_error DATETIME,"
		"	time_finish DATETIME,"
		"	time_kill DATETIME,"
		"	time_start DATETIME,"
		"	url TEXT NOT NULL);"
		IMMUTABLE_JOB_UPDATE("Job")
		/* We pull this out to allow INSERT of time_reap on a terminal job. */
		"CREATE TABLE JobReaped("
		"	id INTEGER PRIMARY KEY REFERENCES Job (id),"
		"	time_reap DATETIME NOT NULL);"
		IMMUTABLE_JOB_UPDATE("JobReaped")
		"CREATE VIEW JobPublic AS"
		"	SELECT"
		"		Job.id,"
		"		Job.error,"
		"		Job.executable,"
		"		Job.exit_code,"
		"		Job.exit_status,"
		"		Job.exit_signal,"
		"		Job.priority,"
		"		Job.status,"
		"		Job.subject,"
		"		Job.tag,"
		"		Job.time_commit,"
		"		Job.time_create,"
		"		Job.time_error,"
		"		Job.time_finish,"
		"		Job.time_kill,"
		"		Job.time_start,"
		"		JobReaped.time_reap"
		"	FROM Job NATURAL LEFT OUTER JOIN JobReaped;"
		/* We use JobStatus as an SQLite enum */
		"CREATE TABLE JobStatus("
		"	status TEXT PRIMARY KEY,"
		"	terminal BOOL NOT NULL);"
		"INSERT INTO JobStatus VALUES"
		"	('CREATED', 0),"
		"	('COMMITTED', 0),"
		"	('ERRORED', 1),"
		"	('FINISHED', 1),"
		"	('KILLED', 1),"
		"	('STARTED', 0);"
		IMMUTABLE("JobStatus")
		/* We use ExitStatus as an SQLite enum */
		"CREATE TABLE ExitStatus (status TEXT PRIMARY KEY);"
		"INSERT INTO ExitStatus VALUES ('EXITED'), ('SIGNALED');"
		IMMUTABLE("ExitStatus")
		"CREATE TABLE JobArgument("
		"	id INTEGER REFERENCES Job (id),"
		"	n INTEGER NOT NULL,"
		"	arg TEXT NOT NULL,"
		"	PRIMARY KEY (id, n));"
		IMMUTABLE_JOB_INSUPD("JobArgument")
		"CREATE TABLE JobEnvironment("
		"	id INTEGER REFERENCES Job (id),"
		"	name TEXT NOT NULL,"
		"	value TEXT NOT NULL,"
		"	PRIMARY KEY (id, name));"
		IMMUTABLE_JOB_INSUPD("JobEnvironment")
		"CREATE TABLE JobFile("
		"	id INTEGER REFERENCES Job (id),"
		"	binding TEXT NOT NULL DEFAULT 'LINK' REFERENCES FileBinding (binding),"
		"	serv_path TEXT NOT NULL,"
		"	task_path TEXT NOT NULL,"
		"	tag TEXT," /* user value */
		"	size INTEGER,"
		"	type TEXT NOT NULL REFERENCES FileType (type),"
		"	PRIMARY KEY (id, task_path, type));"
		IMMUTABLE_JOB_INSUPD("JobFile")
		/* We use FileType as an SQLite enum */
		"CREATE TABLE FileBinding (binding TEXT PRIMARY KEY);"
		"INSERT INTO FileBinding VALUES ('LINK'), ('COPY'), ('URL');"
		IMMUTABLE("FileBinding")
		"CREATE TABLE FileType (type TEXT PRIMARY KEY);"
		"INSERT INTO FileType VALUES ('INPUT'), ('OUTPUT');"
		IMMUTABLE("FileType")
		"END TRANSACTION;";

	int rc;
	char *errmsg = NULL;

	debug(D_DEBUG, "initializing Job DB");
	rc = sqlite3_exec(db, Initialize, NULL, NULL, &errmsg); /* Ignore any errors. */
	if (rc) {
		if (!strstr(sqlite3_errmsg(db), "table Job already exists"))
			debug(D_DEBUG, "[%s:%d] sqlite3 error: %d `%s': %s", __FILE__, __LINE__, rc, sqlite3_errstr(rc), sqlite3_errmsg(db));
		sqlite3_exec(db, "ROLLBACK TRANSACTION;", NULL, NULL, NULL);
	}
	sqlite3_free(errmsg);

	rc = 0;
	goto out;
out:
	return rc;
}

static void profile (void *ud, const char *stmt, sqlite3_uint64 nano)
{
	if (nano > (500*1000*1000)) {
		const size_t limit = 80;
		if (strlen(stmt) > limit) {
			debug(D_DEBUG, "sqlite3 overrun %" PRIu64 "ms '%.*s...'", (uint64_t)nano/(1000*1000), (int)limit-3, stmt);
		} else {
			debug(D_DEBUG, "sqlite3 overrun %" PRIu64 "ms '%s'", (uint64_t)nano/(1000*1000), stmt);
		}
	}
}

static int db_get (sqlite3 **dbp, int timeout)
{
	int rc;
	static sqlite3 *db;
	char uri[PATH_MAX];

	if (db == NULL) {
		debug(D_DEBUG, "using sqlite version %s", sqlite3_libversion());

		if (snprintf(uri, PATH_MAX, "file://%s/.__job.db?mode=rwc", chirp_transient_path) >= PATH_MAX)
			fatal("root path `%s' too long", chirp_transient_path);
		CATCH(sqlite3_open_v2(uri, &db,  SQLITE_OPEN_URI|SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, NULL));

#if 1
	sqlite3_profile(db, profile, NULL);
#else
	(void)profile;
#endif

		sqlite3_busy_timeout(db, timeout < 0 ? CHIRP_SQLITE_TIMEOUT : timeout);
		CATCH(db_init(db));
		CATCH(cfs->job_dbinit(db));
	}
	sqlite3_busy_timeout(db, timeout < 0 ? CHIRP_SQLITE_TIMEOUT : timeout);

	rc = 0;
	goto out;
out:
	if (rc) {
		sqlite3_close(db);
		db = NULL;
	}
	*dbp = db;
	return rc;
}

static int readpath (char file[CHIRP_PATH_MAX], json_value *J) {
	if (J->type != json_string || J->u.string.length+1 > CHIRP_PATH_MAX)
		return 0;
	strcpy(file, J->u.string.ptr);
	return 1;
}

#define jistype(o,t) ((o)->type == (t))
#define jchecktype(o,t) \
	do {\
		json_type tt = (t);\
		if (!jistype(o,tt)) {\
			debug(D_DEBUG, "JSON type failure: type(%s) == %s", #o, json_type_str[tt]);\
			CATCH(EINVAL);\
		}\
	} while (0)

#define jgetnameopt(v,o,n,t) \
	do {\
		unsigned int i;\
		json_value *object = (o);\
		const char *name = (n);\
		json_type tt = (t);\
		assert(object->type == json_object);\
		v = NULL;\
		for (i = 0; i < object->u.object.length; i++) {\
			if (strcmp(name, object->u.object.values[i].name) == 0) {\
				if (jistype(object->u.object.values[i].value, tt)) {\
					v = object->u.object.values[i].value;\
					break;\
				} else if (!jistype(object->u.object.values[i].value, json_null)) {\
					debug(D_DEBUG, "%s[%s] is type `%s' (expected `%s' or `NULL')", #o, #n, json_type_str[object->u.object.values[i].value->type], json_type_str[tt]);\
					CATCH(EINVAL);\
				}\
			}\
		}\
	} while (0)

#define jgetnamefail(v,o,n,t) \
	do {\
		unsigned int i;\
		json_value *object = (o);\
		const char *name = (n);\
		json_type tt = (t);\
		assert(object->type == json_object);\
		v = NULL;\
		for (i = 0; i < object->u.object.length; i++) {\
			if (strcmp(name, object->u.object.values[i].name) == 0) {\
				if (jistype(object->u.object.values[i].value, tt)) {\
					v = object->u.object.values[i].value;\
					break;\
				} else {\
					debug(D_DEBUG, "%s[%s] is type `%s' (expected `%s')", #o, #n, json_type_str[object->u.object.values[i].value->type], json_type_str[tt]);\
					CATCH(EINVAL);\
				}\
			}\
		}\
		if (!v) {\
			debug(D_DEBUG, "%s[%s] is type `%s' (expected `%s')", #o, #n, "NULL", json_type_str[tt]);\
			CATCH(EINVAL);\
		}\
	} while (0)

int chirp_job_create (chirp_jobid_t *id, json_value *J, const char *subject)
{
	static const char Create[] =
		"BEGIN IMMEDIATE TRANSACTION;"
		"INSERT OR ROLLBACK INTO Job (executable, subject, tag, url) VALUES ( ?, ?, ?, ? );"
		"INSERT OR ROLLBACK INTO JobArgument (id, n, arg) VALUES ( ?, ?, ? );"
		"INSERT OR REPLACE INTO JobEnvironment (id, name, value) VALUES ( ?, ?, ? );"
		"INSERT OR REPLACE INTO JobFile (id, type, serv_path, tag, task_path, binding) VALUES ( ?, UPPER(?), ?, ?, ?, UPPER(?) );"
		"END TRANSACTION;";

	time_t timeout = time(NULL)+3;
	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	const char *current;
	int rc;

	if (!chirp_job_enabled) return ENOSYS;
	CATCH(db_get(&db, -1));
	jchecktype(J, json_object);

restart:
	current = Create;
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	{
		json_value *jexecutable;
		char executable[CHIRP_PATH_MAX];
		jgetnamefail(jexecutable, J, "executable", json_string);
		if (!readpath(executable, jexecutable)) CATCH(EINVAL);
		sqlcatch(sqlite3_bind_text(stmt, 1, executable, -1, SQLITE_STATIC));
		sqlcatch(sqlite3_bind_text(stmt, 2, subject, -1, SQLITE_STATIC));
		json_value *jtag;
		char tag[128] = "(unknown)";
		jgetnameopt(jtag, J, "tag", json_string);
		if (jtag)
			snprintf(tag, sizeof(tag), "%s", jtag->u.string.ptr);
		sqlcatch(sqlite3_bind_text(stmt, 3, tag, -1, SQLITE_STATIC));
		sqlcatch(sqlite3_bind_text(stmt, 4, chirp_url, -1, SQLITE_STATIC));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		*id = sqlite3_last_insert_rowid(db); /* in SQLite, this is `id' */
		debug(D_DEBUG, "created job %" PRICHIRP_JOBID_T " as `%s' executable = `%s'", *id, subject, executable);
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	/* handle arguments */
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	{
		int i;
		json_value *arguments;
		jgetnamefail(arguments, J, "arguments", json_array);
		for (i = 0; i < (int)arguments->u.array.length; i++) {
			sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)*id));
			sqlcatch(sqlite3_bind_int64(stmt, 2, (sqlite3_int64)i+1));
			json_value *arg = arguments->u.array.values[i];
			jchecktype(arg, json_string);
			sqlcatch(sqlite3_bind_text(stmt, 3, arg->u.string.ptr, -1, SQLITE_STATIC));
			sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
			debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " bound arg %d as `%s'", *id, i+1, arg->u.string.ptr);
			sqlcatchcode(sqlite3_reset(stmt), SQLITE_OK);
		}
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	/* handle environment */
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	{
		json_value *environment;
		jgetnameopt(environment, J, "environment", json_object);
		if (environment) {
			int i;
			for (i = 0; i < (int)environment->u.object.length; i++) {
				sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)*id));
				const char *n = environment->u.object.values[i].name;
				sqlcatch(sqlite3_bind_text(stmt, 2, n, -1, SQLITE_STATIC));
				json_value *v = environment->u.object.values[i].value;
				jchecktype(v, json_string);
				sqlcatch(sqlite3_bind_text(stmt, 3, v->u.string.ptr, -1, SQLITE_STATIC));
				sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
				debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " environment variable `%s'=`%s'", *id, n, v->u.string.ptr);
				sqlcatchcode(sqlite3_reset(stmt), SQLITE_OK);
			}
		}
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	/* handle files */
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	{
		int i;
		json_value *value;
		jgetnamefail(value, J, "files", json_array);
		for (i = 0; i < (int)value->u.array.length; i++) {
			json_value *file, *tag, *type, *serv_path, *task_path, *binding;

			sqlcatchcode(sqlite3_reset(stmt), SQLITE_OK);

			file = value->u.array.values[i];
			jchecktype(file, json_object);
			jgetnamefail(type, file, "type", json_string);
			jgetnamefail(serv_path, file, "serv_path", json_string);
			jgetnamefail(task_path, file, "task_path", json_string);
			jgetnameopt(tag, file, "tag", json_string);
			jgetnameopt(binding, file, "binding", json_string); /* can be null */

			sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)*id));

			sqlcatch(sqlite3_bind_text(stmt, 2, type->u.string.ptr, -1, SQLITE_STATIC));

			if (strlen(serv_path->u.string.ptr) >= CHIRP_PATH_MAX)
				CATCH(ENAMETOOLONG);
			/* ACL checks are not necessary for URLs */
			if (!(binding && strcmp(binding->u.string.ptr, "URL") == 0)) {
				if (strcmp(type->u.string.ptr, "INPUT") == 0)
					CATCHUNIX(chirp_acl_check_recursive(serv_path->u.string.ptr, subject, CHIRP_ACL_READ) ? 0 : -1);
				else if (strcmp(type->u.string.ptr, "OUTPUT") == 0)
					CATCHUNIX(chirp_acl_check_recursive(serv_path->u.string.ptr, subject, CHIRP_ACL_WRITE) ? 0 : -1);
				else
					CATCH(EINVAL);
			}
			sqlcatch(sqlite3_bind_text(stmt, 3, serv_path->u.string.ptr, -1, SQLITE_STATIC));

			sqlcatch(sqlite3_bind_text(stmt, 4, tag ? tag->u.string.ptr : NULL, -1, SQLITE_STATIC));

			if (strlen(task_path->u.string.ptr) >= CHIRP_PATH_MAX)
				CATCH(ENAMETOOLONG);
			sqlcatch(sqlite3_bind_text(stmt, 5, task_path->u.string.ptr, -1, SQLITE_STATIC));

			sqlcatch(sqlite3_bind_text(stmt, 6, binding ? binding->u.string.ptr : NULL, -1, SQLITE_STATIC));

			sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
			debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " new file `%s' bound as `%s' type `%s'", *id, serv_path->u.string.ptr, task_path->u.string.ptr, type->u.string.ptr);
		}
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	assert(strlen(current) == 0);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	if (rc == EAGAIN && time(NULL) <= timeout) {
		debug(D_DEBUG, "timeout job_create; restarting");
		usleep(2000);
		goto restart;
	}
	return rc;
}

int chirp_job_commit (json_value *J, const char *subject)
{
	static const char Commit[] =
		"BEGIN TRANSACTION;"
		/* This SELECT/UPDATE may be executed multiple times. That's why it's wrapped in a transaction. */
		"SELECT subject = ? OR ? FROM Job WHERE id = ?;"
		"UPDATE OR ROLLBACK Job"
		"	SET status = 'COMMITTED', time_commit = strftime('%s', 'now')"
		"	WHERE id = ? AND status = 'CREATED';"
		"END TRANSACTION;";

	time_t timeout = time(NULL)+3;
	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	const char *current;
	int rc;
	int i;

	if (!chirp_job_enabled) return ENOSYS;
	CATCH(db_get(&db, -1));
	jchecktype(J, json_array);

restart:
	current = Commit;
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_text(stmt, 1, subject, -1, SQLITE_STATIC));
	sqlcatch(sqlite3_bind_int(stmt, 2, strcmp(subject, chirp_super_user) == 0));
	/* id bound in for loop */
	for (i = 0; i < (int)J->u.array.length; i++) {
		sqlcatchcode(sqlite3_reset(stmt), SQLITE_OK);
		jchecktype(J->u.array.values[i], json_integer);
		sqlcatch(sqlite3_bind_int64(stmt, 3, J->u.array.values[i]->u.integer));
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_ROW) {
			/* Job exists */
			if (!sqlite3_column_int(stmt, 0)) {
				CATCH(EACCES);
			}
		} else if (rc == SQLITE_DONE) {
			CATCH(ESRCH);
		} else {
			sqlcatch(rc);
		}
	}
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	/* id bound in for loop */
	for (i = 0; i < (int)J->u.array.length; i++) {
		sqlcatchcode(sqlite3_reset(stmt), SQLITE_OK);
		jchecktype(J->u.array.values[i], json_integer);
		sqlcatch(sqlite3_bind_int64(stmt, 1, J->u.array.values[i]->u.integer));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		if (sqlite3_changes(db)) {
			debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " is committed", (chirp_jobid_t)J->u.array.values[i]->u.integer);
		} else {
			debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " not changed", (chirp_jobid_t)J->u.array.values[i]->u.integer);
		}
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	if (rc == EAGAIN && time(NULL) <= timeout) {
		debug(D_DEBUG, "timeout job_commit; restarting");
		usleep(2000);
		goto restart;
	}
	return rc;
}

int chirp_job_kill (json_value *J, const char *subject)
{
	static const char Kill[] =
		"BEGIN TRANSACTION;"
		/* This UPDATE may be executed multiple times. That's why it's wrapped in a transaction. */
		"SELECT subject = ? OR ? FROM Job WHERE id = ?;"
		"UPDATE Job"
		"    SET status = 'KILLED', time_kill = strftime('%s', 'now')"
		"    WHERE id IN (SELECT Job.id"
		"                     FROM Job NATURAL JOIN JobStatus"
		"                     WHERE id = ? AND NOT JobStatus.terminal);"
		"END TRANSACTION;";

	time_t timeout = time(NULL)+3;
	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	const char *current;
	int rc;
	int i;

	if (!chirp_job_enabled) return ENOSYS;
	CATCH(db_get(&db, -1));
	jchecktype(J, json_array);

restart:
	current = Kill;
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_text(stmt, 1, subject, -1, SQLITE_STATIC));
	sqlcatch(sqlite3_bind_int(stmt, 2, strcmp(subject, chirp_super_user) == 0));
	/* id bound in for loop */
	for (i = 0; i < (int)J->u.array.length; i++) {
		sqlcatchcode(sqlite3_reset(stmt), SQLITE_OK);
		jchecktype(J->u.array.values[i], json_integer);
		sqlcatch(sqlite3_bind_int64(stmt, 3, J->u.array.values[i]->u.integer));
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_ROW) {
			/* Job exists */
			if (!sqlite3_column_int(stmt, 0)) {
				CATCH(EACCES);
			}
		} else if (rc == SQLITE_DONE) {
			CATCH(ESRCH);
		} else {
			sqlcatch(rc);
		}
	}
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	for (i = 0; i < (int)J->u.array.length; i++) {
		sqlcatchcode(sqlite3_reset(stmt), SQLITE_OK);
		jchecktype(J->u.array.values[i], json_integer);
		sqlcatch(sqlite3_bind_int64(stmt, 1, J->u.array.values[i]->u.integer));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		if (sqlite3_changes(db)) {
			debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " is killed", (chirp_jobid_t)J->u.array.values[i]->u.integer);
		} else {
			debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " not killed", (chirp_jobid_t)J->u.array.values[i]->u.integer);
			CATCH(EACCES);
		}
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	if (rc == EAGAIN && time(NULL) <= timeout) {
		debug(D_DEBUG, "timeout job_kill; restarting");
		usleep(2000);
		goto restart;
	}
	return rc;
}

int chirp_job_status (json_value *J, const char *subject, buffer_t *B)
{
	static const char Status[] =
		/* These SELECTs will be executed multiple times, for each job in J. */
		/* FIXME subject check */
		"SELECT JobPublic.* FROM JobPublic WHERE id = ? AND (? OR JobPublic.subject = ?);" /* subject check happens here */
		"SELECT arg FROM JobArgument WHERE id = ? ORDER BY n;"
		"SELECT name, value FROM JobEnvironment WHERE id = ?;"
		"SELECT binding, serv_path, size, tag, task_path, type FROM JobFile WHERE id = ?;";

	time_t timeout = time(NULL)+3;
	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	int rc;
	int i;
	size_t start = buffer_pos(B);

	if (!chirp_job_enabled) return ENOSYS;
	CATCH(db_get(&db, -1));
	jchecktype(J, json_array);

restart:
	buffer_rewind(B, start); /* a failed job_status may add to buffer */

	sqlcatchexec(db, "BEGIN TRANSACTION;");

	CATCHUNIX(buffer_putliteral(B, "["));
	for (i = 0; i < (int)J->u.array.length; i++) {
		const char *current;
		chirp_jobid_t id;
		int first0 = 1;

		jchecktype(J->u.array.values[i], json_integer);
		id = J->u.array.values[i]->u.integer;

		if (i)
			CATCHUNIX(buffer_putliteral(B, ","));

		current = Status;
		sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
		sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
		sqlcatch(sqlite3_bind_int(stmt, 2, strcmp(subject, chirp_super_user) == 0));
		sqlcatch(sqlite3_bind_text(stmt, 3, subject, -1, SQLITE_STATIC));
		if ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
			CATCH(chirp_sqlite3_row_jsonify(stmt, B));
			buffer_rewind(B, buffer_pos(B)-1); /* remove trailing '}' */
		} else if (rc == SQLITE_DONE) {
			CATCH(EACCES);
		} else {
			sqlcatch(rc);
		}
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

		first0 = 1;
		sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
		sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
		CATCHUNIX(buffer_putliteral(B, ",\"arguments\":["));
		while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
			if (!first0)
				CATCHUNIX(buffer_putliteral(B, ","));
			first0 = 0;
			assert(sqlite3_column_count(stmt) == 1);
			CATCH(chirp_sqlite3_column_jsonify(stmt, 0, B));
		}
		sqlcatchcode(rc, SQLITE_DONE);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
		CATCHUNIX(buffer_putliteral(B, "]"));

		first0 = 1;
		sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
		sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
		CATCHUNIX(buffer_putliteral(B, ",\"environment\":{"));
		while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
			if (!first0)
				CATCHUNIX(buffer_putliteral(B, ","));
			first0 = 0;
			assert(sqlite3_column_count(stmt) == 2);
			CATCH(chirp_sqlite3_column_jsonify(stmt, 0, B));
			CATCHUNIX(buffer_putliteral(B, ":"));
			CATCH(chirp_sqlite3_column_jsonify(stmt, 1, B));
		}
		sqlcatchcode(rc, SQLITE_DONE);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
		CATCHUNIX(buffer_putliteral(B, "}"));

		first0 = 1;
		sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
		sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
		CATCHUNIX(buffer_putliteral(B, ",\"files\":["));
		while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
			if (!first0)
				CATCHUNIX(buffer_putliteral(B, ","));
			first0 = 0;
			CATCH(chirp_sqlite3_row_jsonify(stmt, B));
		}
		sqlcatchcode(rc, SQLITE_DONE);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
		CATCHUNIX(buffer_putliteral(B, "]}"));
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	CATCHUNIX(buffer_putliteral(B, "]"));

	sqlcatchexec(db, "END TRANSACTION;");

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	if (rc == EAGAIN && time(NULL) <= timeout) {
		debug(D_DEBUG, "timeout job_status; restarting");
		usleep(2000);
		goto restart;
	}
	return rc;
}

int chirp_job_wait (chirp_jobid_t id, const char *subject, INT64_T timeout, buffer_t *B)
{
	static const char Wait[] =
		"BEGIN TRANSACTION;"
		"SELECT subject = ? OR ? FROM Job WHERE id = ?;"
		/* So we can abort if there are no Job to be waited for. */
		"SELECT COUNT(*) FROM Job NATURAL LEFT OUTER JOIN JobReaped"
		"    WHERE JobReaped.time_reap IS NULL;"
		/* Find Job we wait for. */
		"SELECT Job.id"
		"    FROM Job NATURAL JOIN JobStatus NATURAL LEFT OUTER JOIN JobReaped"
		"    WHERE"
		"          JobStatus.terminal AND"
		"          JobReaped.time_reap IS NULL AND"
		"          (? = 0 OR Job.id = ? OR (? < 0 AND -Job.id <= ?))"
		"    LIMIT 1024;"
		"END TRANSACTION;";

	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	int rc;
	int i, n;
	chirp_jobid_t jobs[1024];
	json_value *J = NULL;

	if (!chirp_job_enabled) return ENOSYS;

	if (timeout < 0) {
		timeout = CHIRP_JOB_WAIT_MAX_TIMEOUT+time(NULL);
	} else if (timeout > 0) {
		timeout = MIN(timeout, CHIRP_JOB_WAIT_MAX_TIMEOUT)+time(NULL);
	}

restart:
	n = 0;
	J = NULL;

	CATCH(db_get(&db, 100)); /* shrink timeout to accomodate RPC timeout */

	do {
		const char *current = Wait;

		sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

		sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
		sqlcatch(sqlite3_bind_text(stmt, 1, subject, -1, SQLITE_STATIC));
		sqlcatch(sqlite3_bind_int(stmt, 2, strcmp(subject, chirp_super_user) == 0));
		/* id bound in for loop */
		sqlcatchcode(sqlite3_reset(stmt), SQLITE_OK);
		sqlcatch(sqlite3_bind_int64(stmt, 3, id));
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_ROW) {
			/* Job exists */
			if (!sqlite3_column_int(stmt, 0)) {
				CATCH(EACCES);
			}
		} else if (rc == SQLITE_DONE) {
			/* do nothing! */
		} else {
			sqlcatch(rc);
		}
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

		sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
		assert(sqlite3_column_count(stmt) == 1 && sqlite3_column_type(stmt, 0) == SQLITE_INTEGER);
		if (sqlite3_column_int(stmt, 0) == 0) {
			CATCH(ESRCH);
		}
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

		sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
		sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
		sqlcatch(sqlite3_bind_int64(stmt, 2, (sqlite3_int64)id));
		sqlcatch(sqlite3_bind_int64(stmt, 3, (sqlite3_int64)id));
		sqlcatch(sqlite3_bind_int64(stmt, 4, (sqlite3_int64)id));
		while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
			assert(sqlite3_column_count(stmt) == 1 && sqlite3_column_type(stmt, 0) == SQLITE_INTEGER);
			jobs[n++] = (chirp_jobid_t)sqlite3_column_int64(stmt, 0);
		}
		sqlcatchcode(rc, SQLITE_DONE);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

		sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

		if (n == 0)
			usleep(5000);
	} while (time(NULL) <= timeout && n == 0);

	{
		int first = 1;
		buffer_t Bstatus[1];
		buffer_init(Bstatus);
		buffer_abortonfailure(Bstatus, 1);

		buffer_putliteral(Bstatus, "[");
		for (i = 0; i < n; i++) {
			if (first)
				buffer_putfstring(Bstatus, "%" PRICHIRP_JOBID_T, jobs[i]);
			else
				buffer_putfstring(Bstatus, ",%" PRICHIRP_JOBID_T, jobs[i]);
			first = 0;
		}
		buffer_putliteral(Bstatus, "]");

		J = json_parse(buffer_tostring(Bstatus), buffer_pos(Bstatus));
		assert(J);
		buffer_free(Bstatus);
		CATCH(chirp_job_status(J, subject, B));
	}

	rc = 0;
	goto out;
out:
	if (J)
		json_value_free(J);
	sqlite3_finalize(stmt);
	sqlend(db);
	if (rc == EAGAIN && time(NULL) <= timeout) {
		debug(D_DEBUG, "timeout job_wait; restarting");
		usleep(2000);
		goto restart;
	}
	if (db)
		sqlite3_busy_timeout(db, CHIRP_SQLITE_TIMEOUT);
	return rc;
}

int chirp_job_reap (json_value *J, const char *subject)
{
	static const char Reap[] =
		"BEGIN TRANSACTION;"
		/* Note: If a job id given has a subject mismatch, has been waited for already, or is not terminal, this INSERT silently does nothing. */
		/* This INSERT may be executed multiple times. That's why it's wrapped in a transaction. */
		"SELECT subject = ? OR ? FROM Job WHERE id = ?;"
		"INSERT OR ROLLBACK INTO JobReaped (id, time_reap)"
		"    SELECT Job.id, strftime('%s', 'now')"
		"        FROM Job NATURAL JOIN JobStatus NATURAL LEFT OUTER JOIN JobReaped"
		"        WHERE Job.id == ? AND JobStatus.terminal AND JobReaped.time_reap IS NULL;"
		"END TRANSACTION;";

	time_t timeout = time(NULL)+3;
	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	const char *current;
	int rc;
	int i;

	if (!chirp_job_enabled) return ENOSYS;
	CATCH(db_get(&db, -1));
	jchecktype(J, json_array);

restart:
	current = Reap;
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_text(stmt, 1, subject, -1, SQLITE_STATIC));
	sqlcatch(sqlite3_bind_int(stmt, 2, strcmp(subject, chirp_super_user) == 0));
	/* id bound in for loop */
	for (i = 0; i < (int)J->u.array.length; i++) {
		sqlcatchcode(sqlite3_reset(stmt), SQLITE_OK);
		jchecktype(J->u.array.values[i], json_integer);
		sqlcatch(sqlite3_bind_int64(stmt, 3, J->u.array.values[i]->u.integer));
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_ROW) {
			/* Job exists */
			if (!sqlite3_column_int(stmt, 0)) {
				CATCH(EACCES);
			}
		} else if (rc == SQLITE_DONE) {
			CATCH(ESRCH);
		} else {
			sqlcatch(rc);
		}
	}
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	for (i = 0; i < (int)J->u.array.length; i++) {
		sqlcatchcode(sqlite3_reset(stmt), SQLITE_OK);
		jchecktype(J->u.array.values[i], json_integer);
		sqlcatch(sqlite3_bind_int64(stmt, 1, J->u.array.values[i]->u.integer));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		if (sqlite3_changes(db)) {
			debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " reaped", (chirp_jobid_t)J->u.array.values[i]->u.integer);
		} else {
			debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " not reaped", (chirp_jobid_t)J->u.array.values[i]->u.integer);
		}
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	assert(strlen(current) == 0);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	if (rc == EAGAIN && time(NULL) <= timeout) {
		debug(D_DEBUG, "timeout job_reap; restarting");
		usleep(2000);
		goto restart;
	}
	return rc;
}

int chirp_job_schedule (void)
{
	int rc;
	sqlite3 *db = NULL;

	if (!chirp_job_enabled) return ENOSYS;

#if defined(__linux__) && defined(SYS_ioprio_get)
	/* The scheduler requires higher IO priority for access to the SQLite db. */
	CATCHUNIX(syscall(SYS_ioprio_get, IOPRIO_WHO_PROCESS, 0));
	debug(D_CHIRP, "iopriority: %d:%d", (int)IOPRIO_PRIO_CLASS(rc), (int)IOPRIO_PRIO_DATA(rc));
	CATCHUNIX(syscall(SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0, IOPRIO_PRIO_VALUE(IOPRIO_CLASS_BE, 0)));
	if (rc == 0)
		debug(D_CHIRP, "iopriority set: %d:%d", (int)IOPRIO_CLASS_BE, 0);
	else assert(0);
	CATCHUNIX(syscall(SYS_ioprio_get, IOPRIO_WHO_PROCESS, 0));
	assert(IOPRIO_PRIO_CLASS(rc) == IOPRIO_CLASS_BE && IOPRIO_PRIO_DATA(rc) == 0);
#endif /* defined(__linux__) && defined(SYS_ioprio_get) */

	CATCH(db_get(&db, -1));

	debug(D_DEBUG, "scheduler running with concurrency: %u", chirp_job_concurrency);
	debug(D_DEBUG, "scheduler running with time limit: %d", chirp_job_time_limit);

	CATCH(cfs->job_schedule(db));
out:
	return rc;
}

/* vim: set noexpandtab tabstop=8: */
