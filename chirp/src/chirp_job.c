/*
 * Copyright (C) 2013- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
*/

#include "chirp_filesystem.h"
#include "chirp_job.h"
#include "chirp_sqlite.h"
#include "chirp_types.h"

#include "debug.h"
#include "json.h"
#include "json_aux.h"
#include "macros.h"
#include "path.h"

#include <assert.h>
#include <errno.h>
#include <float.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* TODO:
 *
 * o Finer grained errors for exception handling in status/commit/kill/reap/wait.
 * o Tags/Job Group for jobs for wait?
 * o Maintain complete job database vs. garbage collecting old/lost jobs.
 * o Job execute ACL?
 */

extern const char *chirp_super_user;
extern char        chirp_transient_path[PATH_MAX];

int   chirp_job_concurrency = 1;
int   chirp_job_enabled = 0;
pid_t chirp_job_schedd = 0;
int   chirp_job_time_limit = 3600; /* 1 hour */

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



#define IMMUTABLE(T) \
		"CREATE TRIGGER " T "ImmutableI BEFORE INSERT ON " T " FOR EACH ROW" \
		"    BEGIN" \
		"        SELECT RAISE(ABORT, 'cannot insert rows of immutable table');" \
		"    END;" \
		"CREATE TRIGGER " T "ImmutableU BEFORE UPDATE ON " T " FOR EACH ROW" \
		"    BEGIN" \
		"        SELECT RAISE(ABORT, 'cannot update rows of immutable table');" \
		"    END;" \
		"CREATE TRIGGER " T "ImmutableD BEFORE DELETE ON " T " FOR EACH ROW" \
		"    BEGIN" \
		"        SELECT RAISE(ABORT, 'cannot delete rows of immutable table');" \
		"    END;"

#define IMMUTABLE_JOB_INSERT(T) \
		"CREATE TRIGGER " T "ImmutableJobI BEFORE INSERT ON " T " FOR EACH ROW" \
		"    BEGIN" \
		"        SELECT RAISE(ABORT, 'cannot update immutable job')" \
		"        FROM Jobs INNER JOIN JobStatus" \
		"        WHERE NEW.id = Jobs.id AND Jobs.status = JobStatus.status AND JobStatus.terminal;" \
		"    END;"

#define IMMUTABLE_JOB_UPDATE(T) \
		"CREATE TRIGGER " T "ImmutableJobU BEFORE UPDATE ON " T " FOR EACH ROW" \
		"    BEGIN" \
		"        SELECT RAISE(ABORT, 'cannot update immutable job')" \
		"        FROM Jobs INNER JOIN JobStatus" \
		"        WHERE OLD.id = Jobs.id AND Jobs.status = JobStatus.status AND JobStatus.terminal;" \
		"    END;"

#define IMMUTABLE_JOB_INSUPD(T) \
		IMMUTABLE_JOB_INSERT(T) \
		IMMUTABLE_JOB_UPDATE(T)

/* TODO trace (log) all SQL commands: http://stackoverflow.com/questions/1607368/sql-query-logging-for-sqlite */
static int db_create (const char *path)
{
	static const char Create[] =
		"BEGIN EXCLUSIVE TRANSACTION;"
		"PRAGMA foreign_keys = ON;"
		"CREATE TABLE Jobs (id INTEGER PRIMARY KEY,"
		"                   error TEXT,"
		"                   executable TEXT NOT NULL," /* no UTF encoding? */
		"                   exit_code INTEGER,"
		"                   exit_status TEXT,"
		"                   exit_signal INTEGER,"
		"                   priority INTEGER NOT NULL DEFAULT 1,"
		"                   status TEXT NOT NULL DEFAULT 'CREATED',"
		"                   subject TEXT NOT NULL," /* no UTF encoding? */
		"                   time_commit DATETIME,"
		"                   time_create DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
		"                   time_error DATETIME,"
		"                   time_finish DATETIME,"
		"                   time_kill DATETIME,"
		"                   time_start DATETIME,"
		"                   url TEXT NOT NULL," /* no UTF encoding? */
		"                   FOREIGN KEY (status) REFERENCES JobStatus(status),"
		"                   FOREIGN KEY (exit_status) REFERENCES ExitStatus(status));"
		IMMUTABLE_JOB_UPDATE("Jobs")
		/* We pull this out to allow INSERT of time_reap on a terminal job. */
		"CREATE TABLE JobReaped (id INTEGER PRIMARY KEY,"
		"                        time_reap DATETIME NOT NULL,"
		"                        FOREIGN KEY (id) REFERENCES Jobs (id));"
		IMMUTABLE_JOB_UPDATE("JobReaped")
		"CREATE VIEW JobsPublic AS"
		"    SELECT Jobs.id,"
		"           Jobs.error,"
		"           Jobs.executable,"
		"           Jobs.exit_code,"
		"           Jobs.exit_status,"
		"           Jobs.exit_signal,"
		"           Jobs.priority,"
		"           Jobs.status,"
		"           Jobs.subject,"
		"           Jobs.time_commit,"
		"           Jobs.time_create,"
		"           Jobs.time_error,"
		"           Jobs.time_finish,"
		"           Jobs.time_kill,"
		"           Jobs.time_start,"
		"           JobReaped.time_reap"
		"    FROM Jobs NATURAL LEFT OUTER JOIN JobReaped;"
		/* We use JobStatus as an SQLite enum */
		"CREATE TABLE JobStatus (status TEXT PRIMARY KEY,"
		"                        terminal BOOL NOT NULL);"
		"INSERT INTO JobStatus VALUES ('CREATED', 0);"
		"INSERT INTO JobStatus VALUES ('COMMITTED', 0);"
		"INSERT INTO JobStatus VALUES ('ERRORED', 1);"
		"INSERT INTO JobStatus VALUES ('FINISHED', 1);"
		"INSERT INTO JobStatus VALUES ('KILLED', 1);"
		"INSERT INTO JobStatus VALUES ('STARTED', 0);"
		IMMUTABLE("JobStatus")
		/* We use ExitStatus as an SQLite enum */
		"CREATE TABLE ExitStatus (status TEXT PRIMARY KEY);"
		"INSERT INTO ExitStatus VALUES ('EXITED');"
		"INSERT INTO ExitStatus VALUES ('SIGNALED');"
		IMMUTABLE("ExitStatus")
		"CREATE TABLE JobArguments (id INTEGER,"
		"                           n INTEGER NOT NULL,"
		"                           arg TEXT NOT NULL," /* no UTF encoding? */
		"                           FOREIGN KEY (id) REFERENCES Jobs (id),"
		"                           PRIMARY KEY (id, n));"
		IMMUTABLE_JOB_INSUPD("JobArguments")
		"CREATE TABLE JobEnvironment (id INTEGER,"
		"                             name TEXT NOT NULL," /* no UTF encoding? */
		"                             value TEXT NOT NULL," /* no UTF encoding? */
		"                             FOREIGN KEY (id) REFERENCES Jobs (id),"
		"                             PRIMARY KEY (id, name));"
		IMMUTABLE_JOB_INSUPD("JobEnvironment")
		"CREATE TABLE JobFiles (id INTEGER,"
		"                       binding TEXT NOT NULL DEFAULT 'SYMLINK',"
		"                       serv_path TEXT NOT NULL," /* no UTF encoding? */
		"                       task_path TEXT NOT NULL," /* no UTF encoding? */
		"                       type TEXT NOT NULL,"
		"                       FOREIGN KEY (binding) REFERENCES FileBinding (binding),"
		"                       FOREIGN KEY (id) REFERENCES Jobs (id),"
		"                       FOREIGN KEY (type) REFERENCES FileType (type),"
		"                       PRIMARY KEY (id, task_path, type));"
		IMMUTABLE_JOB_INSUPD("JobFiles")
		/* We use FileType as an SQLite enum */
		"CREATE TABLE FileBinding (binding TEXT PRIMARY KEY);"
		"INSERT INTO FileBinding VALUES ('LINK');"
		"INSERT INTO FileBinding VALUES ('SYMLINK');"
		"INSERT INTO FileBinding VALUES ('COPY');"
		IMMUTABLE("FileBinding")
		"CREATE TABLE FileType (type TEXT PRIMARY KEY);"
		"INSERT INTO FileType VALUES ('INPUT');"
		"INSERT INTO FileType VALUES ('OUTPUT');"
		IMMUTABLE("FileType")
		"END TRANSACTION;";

	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	int rc;

	debug(D_DEBUG, "creating new database `%s'", path);
	sqlcatch(sqlite3_open_v2(path, &db, SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, NULL));
	sqlcatchexec(db, Create);

	CATCH(cfs->job_dbinit(db));
out:
	sqlite3_close(db);
	if (rc)
		unlink(path);
	return rc;
}

static int db_get (sqlite3 **dbp)
{
	static sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	int rc;

	if (db == NULL) {
		static const char filename[] = ".__job.db";
		static const char Init[] = "PRAGMA foreign_keys = ON;";

		char path[PATH_MAX];

		debug(D_DEBUG, "using sqlite version %s", sqlite3_libversion());

		if (snprintf(path, PATH_MAX, "%s/%s", chirp_transient_path, filename) >= PATH_MAX)
			fatal("transient path `%s' too long", chirp_transient_path);
		debug(D_DEBUG, "opening database `%s'", path);
		rc = sqlite3_open_v2(path, &db, SQLITE_OPEN_READWRITE, NULL);
		if (rc == SQLITE_CANTOPEN) {
			CATCH(db_create(path));
			if (rc == 0)
				sqlcatch(sqlite3_open_v2(path, &db, SQLITE_OPEN_READWRITE, NULL));
		}
		sqlcatch(rc);

		sqlcatchexec(db, Init);
	}
	rc = 0;

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
			goto invalid;\
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
					goto invalid;\
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
					goto invalid;\
				}\
			}\
		}\
		if (!v) {\
			debug(D_DEBUG, "%s[%s] is type `%s' (expected `%s')", #o, #n, "NULL", json_type_str[tt]);\
			goto invalid;\
		}\
	} while (0)

int chirp_job_create (chirp_jobid_t *id, json_value *J, const char *subject)
{
	static const char Create[] =
		"BEGIN TRANSACTION;"
		"INSERT OR ROLLBACK INTO Jobs (subject, executable, url) VALUES ( ?, ?, ? );"
		"INSERT OR ROLLBACK INTO JobArguments (id, n, arg) VALUES ( ?, ?, ? );"
		"INSERT OR REPLACE INTO JobEnvironment (id, name, value) VALUES ( ?, ?, ? );"
		"INSERT OR REPLACE INTO JobFiles (id, serv_path, task_path, type, binding) VALUES ( ?, ?, ?, ?, ? );"
		"END TRANSACTION;";

	time_t timeout = time(NULL)+3;
	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	const char *current = Create;
	int rc;

	if (!chirp_job_enabled) return ENOSYS;
	CATCH(db_get(&db));
	jchecktype(J, json_object);

restart:
	sqlcatch(sqlite3_prepare_v2(db, Create, strlen(Create)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	{
		json_value *value;
		char executable[CHIRP_PATH_MAX];
		sqlcatch(sqlite3_bind_text(stmt, 1, subject, -1, SQLITE_TRANSIENT));
		jgetnamefail(value, J, "executable", json_string);
		if (!readpath(executable, value)) goto invalid;
		sqlcatch(sqlite3_bind_text(stmt, 2, executable, -1, SQLITE_TRANSIENT));
		sqlcatch(sqlite3_bind_text(stmt, 3, chirp_url, -1, SQLITE_TRANSIENT));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		*id = sqlite3_last_insert_rowid(db); /* in SQLite, this is `id' */
		debug(D_DEBUG, "created job %" PRICHIRP_JOBID_T " as `%s' executable = `%s'", *id, subject, executable);
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	/* handle arguments */
	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	{
		int i;
		json_value *arguments;
		jgetnamefail(arguments, J, "arguments", json_array);
		for (i = 0; i < (int)arguments->u.array.length; i++) {
			sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)*id));
			sqlcatch(sqlite3_bind_int64(stmt, 2, (sqlite3_int64)i+1));
			json_value *arg = arguments->u.array.values[i];
			jchecktype(arg, json_string);
			sqlcatch(sqlite3_bind_text(stmt, 3, arg->u.string.ptr, -1, SQLITE_TRANSIENT));
			sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
			debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " bound arg %d as `%s'", *id, i+1, arg->u.string.ptr);
			sqlcatchcode(sqlite3_reset(stmt), SQLITE_OK);
		}
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	/* handle environment */
	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	{
		json_value *environment;
		jgetnameopt(environment, J, "environment", json_object);
		if (environment) {
			int i;
			for (i = 0; i < (int)environment->u.object.length; i++) {
				sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)*id));
				const char *n = environment->u.object.values[i].name;
				sqlcatch(sqlite3_bind_text(stmt, 2, n, -1, SQLITE_TRANSIENT));
				json_value *v = environment->u.object.values[i].value;
				jchecktype(v, json_string);
				sqlcatch(sqlite3_bind_text(stmt, 3, v->u.string.ptr, -1, SQLITE_TRANSIENT));
				sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
				debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " environment variable `%s'=`%s'", *id, n, v->u.string.ptr);
				sqlcatchcode(sqlite3_reset(stmt), SQLITE_OK);
			}
		}
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	/* handle files */
	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	{
		int i;
		json_value *value;
		jgetnamefail(value, J, "files", json_array);
		for (i = 0; i < (int)value->u.array.length; i++) {
			char path[CHIRP_PATH_MAX];

			json_value *file = value->u.array.values[i];
			jchecktype(file, json_object);

			sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)*id));

			json_value *serv_path;
			jgetnamefail(serv_path, file, "serv_path", json_string);
			if (strlen(serv_path->u.string.ptr) >= sizeof(path)) goto invalid;
			path_collapse(serv_path->u.string.ptr, path, 1);
			sqlcatch(sqlite3_bind_text(stmt, 2, path, -1, SQLITE_TRANSIENT));

			json_value *task_path;
			jgetnamefail(task_path, file, "task_path", json_string);
			if (strlen(task_path->u.string.ptr) >= sizeof(path)) goto invalid;
			path_collapse(task_path->u.string.ptr, path, 1);
			sqlcatch(sqlite3_bind_text(stmt, 3, path, -1, SQLITE_TRANSIENT));

			json_value *type;
			jgetnamefail(type, file, "type", json_string);
			sqlcatch(sqlite3_bind_text(stmt, 4, type->u.string.ptr, -1, SQLITE_TRANSIENT));

			json_value *binding;
			jgetnameopt(binding, file, "binding", json_string); /* can be null */
			sqlcatch(sqlite3_bind_text(stmt, 5, binding ? binding->u.string.ptr : "SYMLINK", -1, SQLITE_TRANSIENT));

			sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
			debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " new file `%s' bound as `%s' type `%s'", *id, serv_path->u.string.ptr, task_path->u.string.ptr, type->u.string.ptr);
			sqlcatchcode(sqlite3_reset(stmt), SQLITE_OK);
		}
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	assert(strlen(current) == 0);

	rc = 0;
	goto out;
invalid:
	rc = EINVAL;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	if (rc == EAGAIN && time(NULL) <= timeout) {
		usleep(2000);
		goto restart;
	}
	return rc;
}

int chirp_job_commit (json_value *J, const char *subject)
{
	static const char Commit[] =
		"BEGIN TRANSACTION;"
		/* This UPDATE may be executed multiple times. That's why it's wrapped in a transaction. */
		"UPDATE OR ROLLBACK Jobs"
		"    SET status = 'COMMITTED', time_commit = CURRENT_TIMESTAMP"
		"    WHERE id = ? AND (? OR subject = ?) AND status = 'CREATED';"
		"END TRANSACTION;";

	time_t timeout = time(NULL)+3;
	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	const char *current;
	int rc;
	int i;

	if (!chirp_job_enabled) return ENOSYS;
	CATCH(db_get(&db));
	jchecktype(J, json_array);

restart:
	sqlcatch(sqlite3_prepare_v2(db, Commit, strlen(Commit)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatch(sqlite3_bind_int(stmt, 2, strcmp(subject, chirp_super_user) == 0));
	sqlcatch(sqlite3_bind_text(stmt, 3, subject, -1, SQLITE_TRANSIENT));
	for (i = 0; i < (int)J->u.array.length; i++) {
		int n;
		chirp_jobid_t id;
		jchecktype(J->u.array.values[i], json_integer);
		id = J->u.array.values[i]->u.integer;
		sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		n = sqlite3_changes(db);
		assert(n == 0 || n == 1);
		if (n) {
			debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " is committed", id);
		} else {
			debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " not changed", id);
			rc = EPERM; /* TODO we may want finer granularity of errors in the future. */
			goto out;
		}
		sqlcatchcode(sqlite3_reset(stmt), SQLITE_OK);
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
invalid:
	rc = EINVAL;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	if (rc == EAGAIN && time(NULL) <= timeout) {
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
		"UPDATE Jobs"
		"    SET status = 'KILLED', time_kill = CURRENT_TIMESTAMP"
		"    WHERE id IN (SELECT Jobs.id"
		"                     FROM Jobs NATURAL JOIN JobStatus"
		"                     WHERE id = ? AND (? OR subject = ?) AND NOT JobStatus.terminal);"
		"END TRANSACTION;";

	time_t timeout = time(NULL)+3;
	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	const char *current;
	int rc;
	int i;

	if (!chirp_job_enabled) return ENOSYS;
	CATCH(db_get(&db));
	jchecktype(J, json_array);

restart:
	sqlcatch(sqlite3_prepare_v2(db, Kill, strlen(Kill)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatch(sqlite3_bind_int(stmt, 2, strcmp(subject, chirp_super_user) == 0));
	sqlcatch(sqlite3_bind_text(stmt, 3, subject, -1, SQLITE_TRANSIENT));
	for (i = 0; i < (int)J->u.array.length; i++) {
		int n;
		chirp_jobid_t id;
		jchecktype(J->u.array.values[i], json_integer);
		id = J->u.array.values[i]->u.integer;
		sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		n = sqlite3_changes(db);
		assert(n == 0 || n == 1);
		if (n) {
			debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " is killed", id);
		} else {
			debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " not killed", id);
			rc = EPERM; /* TODO we may want finer granularity of errors in the future. */
			goto out;
		}
		sqlcatchcode(sqlite3_reset(stmt), SQLITE_OK);
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
invalid:
	rc = EINVAL;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	if (rc == EAGAIN && time(NULL) <= timeout) {
		usleep(2000);
		goto restart;
	}
	return rc;
}

static int jsonify (sqlite3 *db, sqlite3_stmt *stmt, int n, buffer_t *B)
{
	int rc = 0;
	switch (sqlite3_column_type(stmt, n)) {
		case SQLITE_NULL:
			CATCHCODE(buffer_putliteral(B, "null"), -1);
			break;
		case SQLITE_INTEGER:
			CATCHCODE(buffer_printf(B, "%" PRId64, (int64_t) sqlite3_column_int64(stmt, n)), -1);
			break;
		case SQLITE_FLOAT:
			CATCHCODE(buffer_printf(B, "%.*e", DBL_DIG, sqlite3_column_double(stmt, n)), -1);
			break;
		case SQLITE_TEXT: {
			const unsigned char *str;

			CATCHCODE(buffer_putliteral(B, "\""), -1);
			for (str = sqlite3_column_text(stmt, n); *str; str++) {
				switch (*str) {
					case '\\': CATCHCODE(buffer_putliteral(B, "\\\\"), -1); break;
					case '"':  CATCHCODE(buffer_putliteral(B, "\\\""), -1); break;
					case '/':  CATCHCODE(buffer_putliteral(B, "\\/"), -1); break;
					case '\b': CATCHCODE(buffer_putliteral(B, "\\b"), -1); break;
					case '\f': CATCHCODE(buffer_putliteral(B, "\\f"), -1); break;
					case '\n': CATCHCODE(buffer_putliteral(B, "\\n"), -1); break;
					case '\r': CATCHCODE(buffer_putliteral(B, "\\r"), -1); break;
					case '\t': CATCHCODE(buffer_putliteral(B, "\\t"), -1); break;
					default:   CATCHCODE(buffer_printf(B, "%c", (int)*str), -1); break;
				}
			}
			CATCHCODE(buffer_putliteral(B, "\""), -1);
			break;
		}
		case SQLITE_BLOB:
		default:
			assert(0); /* we don't handle this */
	}
out:
	return rc;
}

/* TODO In the future, this will need to be adjusted to handle the file
 * system's (i.e. Confuga's) extra data such as the output replica IDs. This
 * should probably be done in a View all file systems make, perhaps called
 * JobFSPublic */
int chirp_job_status (json_value *J, const char *subject, buffer_t *B)
{
	static const char Status[] =
		/* These SELECTs will be executed multiple times, for each job in J. */
		"SELECT JobsPublic.* FROM JobsPublic WHERE id = ? AND (? OR JobsPublic.subject = ?);" /* subject check happens here */
		"SELECT arg FROM JobArguments WHERE id = ? ORDER BY n;"
		"SELECT name, value FROM JobEnvironment WHERE id = ?;"
		"SELECT serv_path, task_path, type, binding FROM JobFiles WHERE id = ?;";

	time_t timeout = time(NULL)+3;
	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	int rc;
	int i;
	size_t start = buffer_pos(B);

	if (!chirp_job_enabled) return ENOSYS;
	CATCH(db_get(&db));
	jchecktype(J, json_array);

restart:
	buffer_rewind(B, start); /* a failed job_status may add to buffer */

	sqlcatchexec(db, "BEGIN TRANSACTION;");

	CATCHCODE(buffer_putliteral(B, "["), -1);
	for (i = 0; i < (int)J->u.array.length; i++) {
		const char *current;
		chirp_jobid_t id;
		int first1 = 1;

		jchecktype(J->u.array.values[i], json_integer);
		id = J->u.array.values[i]->u.integer;

		if (i)
			buffer_putliteral(B, ",");

		sqlcatch(sqlite3_prepare_v2(db, Status, strlen(Status)+1, &stmt, &current));
		sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
		sqlcatch(sqlite3_bind_int(stmt, 2, strcmp(subject, chirp_super_user) == 0));
		sqlcatch(sqlite3_bind_text(stmt, 3, subject, -1, SQLITE_TRANSIENT));
		if ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
		{
			int j;

			CATCHCODE(buffer_putliteral(B, "{"), -1); /* NOTE: this is matched at the end... */
			for (j = 0; j < sqlite3_column_count(stmt); j++) {
				if (first1) {
					first1 = 0;
					CATCHCODE(buffer_printf(B, "\"%s\":", sqlite3_column_name(stmt, j)), -1);
				} else {
					CATCHCODE(buffer_printf(B, ",\"%s\":", sqlite3_column_name(stmt, j)), -1);
				}
				jsonify(db, stmt, j, B);
			}
		} else if (rc == SQLITE_DONE) {
			rc = EPERM;
			goto out;
		} else {
			sqlcatch(rc);
		}
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

		first1 = 1;
		sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
		sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
		CATCHCODE(buffer_putliteral(B, ",\"arguments\":["), -1);
		while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
		{
			assert(sqlite3_column_count(stmt) == 1);
			if (first1) {
				first1 = 0;
			} else {
				CATCHCODE(buffer_putliteral(B, ","), -1);
			}
			jsonify(db, stmt, 0, B);
		}
		sqlcatchcode(rc, SQLITE_DONE);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
		CATCHCODE(buffer_putliteral(B, "]"), -1);

		first1 = 1;
		sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
		sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
		CATCHCODE(buffer_putliteral(B, ",\"environment\":{"), -1);
		while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
		{
			assert(sqlite3_column_count(stmt) == 2);

			if (first1) {
				first1 = 0;
			} else {
				CATCHCODE(buffer_putliteral(B, ","), -1);
			}
			jsonify(db, stmt, 0, B);
			CATCHCODE(buffer_putliteral(B, ":"), -1);
			jsonify(db, stmt, 1, B);
		}
		sqlcatchcode(rc, SQLITE_DONE);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
		CATCHCODE(buffer_putliteral(B, "}"), -1);

		first1 = 1;
		sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
		sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
		CATCHCODE(buffer_putliteral(B, ",\"files\":["), -1);
		while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
		{
			int j;
			int first2 = 1;

			if (first1) {
				first1 = 0;
				CATCHCODE(buffer_putliteral(B, "{"), -1);
			} else {
				CATCHCODE(buffer_putliteral(B, ",{"), -1);
			}
			for (j = 0; j < sqlite3_column_count(stmt); j++) {
				if (first2) {
					first2 = 0;
					CATCHCODE(buffer_printf(B, "\"%s\":", sqlite3_column_name(stmt, j)), -1);
				} else {
					CATCHCODE(buffer_printf(B, ",\"%s\":", sqlite3_column_name(stmt, j)), -1);
				}
				jsonify(db, stmt, j, B);
			}
			CATCHCODE(buffer_putliteral(B, "}"), -1);
		}
		sqlcatchcode(rc, SQLITE_DONE);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
		CATCHCODE(buffer_putliteral(B, "]}"), -1);
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	CATCHCODE(buffer_putliteral(B, "]"), -1);

	sqlcatchexec(db, "END TRANSACTION;");

	rc = 0;
	goto out;
invalid:
	rc = EINVAL;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	if (rc == EAGAIN && time(NULL) <= timeout) {
		usleep(2000);
		goto restart;
	}
	return rc;
}

int chirp_job_wait (chirp_jobid_t id, const char *subject, INT64_T timeout, buffer_t *B)
{
	static const char Wait[] =
		"BEGIN TRANSACTION;"
		/* So we can abort if there are no Jobs to be waited for. */
		"SELECT COUNT(*) FROM Jobs NATURAL LEFT OUTER JOIN JobReaped"
		"    WHERE (? OR Jobs.subject = ?) AND JobReaped.time_reap IS NULL;"
		/* Find Jobs we wait for. */
		"SELECT Jobs.id"
		"    FROM Jobs NATURAL JOIN JobStatus NATURAL LEFT OUTER JOIN JobReaped"
		"    WHERE (? OR Jobs.subject = ?) AND"
		"          JobStatus.terminal AND"
		"          JobReaped.time_reap IS NULL AND"
		"          (? = 0 OR Jobs.id = ? OR (? < 0 AND -Jobs.id <= ?))"
		"    LIMIT 1024;"
		"END TRANSACTION;";

	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	int rc;
	int i, n;
	chirp_jobid_t jobs[1024];
	json_value *J=NULL;

	if (!chirp_job_enabled) return ENOSYS;
	CATCH(db_get(&db));

	if (timeout < 0) {
		timeout = CHIRP_JOB_WAIT_MAX_TIMEOUT+time(NULL);
	} else if (timeout > 0) {
		timeout = MIN(timeout, CHIRP_JOB_WAIT_MAX_TIMEOUT)+time(NULL);
	}

restart:
	n = 0;
	J = NULL;

	do {
		const char *current;

		sqlcatch(sqlite3_prepare_v2(db, Wait, strlen(Wait)+1, &stmt, &current));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

		sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
		sqlcatch(sqlite3_bind_int(stmt, 1, strcmp(subject, chirp_super_user) == 0));
		sqlcatch(sqlite3_bind_text(stmt, 2, subject, -1, SQLITE_TRANSIENT));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
		assert(sqlite3_column_count(stmt) == 1 && sqlite3_column_type(stmt, 0) == SQLITE_INTEGER);
		if (sqlite3_column_int(stmt, 0) == 0) {
			rc = ESRCH;
			goto out;
		}
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

		sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
		sqlcatch(sqlite3_bind_int(stmt, 1, strcmp(subject, chirp_super_user) == 0));
		sqlcatch(sqlite3_bind_text(stmt, 2, subject, -1, SQLITE_TRANSIENT));
		sqlcatch(sqlite3_bind_int64(stmt, 3, (sqlite3_int64)id));
		sqlcatch(sqlite3_bind_int64(stmt, 4, (sqlite3_int64)id));
		sqlcatch(sqlite3_bind_int64(stmt, 5, (sqlite3_int64)id));
		sqlcatch(sqlite3_bind_int64(stmt, 6, (sqlite3_int64)id));
		while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
			assert(sqlite3_column_count(stmt) == 1 && sqlite3_column_type(stmt, 0) == SQLITE_INTEGER);
			jobs[n++] = (chirp_jobid_t)sqlite3_column_int64(stmt, 0);
		}
		sqlcatchcode(rc, SQLITE_DONE);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

		sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

		if (n == 0)
			usleep(5000);
	} while (time(NULL) <= timeout && n == 0);

	{
		int first = 1;
		buffer_t Bstatus;
		buffer_init(&Bstatus);
		buffer_abortonfailure(&Bstatus, 1);

		buffer_putliteral(&Bstatus, "[");
		for (i = 0; i < n; i++) {
			if (first)
				buffer_putfstring(&Bstatus, "%" PRICHIRP_JOBID_T, jobs[i]);
			else
				buffer_putfstring(&Bstatus, ",%" PRICHIRP_JOBID_T, jobs[i]);
			first = 0;
		}
		buffer_putliteral(&Bstatus, "]");

		J = json_parse(buffer_tostring(&Bstatus, NULL), buffer_pos(&Bstatus));
		assert(J);
		buffer_free(&Bstatus);
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
		usleep(2000);
		goto restart;
	}
	return rc;
}

int chirp_job_reap (json_value *J, const char *subject)
{
	static const char Reap[] =
		"BEGIN TRANSACTION;"
		/* Note: If a job id given has a subject mismatch, has been waited for already, or is not terminal, this INSERT silently does nothing. */
		/* This INSERT may be executed multiple times. That's why it's wrapped in a transaction. */
		"INSERT OR ROLLBACK INTO JobReaped (id, time_reap)"
		"    SELECT Jobs.id, CURRENT_TIMESTAMP"
		"        FROM Jobs NATURAL JOIN JobStatus NATURAL LEFT OUTER JOIN JobReaped"
		"        WHERE Jobs.id == ? AND"
		"              (? OR Jobs.subject = ?) AND"
		"              JobStatus.terminal AND"
		"              JobReaped.time_reap IS NULL;"
		"END TRANSACTION;";

	time_t timeout = time(NULL)+3;
	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	const char *current;
	int rc;
	int i;

	if (!chirp_job_enabled) return ENOSYS;
	CATCH(db_get(&db));
	jchecktype(J, json_array);

restart:
	sqlcatch(sqlite3_prepare_v2(db, Reap, strlen(Reap)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatch(sqlite3_bind_int(stmt, 2, strcmp(subject, chirp_super_user) == 0));
	sqlcatch(sqlite3_bind_text(stmt, 3, subject, -1, SQLITE_TRANSIENT));
	for (i = 0; i < (int)J->u.array.length; i++) {
		chirp_jobid_t id;
		jchecktype(J->u.array.values[i], json_integer);
		id = J->u.array.values[i]->u.integer;
		sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		if (sqlite3_changes(db)) {
			debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " reaped", id);
		} else {
			debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " not reaped", id);
		}
		sqlcatchcode(sqlite3_reset(stmt), SQLITE_OK);
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	assert(strlen(current) == 0);

	rc = 0;
	goto out;
invalid:
	rc = EINVAL;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	if (rc == EAGAIN && time(NULL) <= timeout) {
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
	CATCH(db_get(&db));

	debug(D_DEBUG, "scheduler running with concurrency: %d", chirp_job_concurrency);
	debug(D_DEBUG, "scheduler running with time limit: %d", chirp_job_time_limit);

	CATCH(cfs->job_schedule(db));
out:
	return rc;
}

/* vim: set noexpandtab tabstop=4: */
