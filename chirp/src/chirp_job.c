#include "chirp_filesystem.h"
#include "chirp_job.h"
#include "chirp_sqlite.h"
#include "chirp_types.h"

#include "debug.h"
#include "json.h"
#include "macros.h"
#include "path.h"

#include <assert.h>
#include <errno.h>
#include <float.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern const char *chirp_super_user;
extern char        chirp_transient_path[PATH_MAX];

int   chirp_job_concurrency = 1;
pid_t chirp_job_schedd = 0;
int   chirp_job_time_limit = 3600; /* 1 hour */

#define CATCH(expr) \
	do {\
		rc = (expr);\
		if (rc) goto out;\
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
		IMMUTABLE("FileType");

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

static sqlite3 *db_get (void)
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
	return db;
}

static int readpath (char file[CHIRP_PATH_MAX], json_value *J) {
	if (J->type != json_string || J->u.string.length+1 > CHIRP_PATH_MAX)
		return 0;
	strcpy(file, J->u.string.ptr);
	return 1;
}

#define jistype(o,t) ((o)->type == (t))

static json_value *jgetname (json_value *object, const char *name, json_type t)
{
	unsigned int i;
	assert(object->type == json_object);
	for (i = 0; i < object->u.object.length; i++) {
		if (strcmp(name, object->u.object.values[i].name) == 0) {
			if (jistype(object->u.object.values[i].value, t)) {
				return object->u.object.values[i].value;
			} else {
				return NULL;
			}
		}
	}
	return NULL;
}

int chirp_job_create (chirp_jobid_t *id, json_value *J, const char *subject)
{
	static const char Create[] =
		"BEGIN TRANSACTION;"
		"INSERT OR ROLLBACK INTO Jobs (subject, executable, url) VALUES ( ?, ?, ? );"
		"INSERT OR ROLLBACK INTO JobArguments (id, n, arg) VALUES ( ?, ?, ? );"
		"INSERT OR ROLLBACK INTO JobEnvironment (id, name, value) VALUES ( ?, ?, ? );"
		"INSERT OR ROLLBACK INTO JobFiles (id, serv_path, task_path, type, binding) VALUES ( ?, ?, ?, ?, ? );"
		"END TRANSACTION;";

	time_t timeout = time(NULL)+3;
	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	const char *current = Create;
	int rc;

	if (!(db = db_get())) return EIO;
	if (!jistype(J, json_object)) goto invalid;

restart:
	sqlcatch(sqlite3_prepare_v2(db, Create, strlen(Create)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	{
		sqlcatch(sqlite3_bind_text(stmt, 1, subject, -1, SQLITE_TRANSIENT));
		char executable[CHIRP_PATH_MAX];
		json_value *value = jgetname(J, "executable", json_string);
		if (!value) goto invalid;
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
		json_value *value = jgetname(J, "arguments", json_array);
		if (!value) goto invalid;
		for (i = 0; i < (int)value->u.array.length; i++) {
			sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)*id));
			sqlcatch(sqlite3_bind_int64(stmt, 2, (sqlite3_int64)i+1));
			json_value *arg = value->u.array.values[i];
			if (!jistype(arg, json_string)) goto invalid;
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
		json_value *value = jgetname(J, "environment", json_object);
		if (value) {
			int i;
			for (i = 0; i < (int)value->u.object.length; i++) {
				sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)*id));
				const char *n = value->u.object.values[i].name;
				sqlcatch(sqlite3_bind_text(stmt, 2, n, -1, SQLITE_TRANSIENT));
				json_value *v = value->u.object.values[i].value;
				if (!jistype(v, json_string)) goto invalid;
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
		json_value *value = jgetname(J, "files", json_array);
		if (!value) goto invalid;
		for (i = 0; i < (int)value->u.array.length; i++) {
			char path[CHIRP_PATH_MAX];

			json_value *file = value->u.array.values[i];
			if (!jistype(file, json_object)) goto invalid;

			sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)*id));

			json_value *serv_path = jgetname(file, "serv_path", json_string);
			if (!serv_path || strlen(serv_path->u.string.ptr) >= sizeof(path)) goto invalid;
			path_collapse(serv_path->u.string.ptr, path, 1);
			sqlcatch(sqlite3_bind_text(stmt, 2, path, -1, SQLITE_TRANSIENT));

			json_value *task_path = jgetname(file, "task_path", json_string);
			if (!task_path || strlen(task_path->u.string.ptr) >= sizeof(path)) goto invalid;
			path_collapse(task_path->u.string.ptr, path, 1);
			sqlcatch(sqlite3_bind_text(stmt, 3, path, -1, SQLITE_TRANSIENT));

			json_value *type = jgetname(file, "type", json_string);
			if (!type) goto invalid;
			sqlcatch(sqlite3_bind_text(stmt, 4, type->u.string.ptr, -1, SQLITE_TRANSIENT));

			json_value *binding = jgetname(file, "binding", json_string); /* can be null */
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
		debug(D_DEBUG, "the database is busy, restarting create");
		usleep(2000);
		goto restart;
	}
	return rc;
}

int chirp_job_commit (chirp_jobid_t id, const char *subject)
{
	static const char Commit[] =
		/* FIXME subject failure abort */
		"UPDATE OR ROLLBACK Jobs"
		"    SET status = 'COMMITTED', time_commit = CURRENT_TIMESTAMP"
		"    WHERE id = ? AND (? OR subject = ?) AND status = 'CREATED';";

	time_t timeout = time(NULL)+3;
	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	int rc;

	if (!(db = db_get())) return EIO;

restart:
	sqlcatch(sqlite3_prepare_v2(db, Commit, strlen(Commit)+1, &stmt, NULL));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	sqlcatch(sqlite3_bind_int(stmt, 2, strcmp(subject, chirp_super_user) == 0));
	sqlcatch(sqlite3_bind_text(stmt, 3, subject, -1, SQLITE_TRANSIENT));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	int n = sqlite3_changes(db);
	assert(n == 0 || n == 1);
	if (n) {
		debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " is committed", id);
		rc = 0;
	} else {
		debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " not changed", id);
		rc = EPERM;
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;

out:
	sqlite3_finalize(stmt);
	sqlend(db);
	if (rc == EAGAIN && time(NULL) <= timeout) {
		debug(D_DEBUG, "the database is busy, restarting commit");
		usleep(2000);
		goto restart;
	}
	return rc;
}

int chirp_job_kill (chirp_jobid_t id, const char *subject)
{
	static const char Kill[] =
		"UPDATE Jobs"
		"    SET status = 'KILLED', time_kill = CURRENT_TIMESTAMP"
		"    WHERE id IN (SELECT Jobs.id"
		"                     FROM Jobs NATURAL JOIN JobStatus"
		"                     WHERE id = ? AND (? OR subject = ?) AND NOT JobStatus.terminal);";

	time_t timeout = time(NULL)+3;
	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	int rc;

	if (!(db = db_get())) return EIO;

restart:
	sqlcatch(sqlite3_prepare_v2(db, Kill, strlen(Kill)+1, &stmt, NULL));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	sqlcatch(sqlite3_bind_int(stmt, 2, strcmp(subject, chirp_super_user) == 0));
	sqlcatch(sqlite3_bind_text(stmt, 3, subject, -1, SQLITE_TRANSIENT));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	int n = sqlite3_changes(db);
	assert(n == 0 || n == 1);
	if (n) {
		debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " is killed", id);
		rc = 0;
	} else {
		debug(D_DEBUG, "job %" PRICHIRP_JOBID_T " not killed", id);
		rc = EPERM;
	}
out:
	sqlite3_finalize(stmt);
	if (rc == EAGAIN && time(NULL) <= timeout) {
		debug(D_DEBUG, "the database is busy, restarting kill");
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
			CATCH(buffer_putliteral(B, "null"));
			break;
		case SQLITE_INTEGER:
			CATCH(buffer_printf(B, "%" PRId64, (int64_t) sqlite3_column_int64(stmt, n)));
			break;
		case SQLITE_FLOAT:
			CATCH(buffer_printf(B, "%.*e", DBL_DIG, sqlite3_column_double(stmt, n)));
			break;
		case SQLITE_TEXT: {
			const unsigned char *str;

			CATCH(buffer_putliteral(B, "\""));
			for (str = sqlite3_column_text(stmt, n); *str; str++) {
				switch (*str) {
					case '\\': CATCH(buffer_putliteral(B, "\\\\")); break;
					case '"':  CATCH(buffer_putliteral(B, "\\\"")); break;
					case '/':  CATCH(buffer_putliteral(B, "\\/")); break;
					case '\b': CATCH(buffer_putliteral(B, "\\b")); break;
					case '\f': CATCH(buffer_putliteral(B, "\\f")); break;
					case '\n': CATCH(buffer_putliteral(B, "\\n")); break;
					case '\r': CATCH(buffer_putliteral(B, "\\r")); break;
					case '\t': CATCH(buffer_putliteral(B, "\\t")); break;
					default:   CATCH(buffer_printf(B, "%c", (int)*str)); break;
				}
			}
			CATCH(buffer_putliteral(B, "\""));
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
int chirp_job_status (chirp_jobid_t id, const char *subject, buffer_t *B)
{
	static const char Status[] =
		"BEGIN TRANSACTION;"
		"SELECT JobsPublic.* FROM JobsPublic WHERE id = ? AND (? OR JobsPublic.subject = ?);" /* subject check happens here */
		"SELECT arg FROM JobArguments WHERE id = ? ORDER BY n;"
		"SELECT name, value FROM JobEnvironment WHERE id = ?;"
		"SELECT serv_path, task_path, type, binding FROM JobFiles WHERE id = ?;"
		"END TRANSACTION;";

	time_t timeout = time(NULL)+3;
	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	int rc;
	const char *current;
	int first1 = 1;

	if (!(db = db_get())) return EIO;

restart:
	sqlcatch(sqlite3_prepare_v2(db, Status, strlen(Status)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	sqlcatch(sqlite3_bind_int(stmt, 2, strcmp(subject, chirp_super_user) == 0));
	sqlcatch(sqlite3_bind_text(stmt, 3, subject, -1, SQLITE_TRANSIENT));
	if ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
	{
		int i;

		CATCH(buffer_putliteral(B, "{")); /* NOTE: this is matched at the end... */
		for (i = 0; i < sqlite3_column_count(stmt); i++) {

			if (first1) {
				first1 = 0;
				CATCH(buffer_printf(B, "\"%s\":", sqlite3_column_name(stmt, i)));
			} else {
				CATCH(buffer_printf(B, ",\"%s\":", sqlite3_column_name(stmt, i)));
			}
			jsonify(db, stmt, i, B);
		}
	} else if (rc == SQLITE_DONE) {
		rc = EPERM;
		goto out;
	} else {
		sqlcatch(rc);
	}
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	first1 = 1;
	CATCH(buffer_putliteral(B, ",\"arguments\":["));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
	{
		assert(sqlite3_column_count(stmt) == 1);
		if (first1) {
			first1 = 0;
		} else {
			CATCH(buffer_putliteral(B, ","));
		}
		jsonify(db, stmt, 0, B);
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	CATCH(buffer_putliteral(B, "]"));

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	first1 = 1;
	CATCH(buffer_putliteral(B, ",\"environment\":{"));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
	{
		assert(sqlite3_column_count(stmt) == 2);

		if (first1) {
			first1 = 0;
		} else {
			CATCH(buffer_putliteral(B, ","));
		}
		jsonify(db, stmt, 0, B);
		CATCH(buffer_putliteral(B, ":"));
		jsonify(db, stmt, 1, B);
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	CATCH(buffer_putliteral(B, "}"));

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	first1 = 1;
	CATCH(buffer_putliteral(B, ",\"files\":["));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
	{
		int i;
		int first2 = 1;

		if (first1) {
			first1 = 0;
			CATCH(buffer_putliteral(B, "{"));
		} else {
			CATCH(buffer_putliteral(B, ",{"));
		}
		for (i = 0; i < sqlite3_column_count(stmt); i++) {
			if (first2) {
				first2 = 0;
				CATCH(buffer_printf(B, "\"%s\":", sqlite3_column_name(stmt, i)));
			} else {
				CATCH(buffer_printf(B, ",\"%s\":", sqlite3_column_name(stmt, i)));
			}
			jsonify(db, stmt, i, B);
		}
		CATCH(buffer_putliteral(B, "}"));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	CATCH(buffer_putliteral(B, "]}"));

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	if (rc == EAGAIN && time(NULL) <= timeout) {
		debug(D_DEBUG, "the database is busy, restarting reap");
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

	if (!(db = db_get())) return EIO;

	if (timeout < 0) {
		timeout = CHIRP_JOB_WAIT_MAX_TIMEOUT+time(NULL);
	} else if (timeout > 0) {
		timeout = MIN(timeout, CHIRP_JOB_WAIT_MAX_TIMEOUT)+time(NULL);
	}

restart:
	stmt = NULL;
	rc = 0;
	n = 0;

	do {
		const char *current = Wait;

		sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
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

	buffer_putliteral(B, "[");
	for (i = 0; i < n; i++) {
		if (i == 0) {
			CATCH(chirp_job_status(jobs[i], subject, B));
		} else {
			buffer_putliteral(B, ",");
			CATCH(chirp_job_status(jobs[i], subject, B));
		}
	}
	buffer_putliteral(B, "]");

	rc = 0;
	goto out;

out:
	sqlite3_finalize(stmt);
	sqlend(db);
	if (rc == EAGAIN && time(NULL) <= timeout) {
		debug(D_DEBUG, "the database is busy, restarting wait");
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
		"              JobStatus.terminal;"
		"END TRANSACTION;";

	time_t timeout = time(NULL)+3;
	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	const char *current;
	int rc;
	int i;

	if (!(db = db_get())) return EIO;
	if (!jistype(J, json_array)) goto invalid;

restart:
	sqlcatch(sqlite3_prepare_v2(db, Reap, strlen(Reap)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatch(sqlite3_bind_int(stmt, 2, strcmp(subject, chirp_super_user) == 0));
	sqlcatch(sqlite3_bind_text(stmt, 3, subject, -1, SQLITE_TRANSIENT));
	for (i = 0; i < (int)J->u.array.length; i++) {
		chirp_jobid_t id;
		if (!jistype(J->u.array.values[i], json_integer)) goto invalid;
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
	/* FIXME we have this annoying problem where sqlend finishes a transaction on error for RPC like status but is worthless for these RPCs... Perhaps only have sqlend END TRANSACTION for selects? */
	if (rc == EAGAIN && time(NULL) <= timeout) {
		debug(D_DEBUG, "the database is busy, restarting reap");
		usleep(2000);
		goto restart;
	}
	return rc;
}

int chirp_job_schedule (void)
{
	sqlite3 *db = NULL;
	if (!(db = db_get())) return EIO;

	debug(D_DEBUG, "scheduler running with concurrency: %d", chirp_job_concurrency);
	debug(D_DEBUG, "scheduler running with time limit: %d", chirp_job_time_limit);

	return cfs->job_schedule(db);
}

/* vim: set noexpandtab tabstop=4: */
