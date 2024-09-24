/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "confuga_fs.h"

#include "debug.h"
#include "json.h"
#include "json_aux.h"

#include "catch.h"
#include "chirp_reli.h"
#include "chirp_sqlite.h"
#include "chirp_types.h"

#include <sys/socket.h>

#include <assert.h>
#include <float.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>

#define streql(s1,s2) (strcmp(s1,s2) == 0)

#define STOPTIME (time(NULL)+5)

#define CONFUGA_OUTPUT_TAG "confuga-output-fid"
#define CONFUGA_PULL_TAG "confuga-pull-fid"

struct job_stats {
	confuga_off_t pull_bytes;
	uint64_t      pull_count;
	uint64_t      repl_bytes;
	uint64_t      repl_count;
};

/* TODO:
 *
 * o Separate db instances for Confuga/Chirp Job. Use synchronization code.
 * o Tagged Chirp jobs for wait. Propagate tag for accounting?
 * o Move all Chirp job stuff to separate table/code for unification:
 *   o batch job operations (create/commit/wait/reap) and success/failure
 *   o Chirp job code can transparently retry some operations, like CREATE/COMMIT.
 *   o Tables:
 *     o JobInputFile <id, task_path, serv_path>
 *     o JobInputFID  <id, task_path, fid> (ChirpJob and ConfugaJob share same id?)
 *     o JobOutputFID <id, task_path, fid> (ChirpJob and ConfugaJob share same id?)
 * o Hash replicas for health check.
 * o Turn on delayed replication; gives job scheduling a chance to choose targets.
 * o Allow for multi-core SN.
 */

static void jdebug (uint64_t level, chirp_jobid_t id, const char *tag, const char *fmt, ...)
{
	va_list va;
	BUFFER_STACK_PRINT(B, 4096, "job %" PRICHIRP_JOBID_T " (`%s'): %s", id, tag, fmt)
	va_start(va, fmt);
	vdebug(level, buffer_tostring(B), va);
	va_end(va);
}

CONFUGA_API int confuga_job_dbinit (confuga *C, sqlite3 *db)
{
	static const char SQL[] =
		"BEGIN TRANSACTION;"
		"CREATE TABLE ConfugaJob ("
		"	id INTEGER PRIMARY KEY REFERENCES Job (id),"
		"	cid INTEGER," /* job id on sid */
		"	sid INTEGER,"
		"	error TEXT,"
		"	pull_bytes INTEGER," /* bytes pulled by job */
		"	pull_count INTEGER," /* # of files pulled by job */
		"	repl_bytes INTEGER," /* bytes colocated on SN when job was scheduled */
		"	repl_count INTEGER," /* # of files colocated on SN when job was scheduled */
		"	tag TEXT NOT NULL DEFAULT '(unknown)',"
		"   time_new DATETIME NOT NULL DEFAULT (strftime('%s', 'now')),"
		"	time_bound_inputs DATETIME,"
		"	time_scheduled DATETIME,"
		"	time_replicated DATETIME,"
		"	time_created DATETIME,"
		"	time_committed DATETIME,"
		"	time_waited DATETIME,"
		"	time_reaped DATETIME,"
		"	time_bound_outputs DATETIME,"
		"	time_errored DATETIME,"
		"	time_killed DATETIME,"
		"	state TEXT NOT NULL REFERENCES ConfugaJobState (state));"
		/* Fields collected from Chirp job we have waited for. */
		"CREATE TABLE ConfugaJobWaitResult ("
		"	id INTEGER PRIMARY KEY REFERENCES ConfugaJob (id),"
		"	error TEXT,"
		"	exit_code INTEGER,"
		"	exit_signal TEXT,"
		"	exit_status TEXT REFERENCES ExitStatus (status),"
		"	status TEXT NOT NULL REFERENCES JobStatus (status));"
		/* TODO add "attempts" here, use TRIGGER maybe to reset to 0 */
		//"CREATE TABLE ConfugaJobSchedulingAttempt ("
		//"	id INTEGER NOT NULL REFERENCES ConfugaJob (id),"
		//"	jid INTEGER," /* job id on sid */
		//"	sid INTEGER NOT NULL,"
		//"	time_scheduled DATETIME NOT NULL,"
		//"	PRIMARY KEY (id, sid));"
		"CREATE TABLE ConfugaJobState (state TEXT PRIMARY KEY, allocated INTEGER NOT NULL, executing INTEGER NOT NULL);"
		"INSERT INTO ConfugaJobState (state, allocated, executing) VALUES"
		"	('NEW', 0, 0),"
		"	('BOUND_INPUTS', 0, 0),"
		"	('SCHEDULED', 1, 0),"
		"	('REPLICATED', 1, 0),"
		"	('CREATED', 1, 1),"
		"	('COMMITTED', 1, 1),"
		"	('WAITED', 1, 1),"
		"	('REAPED', 0, 0),"
		"	('BOUND_OUTPUTS', 0, 0)," /* aka 'COMPLETED' */
		"	('ERRORED', 0, 0),"
		"	('KILLED', 0, 0);"
		IMMUTABLE("ConfugaJobState")
		"CREATE TABLE ConfugaInputFile ("
		"	fid BLOB NOT NULL,"
		"	jid INTEGER REFERENCES ConfugaJob (id),"
		"	task_path TEXT NOT NULL,"
		"	PRIMARY KEY (jid, task_path));"
		"CREATE TABLE ConfugaOutputFile ("
		"	fid BLOB NOT NULL,"
		"	jid INTEGER REFERENCES ConfugaJob (id),"
		"	size INTEGER NOT NULL,"
		"	task_path TEXT NOT NULL,"
		"	PRIMARY KEY (jid, task_path));"
		"CREATE VIEW ConfugaJobAllocated AS"
		"	SELECT ConfugaJob.* FROM ConfugaJob NATURAL JOIN ConfugaJobState WHERE ConfugaJobState.allocated = 1;"
		"CREATE VIEW ConfugaJobExecuting AS"
		"	SELECT ConfugaJob.* FROM ConfugaJob NATURAL JOIN ConfugaJobState WHERE ConfugaJobState.executing = 1;"
		"END TRANSACTION;";

	int rc;
	char *errmsg = NULL;

	debug(D_DEBUG, "initializing ConfugaJob DB");
	rc = sqlite3_exec(db, SQL, NULL, NULL, &errmsg); /* Ignore any errors. */
	if (rc) {
		if (!strstr(sqlite3_errmsg(db), "table ConfugaJob already exists"))
			debug(D_DEBUG, "[%s:%d] sqlite3 error: %d `%s': %s", __FILE__, __LINE__, rc, sqlite3_errstr(rc), sqlite3_errmsg(db));
		sqlite3_exec(db, "ROLLBACK TRANSACTION;", NULL, NULL, NULL);
	}
	sqlite3_free(errmsg);

	rc = 0;
	goto out;
out:
	return rc;
}

CONFUGA_API int confuga_job_attach (confuga *C, sqlite3 *db)
{
	int rc;

	CATCH(confugaI_dbclose(C));
	CATCH(confugaI_dbload(C, db));

	rc = 0;
	goto out;
out:
	return rc;
}

static int fail (confuga *C, chirp_jobid_t id, const char *tag, const char *error)
{
	static const char SQL[] =
		"BEGIN TRANSACTION;"
		"UPDATE Job"
		"	SET"
		"		error = ?,"
		"		status = 'ERRORED',"
		"		time_error = strftime('%s', 'now')"
		"	WHERE id = ?;"
		"UPDATE ConfugaJob"
		"	SET"
		"		error = ?,"
		"		state = 'ERRORED',"
		"		time_errored = strftime('%s', 'now')"
		"	WHERE id = ?;"
		"END TRANSACTION;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	jdebug(D_DEBUG, id, tag, "fatal error: %s", error);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_text(stmt, 1, error, -1, SQLITE_STATIC));
	sqlcatch(sqlite3_bind_int64(stmt, 2, id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_text(stmt, 1, error, -1, SQLITE_STATIC));
	sqlcatch(sqlite3_bind_int64(stmt, 2, id));
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
	return rc;
}

/* TODO This is all pretty evil since jobs may never get reaped. */
static int reschedule (confuga *C, chirp_jobid_t id, const char *tag, int reason)
{
	static const char SQL[] =
		"BEGIN TRANSACTION;"
		"DELETE FROM ConfugaOutputFile"
		"	WHERE jid = ?;"
		"DELETE FROM ConfugaJobWaitResult"
		"	WHERE id = ?;"
		"UPDATE ConfugaJob"
		"	SET"
		"		cid = NULL,"
		"		sid = NULL,"
		"		pull_bytes = NULL,"
		"		pull_count = NULL,"
		"		repl_bytes = NULL,"
		"		repl_count = NULL,"
		"		state = 'BOUND_INPUTS',"
				/* time_bound_inputs =  keep old value */
		"		time_scheduled = NULL,"
		"		time_replicated = NULL,"
		"		time_created = NULL,"
		"		time_committed = NULL,"
		"		time_waited = NULL,"
		"		time_reaped = NULL,"
		"		time_bound_outputs = NULL,"
		"		time_killed = NULL"
		"	WHERE id = ?;"
		"END TRANSACTION;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	jdebug(D_DEBUG, id, tag, "attempting to reschedule due to `%s'", strerror(reason));

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
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
	return rc;
}

static void CATCHJOB (confuga *C, chirp_jobid_t id, const char *tag, int rc)
{
	switch (rc) {
		case 0: /* no error */
			break;
		/* Transient errors. */
#ifdef ECONNRESET
		case ECONNRESET:
#endif
#ifdef ETIMEDOUT
		case ETIMEDOUT:
#endif
		case EAGAIN: /* This is either a SQL database lock error or the Chirp server is busy (also with a locked Job database). */
		case EINTR: /* Temporary error. */
			break;
		/* Permanent connection failures. */
#ifdef EADDRNOTAVAIL
		case EADDRNOTAVAIL:
#endif
#ifdef ECONNABORTED
		case ECONNABORTED:
#endif
#ifdef ECONNREFUSED
		case ECONNREFUSED:
#endif
#ifdef EHOSTUNREACH
		case EHOSTUNREACH:
#endif
#ifdef ENETDOWN
		case ENETDOWN:
#endif
#ifdef ENETRESET
		case ENETRESET:
#endif
#ifdef ENETUNREACH
		case ENETUNREACH:
#endif
		case ESRCH: /* somehow the job was lost on the remote Chirp server, probably Job DB got wiped? */
		case EIO: /* Internal Confuga error. */
			reschedule(C, id, tag, rc);
			break;
		default:
			fail(C, id, tag, strerror(rc));
			break;
	}
}

static int job_new (confuga *C)
{
	static const char SQL[] =
		"INSERT INTO ConfugaJob (id, state, tag, time_new)"
		"	SELECT Job.id, 'NEW', Job.tag, (strftime('%s', 'now'))"
		"		FROM Job LEFT OUTER JOIN ConfugaJob ON Job.id = ConfugaJob.id"
		"		WHERE ConfugaJob.id IS NULL;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	C->operations += sqlite3_changes(db);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int bindinput (confuga *C, chirp_jobid_t id, const char *tag, const char *serv_path, const char *task_path)
{
	static const char SQL[] =
		"INSERT INTO ConfugaInputFile (fid, jid, task_path) VALUES (?, ?, ?);";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	confuga_fid_t fid;

	jdebug(D_DEBUG, id, tag, "binding input `%s'=`%s'", serv_path, task_path);

	rc = confuga_lookup(C, serv_path, &fid, NULL);
	if (rc == 0) {
		sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
		sqlcatch(sqlite3_bind_blob(stmt, 1, confugaF_id(fid), confugaF_size(fid), SQLITE_STATIC));
		sqlcatch(sqlite3_bind_int64(stmt, 2, id));
		sqlcatch(sqlite3_bind_text(stmt, 3, task_path, -1, SQLITE_STATIC));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	} else if (rc == EISDIR) {
		struct confuga_dir *dir;
		CATCH(confuga_opendir(C, serv_path, &dir));
		while (1) {
			struct confuga_dirent *dirent;
			char serv_subpath[CONFUGA_PATH_MAX];
			char task_subpath[CONFUGA_PATH_MAX];
			CATCH(confuga_readdir(dir, &dirent));
			if (dirent == NULL)
				break;
			if (strcmp(dirent->name, ".") == 0 || strcmp(dirent->name, "..") == 0 || strncmp(dirent->name, ".__", 3) == 0)
				continue;
			snprintf(serv_subpath, sizeof(serv_subpath), "%s/%s", serv_path, dirent->name);
			snprintf(task_subpath, sizeof(task_subpath), "%s/%s", task_path, dirent->name);
			CATCH(bindinput(C, id, tag, serv_subpath, task_subpath));
		}
	} else {
		CATCH(rc);
	}

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int bindinputs (confuga *C, chirp_jobid_t id, const char *tag)
{
	static const char SQL[] =
		"BEGIN TRANSACTION;"
		"SELECT serv_path, task_path"
		"	FROM JobFile"
		"	WHERE id = ? AND type = 'INPUT';"
		"UPDATE ConfugaJob"
		"	SET"
		"		state = 'BOUND_INPUTS',"
		"		time_bound_inputs = (strftime('%s', 'now'))"
		"	WHERE id = ?;"
		"END TRANSACTION;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	/* FIXME input file mode may need executable bit */
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		CATCH(bindinput(C, id, tag, (const char *)sqlite3_column_text(stmt, 0), (const char *)sqlite3_column_text(stmt, 1)));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
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
	return rc;
}

static int job_bind_inputs (confuga *C)
{
	static const char SQL[] =
		"SELECT id, tag"
		"	FROM ConfugaJob"
		"	WHERE state = 'NEW'"
		"	ORDER BY RANDOM();"; /* to ensure no starvation, bindinputs may result in a ROLLBACK that aborts this SELECT */

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		chirp_jobid_t id = sqlite3_column_int64(stmt, 0);
		const char *tag = (const char *)sqlite3_column_text(stmt, 1);
		jdebug(D_DEBUG, id, tag, "binding inputs");
		CATCHJOB(C, id, tag, bindinputs(C, id, tag));
		C->operations++;
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int dispatch (confuga *C, chirp_jobid_t id, const char *tag)
{
	/* TODO Scheduling a job isn't simply acquiring a SN resource X, you also
	 * must acquire the transfer slots of other SN that will transfer files to
	 * X. What makes this particularly hard and interesting is there are two
	 * phases, (a) acquire transfer slots (which may be in incremental steps if
	 * the same source SN is sending multiple files!) and (b) run the job.
	 */
	static const char SQL[] =
		/* Find an available Storage Node with the most needed bytes. */
		"BEGIN TRANSACTION;"
		"WITH"
			/* We want every active SN, even if it has no input file. */
		"	StorageNodeAvailable AS ("
		"		SELECT StorageNodeActive.id"
		"			FROM Confuga.StorageNodeActive LEFT OUTER JOIN ConfugaJobAllocated ON StorageNodeActive.id = ConfugaJobAllocated.sid"
		"			GROUP BY StorageNodeActive.id"
		"			HAVING COUNT(ConfugaJobAllocated.id) < 1" /* TODO: allow more than one job on a SN */
		"	),"
		"	ConfugaInputFileReplicas AS ("
		"		SELECT ConfugaInputFile.jid, FileReplicas.*"
		"			FROM ConfugaInputFile JOIN Confuga.FileReplicas ON ConfugaInputFile.fid = FileReplicas.fid"
		"	),"
		"	StorageNodeJobBytes AS ("
		"		SELECT ConfugaJob.id AS jid, StorageNodeAvailable.id AS sid, COUNT(ConfugaInputFileReplicas.size) AS count, SUM(ConfugaInputFileReplicas.size) AS size, RANDOM() AS _r"
		"			FROM"
		"				ConfugaJob CROSS JOIN StorageNodeAvailable"
		"				LEFT OUTER JOIN ConfugaInputFileReplicas ON ConfugaJob.id = ConfugaInputFileReplicas.jid AND StorageNodeAvailable.id = ConfugaInputFileReplicas.sid"
		"			GROUP BY ConfugaJob.id, StorageNodeAvailable.id"
		"	)"
		/* N.B. if there are no available storage nodes, sid will be NULL! */
		"SELECT StorageNodeJobBytes.sid, StorageNodeJobBytes.count, StorageNodeJobBytes.size"
		"	FROM StorageNodeJobBytes"
		"	WHERE StorageNodeJobBytes.jid = ?1"
		"	ORDER BY StorageNodeJobBytes.size DESC, _r DESC" /* choose a random storage node if equally desirable */
		"	LIMIT 1;"
		"UPDATE ConfugaJob"
		"	SET"
		"		sid = ?2,"
		"		state = 'SCHEDULED',"
		"		repl_bytes = ?3,"
		"		repl_count = ?4,"
		"		time_scheduled = (strftime('%s', 'now'))"
		"	WHERE id = ?1;"
		"UPDATE Job"
		"	SET status = 'STARTED', time_start = strftime('%s', 'now')"
		"	WHERE id = ?;"
		"END TRANSACTION;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	confuga_sid_t sid=-1;
	struct job_stats stats;
	memset(&stats, 0, sizeof(stats));

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	rc = sqlite3_step(stmt);
	if (rc == SQLITE_ROW) {
		if (sqlite3_column_type(stmt, 0) == SQLITE_INTEGER) {
			sid = sqlite3_column_int64(stmt, 0);
			stats.repl_count = sqlite3_column_int64(stmt, 1);
			stats.repl_bytes = sqlite3_column_int64(stmt, 2);
			assert(sid > 0);
			jdebug(D_CONFUGA, id, tag, "scheduling on " CONFUGA_SID_DEBFMT, sid);
			C->operations++;
		} else {
			assert(sqlite3_column_type(stmt, 0) == SQLITE_NULL);
			jdebug(D_DEBUG, id, tag, "could not schedule yet");
			THROW_QUIET(EAGAIN); /* come back later */
		}
	} else if (rc == SQLITE_DONE) {
		jdebug(D_DEBUG, id, tag, "could not schedule yet");
		THROW_QUIET(EAGAIN); /* come back later */
	} else {
		C->operations++;
		sqlcatch(rc);
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	sqlcatch(sqlite3_bind_int64(stmt, 2, sid));
	sqlcatch(sqlite3_bind_int64(stmt, 3, stats.repl_bytes));
	sqlcatch(sqlite3_bind_int64(stmt, 4, stats.repl_count));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
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
	return rc;
}

static int job_schedule (confuga *C)
{
	static const char SQL[] =
		"WITH"
		"	ScheduledJob AS ("
		"		SELECT id"
		"			FROM ConfugaJob"
		"			WHERE ConfugaJob.state = 'SCHEDULED'"
		"	)"
		"SELECT ConfugaJob.id, ConfugaJob.tag"
		"	FROM Job INNER JOIN ConfugaJob ON Job.id = ConfugaJob.id"
		"	WHERE ConfugaJob.state = 'BOUND_INPUTS'"
		"	ORDER BY Job.priority, Job.time_commit"
		"	LIMIT (CASE WHEN ?1 == 0 OR (SELECT COUNT(*) FROM ScheduledJob) < ?1 THEN 1 ELSE 0 END);";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	assert(C->scheduler == CONFUGA_SCHEDULER_FIFO);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, C->scheduler_n));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		chirp_jobid_t id = sqlite3_column_int64(stmt, 0);
		const char *tag = (const char *)sqlite3_column_text(stmt, 1);
		CATCHJOB(C, id, tag, dispatch(C, id, tag));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int replicate_push_synchronous (confuga *C)
{
	static const char SQL[] =
		"SELECT ConfugaJob.id, ConfugaJob.tag, ConfugaJob.sid, ConfugaInputFile.fid"
		"	FROM"
		"		ConfugaJob"
		"		JOIN ConfugaInputFile ON ConfugaJob.id = ConfugaInputFile.jid"
		"		JOIN Confuga.File ON ConfugaInputFile.fid = File.id"
		"		LEFT OUTER JOIN Confuga.Replica ON ConfugaInputFile.fid = Replica.fid AND ConfugaJob.sid = Replica.sid"
		"	WHERE"
		"		ConfugaJob.state = 'SCHEDULED' AND File.size >= ?1 AND Replica.fid IS NULL AND Replica.sid IS NULL"
		"	ORDER BY time_scheduled ASC, File.size DESC"
		";"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	time_t start = time(0);
	char *tag = NULL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, C->pull_threshold));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		chirp_jobid_t id = sqlite3_column_int64(stmt, 0);
		tag = (free(tag), NULL);
		tag = strdup((const char *)sqlite3_column_text(stmt, 1)); /* save copy before reset */
		CATCHUNIX(tag == NULL ? -1 : 0);
		confuga_sid_t sid = sqlite3_column_int64(stmt, 2);
		confuga_fid_t fid;
		assert(sqlite3_column_type(stmt, 3) == SQLITE_BLOB && (size_t)sqlite3_column_bytes(stmt, 3) == confugaF_size(fid));
		CATCH(confugaF_set(C, &fid, sqlite3_column_blob(stmt, 3)));

		/* now release locks */
		sqlcatch(sqlite3_reset(stmt));

		jdebug(D_DEBUG, id, tag, "synchronously replicating file " CONFUGA_FID_DEBFMT, CONFUGA_FID_PRIARGS(fid));
		CATCH(confugaR_replicate(C, fid, sid, tag, STOPTIME));
		C->operations++;
		if (start+60 <= time(0)) {
			jdebug(D_DEBUG, id, tag, "exceeded one minute of replication, coming back later to finish");
			break;
		}
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	free(tag);
	return rc;
}

/* FIXME check for stagnant jobs */
static int replicate_push_asynchronous (confuga *C)
{
	static const char SQL[] =
		/* TODO: Unfortunately, there seems to be a bug in SQLite [1] which
		 * will always do a commit (resulting in a write) on this usually NO-OP
		 * INSERT. The workaround is to check for rows in the select before
		 * doing the insert.  The SQLite developers have so far refused to
		 * acknowledge this bug :(.
		 *
		 * [1] https://www.mail-archive.com/sqlite-users@mailinglists.sqlite.org/msg05276.html
		 */
		"CREATE TEMPORARY TABLE IF NOT EXISTS TransferScheduleParameters____replicate_push_asynchronous ("
		"	key TEXT PRIMARY KEY,"
		"	value INTEGER"
		");"
		"INSERT OR REPLACE INTO TransferScheduleParameters____replicate_push_asynchronous"
		"	VALUES ('pull-threshold', ?1), ('transfer-slots', ?2);"
		"CREATE TEMPORARY VIEW IF NOT EXISTS TransferSchedule__replicate_push_asynchronous AS"
		"	WITH"
		"		PullThreshold AS ("
		"			SELECT value FROM TransferScheduleParameters____replicate_push_asynchronous WHERE key = 'pull-threshold'"
		"		),"
		"		TransferSlots AS ("
		"			SELECT value FROM TransferScheduleParameters____replicate_push_asynchronous WHERE key = 'transfer-slots'"
		"		),"
				/* Storage Node with available Transfer Slots. */
		"		StorageNodeTransferReady AS ("
		"			SELECT id"
		"				FROM"
		"					("
		"						SELECT StorageNodeActive.id AS id, ActiveTransfers.id AS tjid"
		"							FROM (Confuga.StorageNodeActive LEFT OUTER JOIN Confuga.ActiveTransfers ON StorageNodeActive.id = ActiveTransfers.tsid)"
		"					UNION ALL"
		"						SELECT StorageNodeActive.id AS id, ActiveTransfers.id AS tjid"
		"							FROM (Confuga.StorageNodeActive LEFT OUTER JOIN Confuga.ActiveTransfers ON StorageNodeActive.id = ActiveTransfers.fsid)"
		"					)"
		"				GROUP BY id"
		"				HAVING ((SELECT * FROM TransferSlots) == 0 OR COUNT(tjid) < (SELECT * FROM TransferSlots))"
		"		),"
				/* This is a StorageNode we are able to use to transfer a replica. */
		"		SourceStorageNode AS ("
		"			SELECT FileReplicas.fid, StorageNodeTransferReady.id as sid"
		"				FROM"
		"					StorageNodeTransferReady"
		"					JOIN Confuga.FileReplicas ON StorageNodeTransferReady.id = FileReplicas.sid"
		"		),"
				/* For each fid, pick a random SourceStorageNode. */
		"		RandomSourceStorageNode AS ("
		"			SELECT *, MIN(_r)"
		"				FROM"
		"					(SELECT *, RANDOM() AS _r FROM SourceStorageNode)"
		"				GROUP BY fid"
		"		),"
				/* This contains all the Replica of a File AND ongoing transfers of the File to some StorageNode */
		"		PotentialReplicas AS ("
		"				SELECT fid, sid FROM Confuga.FileReplicas"
		"			UNION ALL"
		"				SELECT File.id AS fid, ActiveTransfers.tsid AS sid"
		"					FROM Confuga.File JOIN Confuga.ActiveTransfers ON File.id = ActiveTransfers.fid"
		"		),"
				/* Files needed by the scheduled job which are not in PotentialReplicas. Ignore files with size under the pull threshold. */
		"		MissingDependencies AS ("
		"			SELECT ConfugaJob.id, File.id AS fid, File.size"
		"				FROM"
		"					ConfugaJob"
		"					JOIN ConfugaInputFile ON ConfugaJob.id = ConfugaInputFile.jid"
		"					JOIN Confuga.File ON ConfugaInputFile.fid = File.id"
		"					LEFT OUTER JOIN PotentialReplicas ON ConfugaInputFile.fid = PotentialReplicas.fid AND ConfugaJob.sid = PotentialReplicas.sid"
		"				WHERE File.size >= (SELECT * FROM PullThreshold) AND PotentialReplicas.fid IS NULL AND PotentialReplicas.sid IS NULL"
		"		),"
				/* Largest ready push transfer for each ConfugaJob. Large files first as they can delay the workflow. */
		"		LargestReadyPushTransfers AS ("
		"			SELECT ConfugaJob.id, MissingDependencies.fid, MAX(MissingDependencies.size), RandomSourceStorageNode.sid AS fsid, ConfugaJob.sid AS tsid"
		"				FROM"
		"					ConfugaJob"
		"					JOIN StorageNodeTransferReady ON ConfugaJob.sid = StorageNodeTransferReady.id"
		"					JOIN MissingDependencies ON ConfugaJob.id = MissingDependencies.id"
		"					JOIN RandomSourceStorageNode ON MissingDependencies.fid = RandomSourceStorageNode.fid"
		"			GROUP BY ConfugaJob.id"
		"		)"
		"	SELECT 'NEW', 'JOB', ConfugaJob.id, ConfugaJob.tag, LargestReadyPushTransfers.fid, LargestReadyPushTransfers.fsid, LargestReadyPushTransfers.tsid"
		"		FROM"
		"			ConfugaJob"
		"			JOIN LargestReadyPushTransfers ON ConfugaJob.id = LargestReadyPushTransfers.id"
		"		WHERE ConfugaJob.state = 'SCHEDULED'"
		"		ORDER BY RANDOM()"
				/* TODO it would be great to break file size ties with replica count. */
		/*"		ORDER BY FLOOR(LOG(LargestReadyPushTransfers.size+1)) DESC, ConfugaJob.time_new ASC"*/
		"		LIMIT 1;"
		"SELECT COUNT(*) FROM TransferSchedule__replicate_push_asynchronous;"
		"BEGIN IMMEDIATE TRANSACTION;"
		"INSERT INTO Confuga.TransferJob (state, source, source_id, tag, fid, fsid, tsid)"
		"	SELECT * FROM TransferSchedule__replicate_push_asynchronous;" /* Note: always only 0 or 1 rows inserted! */
		"SELECT id, source_id, tag, fid, fsid, tsid"
		"	FROM Confuga.TransferJob"
		"	WHERE id = LAST_INSERT_ROWID();"
		"END TRANSACTION;"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	sqlite3_stmt *insert = NULL;
	sqlite3_stmt *select = NULL;
	const char *current = SQL;
	uint64_t count;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, C->pull_threshold));
	sqlcatch(sqlite3_bind_int64(stmt, 2, C->replication_n));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
	if (sqlite3_column_int(stmt, 0) == 0) {
		rc = 0;
		goto out;
	}
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &insert, &current));
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &select, &current));

	/* don't schedule more than 100 transfer jobs per cycle */
	for (count = 0; count < 100; count++) {
		sqlite3_int64 changes;

		sqlcatch(sqlite3_reset(insert));
		sqlcatchcode(sqlite3_step(insert), SQLITE_DONE);
		changes = sqlite3_changes(db);

		if (changes == 1) {
			chirp_jobid_t tjid, jid;
			const char *tag;
			confuga_sid_t fsid, tsid;
			confuga_fid_t fid;

			sqlcatch(sqlite3_reset(select));
			sqlcatchcode(sqlite3_step(select), SQLITE_ROW);
			tjid = sqlite3_column_int64(select, 0);
			jid = sqlite3_column_int64(select, 1);
			tag = (const char *)sqlite3_column_text(select, 2);
			assert(sqlite3_column_type(select, 3) == SQLITE_BLOB && (size_t)sqlite3_column_bytes(select, 3) == confugaF_size(fid));
			CATCH(confugaF_set(C, &fid, sqlite3_column_blob(select, 3)));
			fsid = sqlite3_column_int64(select, 4);
			tsid = sqlite3_column_int64(select, 5);
			jdebug(D_DEBUG, jid, tag, "scheduled transfer job %" PRId64 " (" CONFUGA_FID_DEBFMT ": " CONFUGA_SID_DEBFMT " -> " CONFUGA_SID_DEBFMT ")", tjid, CONFUGA_FID_PRIARGS(fid), fsid, tsid);
			C->operations++;
		} else if (changes == 0) {
			break;
		} else assert(0);
	}

	sqlcatch(sqlite3_finalize(insert); insert = NULL);
	sqlcatch(sqlite3_finalize(select); select = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlite3_finalize(insert);
	sqlite3_finalize(select);
	sqlend(db);
	return rc;
}

static int set_replicated (confuga *C, chirp_jobid_t id)
{
	static const char SQL[] =
			"UPDATE ConfugaJob"
			"	SET"
			"		state = 'REPLICATED',"
			"		time_replicated = (strftime('%s', 'now'))"
			"	WHERE id = ?1;"
			;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

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

static int job_replicate (confuga *C)
{
	static const char SQL[] =
		"SELECT ConfugaJob.id, ConfugaJob.tag"
		"	FROM ConfugaJob"
		"	WHERE state = 'SCHEDULED' AND NOT EXISTS ("
		"		SELECT ConfugaJob.id"
		"			FROM"
		"				ConfugaInputFile"
		"				JOIN Confuga.File ON ConfugaInputFile.fid = File.id"
		"				LEFT OUTER JOIN Confuga.Replica ON ConfugaInputFile.fid = Replica.fid AND ConfugaJob.sid = Replica.sid"
		"			WHERE ConfugaInputFile.jid = ConfugaJob.id AND File.size >= ?1 AND Replica.fid IS NULL AND Replica.sid IS NULL"
		"	)"
		";"
		"SELECT ConfugaJob.id, ConfugaJob.tag"
		"	FROM"
		"		ConfugaJob"
		"		LEFT OUTER JOIN Confuga.StorageNodeActive ON ConfugaJob.sid = StorageNodeActive.id"
		"	WHERE ConfugaJob.state = 'SCHEDULED' AND StorageNodeActive.id IS NULL"
		";"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	/* check for jobs with all dependencies replicated */
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, C->pull_threshold));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		chirp_jobid_t id = sqlite3_column_int64(stmt, 0);
		const char *tag = (const char *)sqlite3_column_text(stmt, 1);
		jdebug(D_DEBUG, id, tag, "all dependencies are replicated");
		CATCHJOB(C, id, tag, set_replicated(C, id));
		C->operations++;
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	/* check for jobs scheduled on inactive storage nodes */
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		chirp_jobid_t id = sqlite3_column_int64(stmt, 0);
		const char *tag = (const char *)sqlite3_column_text(stmt, 1);
		jdebug(D_DEBUG, id, tag, "storage node lost");
		reschedule(C, id, tag, ESRCH); /* someone else killed it? reschedule */
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	/* now replicate missing dependencies */
	if (C->replication == CONFUGA_REPLICATION_PUSH_ASYNCHRONOUS)
		CATCH(replicate_push_asynchronous(C));
	else if (C->replication == CONFUGA_REPLICATION_PUSH_SYNCHRONOUS)
		CATCH(replicate_push_synchronous(C));
	else assert(0);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int encode (confuga *C, chirp_jobid_t id, const char *tag, buffer_t *B, struct job_stats *stats)
{
	static const char SQL[] =
		"SELECT executable, State.value"
		"	FROM Job JOIN Confuga.State"
		"	WHERE Job.id = ? AND State.key = 'id'"
		";"
		"SELECT arg FROM JobArgument WHERE id = ? ORDER BY n"
		";"
		"	SELECT 'CHIRP_CLIENT_TICKETS', './.confuga.ticket'"
		"UNION ALL"
		"	SELECT name, value FROM JobEnvironment WHERE id = ?"
		";"
		"WITH"
			/* SourceReplicaRandom gives a list replica sources (for pull transfers)... */
		"	SourceReplicaRandom AS ("
			/* URL_TRUNCATE is defined in confuga.c. */
		"		SELECT RandomReplica.fid, URL_TRUNCATE(GROUP_CONCAT(PRINTF('chirp://%s/%s/file/%s', StorageNodeActive.hostport, StorageNodeActive.root, HEX(RandomReplica.fid)), '\t')) AS urls, RandomReplica.size AS size"
		"			FROM"
						/* XXX Note that this ORDER BY clause only works with SQLite! */
		"				(SELECT FileReplicas.*, RANDOM() AS _r FROM Confuga.FileReplicas ORDER BY _r) AS RandomReplica"
		"				INNER JOIN Confuga.StorageNodeActive ON RandomReplica.sid = StorageNodeActive.id"
		"			GROUP BY RandomReplica.fid"
		"	)"
			/* Job ticket */
		"	SELECT 'LINK' AS binding, PRINTF('%s/ticket', StorageNode.root) AS serv_path, './.confuga.ticket' AS task_path, NULL AS tag, 'INPUT' AS type, NULL AS size"
		"		FROM ConfugaJob INNER JOIN Confuga.StorageNode ON ConfugaJob.sid = StorageNode.id"
		"		WHERE ConfugaJob.id = ?1"
		" UNION ALL"
			/* Replicated files, pull is done later... */
		"	SELECT 'LINK' AS binding, PRINTF('%s/file/%s', StorageNode.root, HEX(ConfugaInputFile.fid)) AS serv_path, task_path, NULL AS tag, 'INPUT' AS type, FileReplicas.size AS size"
		"		FROM"
		"			ConfugaInputFile"
		"			INNER JOIN ConfugaJob ON ConfugaInputFile.jid = ConfugaJob.id"
		"			INNER JOIN Confuga.StorageNode ON ConfugaJob.sid = StorageNode.id"
		"			INNER JOIN Confuga.FileReplicas ON ConfugaInputFile.fid = FileReplicas.fid AND StorageNode.id = FileReplicas.sid"
		"		WHERE ConfugaInputFile.jid = ?1"
		" UNION ALL"
			/* Now include pull transfers... */
		"	SELECT 'URL' AS binding, SourceReplicaRandom.urls AS serv_path, ConfugaInputFile.task_path AS task_path, NULL AS tag, 'INPUT' AS type, SourceReplicaRandom.size"
		"		FROM"
		"			ConfugaInputFile"
		"			INNER JOIN ConfugaJob ON ConfugaInputFile.jid = ConfugaJob.id"
					/* Get available replicas, XXX SourceReplicaRandom.urls CAN BE NULL. */
		"			LEFT OUTER JOIN SourceReplicaRandom ON ConfugaInputFile.fid = SourceReplicaRandom.fid"
					/* Exclude ConfugaInputFile that are replicated... */
		"			LEFT OUTER JOIN Confuga.Replica AS NoReplica ON ConfugaInputFile.fid = NoReplica.fid AND ConfugaJob.sid = NoReplica.sid"
		"		WHERE ConfugaInputFile.jid = ?1 AND NoReplica.fid IS NULL AND NoReplica.sid IS NULL"
		" UNION ALL"
			/* Now outputs... */
		"	SELECT 'LINK' AS binding, PRINTF('%s/file/%%s', StorageNode.root) AS serv_path, JobFile.task_path AS task_path, '" CONFUGA_OUTPUT_TAG "' AS tag, 'OUTPUT' AS type, NULL AS size"
		"		FROM"
		"			JobFile"
		"			INNER JOIN ConfugaJob ON JobFile.id = ConfugaJob.id"
		"			INNER JOIN Confuga.StorageNode ON ConfugaJob.sid = StorageNode.id"
		"		WHERE JobFile.id = ?1 AND JobFile.type = 'OUTPUT'"
		" UNION ALL"
			/* Now cache the pull transfer as an output replica. */
		"	SELECT 'LINK' AS binding, PRINTF('%s/file/%%s', StorageNode.root) AS serv_path, ConfugaInputFile.task_path AS task_path, '" CONFUGA_PULL_TAG "' AS tag, 'OUTPUT' AS type, NULL AS size"
		"		FROM"
		"			ConfugaInputFile"
		"			INNER JOIN ConfugaJob ON ConfugaInputFile.jid = ConfugaJob.id"
		"			INNER JOIN Confuga.StorageNode ON ConfugaJob.sid = StorageNode.id"
					/* Exclude ConfugaInputFile that are replicated... */
		"			LEFT OUTER JOIN Confuga.Replica AS NoReplica ON ConfugaInputFile.fid = NoReplica.fid AND ConfugaJob.sid = NoReplica.sid"
		"		WHERE ConfugaInputFile.jid = ?1 AND NoReplica.fid IS NULL AND NoReplica.sid IS NULL"
/* debugging for pull transfers */
#if 1
		" UNION ALL"
		"	SELECT 'LINK' AS binding, StorageNode.root || '/debug.%j' AS serv_path, '.chirp.debug' AS task_path, NULL AS tag, 'OUTPUT' AS type, NULL AS size"
		"		FROM ConfugaJob INNER JOIN Confuga.StorageNode ON ConfugaJob.sid = StorageNode.id"
		"		WHERE ConfugaJob.id = ?1;"
#endif
		";";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	int first0;

	CATCHUNIX(buffer_putliteral(B, "{"));

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
	CATCHUNIX(buffer_putliteral(B, "\"executable\":"));
	CATCH(chirp_sqlite3_column_jsonify(stmt, 0, B));
	CATCHUNIX(buffer_putliteral(B, ",\"tag\":"));
	CATCH(chirp_sqlite3_column_jsonify(stmt, 1, B));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	first0 = 1;
	CATCHUNIX(buffer_putliteral(B, ",\"arguments\":["));
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		if (!first0)
			CATCHUNIX(buffer_putliteral(B, ","));
		first0 = 0;
		CATCH(chirp_sqlite3_column_jsonify(stmt, 0, B));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	CATCHUNIX(buffer_putliteral(B, "]"));

	first0 = 1;
	CATCHUNIX(buffer_putliteral(B, ",\"environment\":{"));
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		if (!first0)
			CATCHUNIX(buffer_putliteral(B, ","));
		first0 = 0;
		CATCH(chirp_sqlite3_column_jsonify(stmt, 0, B));
		CATCHUNIX(buffer_putliteral(B, ":"));
		CATCH(chirp_sqlite3_column_jsonify(stmt, 1, B));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	CATCHUNIX(buffer_putliteral(B, "}"));

	first0 = 1;
	CATCHUNIX(buffer_putliteral(B, ",\"files\":["));
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		if (!first0)
			CATCHUNIX(buffer_putliteral(B, ","));
		first0 = 0;
		assert(streql(sqlite3_column_name(stmt, 0), "binding"));
		assert(streql(sqlite3_column_name(stmt, 1), "serv_path"));
		assert(streql(sqlite3_column_name(stmt, 4), "type"));
		assert(streql(sqlite3_column_name(stmt, 5), "size"));
		if (streql((const char *)sqlite3_column_text(stmt, 4), "INPUT") && streql((const char *)sqlite3_column_text(stmt, 0), "URL")) {
			stats->pull_bytes += sqlite3_column_int64(stmt, 5); /* size */
			stats->pull_count += 1;
		}
		if (sqlite3_column_type(stmt, 1) == SQLITE_NULL) {
			/* XXX If this is a pull, then there is no available storage node. Reschedule... */
			CATCH(EIO);
		}
		CATCH(chirp_sqlite3_row_jsonify(stmt, B));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	CATCHUNIX(buffer_putliteral(B, "]"));

	CATCHUNIX(buffer_putliteral(B, "}"));

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int jcreate (confuga *C, chirp_jobid_t id, const char *tag, const char *hostport)
{
	static const char SQL[] =
		"BEGIN TRANSACTION;"
		/* call to encode(...) */
		"UPDATE ConfugaJob"
		"	SET"
		"		state = 'CREATED',"
		"		cid = ?2,"
		"		pull_bytes = ?3,"
		"		pull_count = ?4,"
		"		time_created = (strftime('%s', 'now'))"
		"	WHERE id = ?1;"
		"END TRANSACTION;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	buffer_t B[1];
	chirp_jobid_t cid;
	struct job_stats stats;
	memset(&stats, 0, sizeof(stats));

	buffer_init(B);

	jdebug(D_DEBUG, id, tag, "creating job on storage node");

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	CATCH(encode(C, id, tag, B, &stats));
	debug(D_DEBUG, "json = `%s'", buffer_tostring(B));

	CATCHUNIX(chirp_reli_job_create(hostport, buffer_tostring(B), &cid, STOPTIME));

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	sqlcatch(sqlite3_bind_int64(stmt, 2, cid));
	sqlcatch(sqlite3_bind_int64(stmt, 3, stats.pull_bytes));
	sqlcatch(sqlite3_bind_int64(stmt, 4, stats.pull_count));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	buffer_free(B);
	sqlite3_finalize(stmt);
	sqlend(db);
	return rc;
}

static int job_create (confuga *C)
{
	static const char SQL[] =
		"SELECT ConfugaJob.id, ConfugaJob.tag, StorageNode.hostport"
		"	FROM ConfugaJob INNER JOIN Confuga.StorageNode ON ConfugaJob.sid = StorageNode.id"
		"	WHERE ConfugaJob.state = 'REPLICATED'"
		"	ORDER BY RANDOM()" /* to ensure no starvation, create may result in a ROLLBACK that aborts this SELECT */
		"	LIMIT (CASE WHEN ?1 == 0 THEN -1 ELSE MAX(0, (?1 - (SELECT COUNT(*) FROM ConfugaJobExecuting))) END);";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, C->concurrency));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		chirp_jobid_t id = sqlite3_column_int64(stmt, 0);
		const char *tag = (const char *)sqlite3_column_text(stmt, 1);
		const char *hostport = (const char *)sqlite3_column_text(stmt, 2);
		CATCHJOB(C, id, tag, jcreate(C, id, tag, hostport));
		C->operations++;
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int jcommit (confuga *C, chirp_jobid_t id, const char *tag, const char *hostport, chirp_jobid_t cid)
{
	static const char SQL[] =
		"UPDATE ConfugaJob"
		"	SET"
		"		state = 'COMMITTED',"
		"		time_committed = (strftime('%s', 'now'))"
		"	WHERE id = ?;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	BUFFER_STACK_PRINT(B, 64, "[%" PRICHIRP_JOBID_T "]", cid);

	jdebug(D_DEBUG, id, tag, "committing job on storage node");

	CATCHUNIX(chirp_reli_job_commit(hostport, buffer_tostring(B), STOPTIME));

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

static int job_commit (confuga *C)
{
	static const char SQL[] =
		"SELECT ConfugaJob.id, ConfugaJob.tag, StorageNode.hostport, ConfugaJob.cid"
		"	FROM ConfugaJob INNER JOIN Confuga.StorageNode ON ConfugaJob.sid = StorageNode.id"
		"	WHERE ConfugaJob.state = 'CREATED';";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		chirp_jobid_t id = sqlite3_column_int64(stmt, 0);
		const char *tag = (const char *)sqlite3_column_text(stmt, 1);
		const char *hostport = (const char *)sqlite3_column_text(stmt, 2);
		chirp_jobid_t cid = sqlite3_column_int64(stmt, 3);
		CATCHJOB(C, id, tag, jcommit(C, id, tag, hostport, cid));
		C->operations++;
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int jwait (confuga *C, chirp_jobid_t id, const char *tag, confuga_sid_t sid, const char *hostport, chirp_jobid_t cid)
{
	static const char SQL[] =
		"BEGIN TRANSACTION;"
		"INSERT INTO ConfugaOutputFile (jid, task_path, fid, size) VALUES (?, ?, ?, ?);"
		"INSERT OR REPLACE INTO ConfugaJobWaitResult (id, error, exit_code, exit_signal, exit_status, status)"
		"	VALUES (?, ?, ?, ?, ?, ?);"
		"UPDATE ConfugaJob"
		"	SET"
		"		state = 'WAITED',"
		"		time_waited = (strftime('%s', 'now'))"
		"	WHERE id = ?;"
		"END TRANSACTION;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	char *status = NULL;
	unsigned i;
	json_value *J = NULL;

	jdebug(D_DEBUG, id, tag, "waiting for job");

	CATCHUNIX(chirp_reli_job_wait(hostport, cid, 0, &status, STOPTIME));
	assert(status);
	assert(strlen(status) == (size_t)rc);
	debug(D_DEBUG, "status = `%s'", status);

	J = json_parse(status, strlen(status));
	if (J == NULL)
		CATCH(EINVAL);

	assert(jistype(J, json_array));
	for (i = 0; i < J->u.array.length; i++) {
		json_value *job = J->u.array.values[i];
		assert(jistype(job, json_object));
		if (jsonA_getname(job, "id", json_integer)->u.integer == cid) {
			jdebug(D_CONFUGA, id, tag, "storage node job %" PRICHIRP_JOBID_T " finished", cid);
			C->operations++;

			sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
			sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
			sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

			/* UPDATE ConfugaOutputFile */
			json_value *error = jsonA_getname(job, "error", json_string);
			json_value *exit_code = jsonA_getname(job, "exit_code", json_integer);
			json_value *exit_signal = jsonA_getname(job, "exit_signal", json_string);
			json_value *exit_status = jsonA_getname(job, "exit_status", json_string);
			json_value *status = jsonA_getname(job, "status", json_string);

			sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
			if (status && strcmp(status->u.string.ptr, "FINISHED") == 0 && exit_status && strcmp(exit_status->u.string.ptr, "EXITED") == 0) {
				json_value *files = jsonA_getname(job, "files", json_array);
				if (files) {
					unsigned j;

					for (j = 0; j < files->u.array.length; j++) {
						json_value *file = files->u.array.values[j];
						if (jistype(file, json_object)) {
							json_value *task_path = jsonA_getname(file, "task_path", json_string);
							json_value *serv_path = jsonA_getname(file, "serv_path", json_string);
							json_value *type = jsonA_getname(file, "type", json_string);
							if (task_path && serv_path && type) {
								json_value *size = jsonA_getname(file, "size", json_integer);
								json_value *file_tag = jsonA_getname(file, "tag", json_string);
								if (strcmp(type->u.string.ptr, "OUTPUT") == 0 && size && file_tag && (streql(file_tag->u.string.ptr, CONFUGA_OUTPUT_TAG) || streql(file_tag->u.string.ptr, CONFUGA_PULL_TAG))) {
									confuga_fid_t fid;
									const char *sp;

									sp = strrchr(serv_path->u.string.ptr, '/');
									if (sp == NULL)
										CATCH(EINVAL);
									sp += 1;
									CATCH(confugaF_extract(C, &fid, sp, NULL));

									CATCH(confugaR_register(C, fid, size->u.integer, sid));
									if (streql(file_tag->u.string.ptr, CONFUGA_OUTPUT_TAG)) {
										jdebug(D_DEBUG, id, tag, "setting output fid = " CONFUGA_FID_PRIFMT " size = %" PRICONFUGA_OFF_T " task_path = `%s'", CONFUGA_FID_PRIARGS(fid), size, task_path->u.string.ptr);
										sqlcatch(sqlite3_reset(stmt));
										sqlcatch(sqlite3_bind_int64(stmt, 1, id));
										sqlcatch(sqlite3_bind_text(stmt, 2, task_path->u.string.ptr, -1, SQLITE_STATIC));
										sqlcatch(sqlite3_bind_blob(stmt, 3, confugaF_id(fid), confugaF_size(fid), SQLITE_STATIC));
										sqlcatch(sqlite3_bind_int64(stmt, 4, size->u.integer));
										sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
									}
								}
							} else {
								CATCH(EINVAL);
							}
						} else {
							CATCH(EINVAL);
						}
					}
				}
			} else if (status && strcmp(status->u.string.ptr, "FINISHED") == 0 && exit_status && strcmp(exit_status->u.string.ptr, "SIGNALED") == 0 && exit_signal && strcmp(exit_signal->u.string.ptr, "SIGUSR1") == 0) {
				/* This indicates the job failed startup, probably could not source a URL. We should retry the job! */
				CATCH(EIO);
			}
			sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

			sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
			sqlcatch(sqlite3_bind_int64(stmt, 1, id));
			if (error)
				sqlcatch(sqlite3_bind_text(stmt, 2, error->u.string.ptr, -1, SQLITE_STATIC));
			if (exit_code)
				sqlcatch(sqlite3_bind_int64(stmt, 3, exit_code->u.integer));
			if (exit_signal)
				sqlcatch(sqlite3_bind_text(stmt, 4, exit_signal->u.string.ptr, -1, SQLITE_STATIC));
			if (exit_status)
				sqlcatch(sqlite3_bind_text(stmt, 5, exit_status->u.string.ptr, -1, SQLITE_STATIC));
			if (status)
				sqlcatch(sqlite3_bind_text(stmt, 6, status->u.string.ptr, -1, SQLITE_STATIC));
			sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
			sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

			sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
			sqlcatch(sqlite3_bind_int64(stmt, 1, id));
			sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
			sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

			sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
			sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
			sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
		}
	}

	rc = 0;
	goto out;
out:
	free(status);
	json_value_free(J);
	sqlite3_finalize(stmt);
	sqlend(db);
	return rc;
}

static int job_wait (confuga *C)
{
	static const char SQL[] =
		"SELECT ConfugaJob.id, ConfugaJob.tag, StorageNode.id, StorageNode.hostport, ConfugaJob.cid"
		"	FROM ConfugaJob INNER JOIN Confuga.StorageNode ON ConfugaJob.sid = StorageNode.id"
		"	WHERE ConfugaJob.state = 'COMMITTED'"
		"	ORDER BY RANDOM();"; /* to ensure no starvation, wait may result in a ROLLBACK that aborts this SELECT */

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		chirp_jobid_t id = sqlite3_column_int64(stmt, 0);
		const char *tag = (const char *)sqlite3_column_text(stmt, 1);
		confuga_sid_t sid = sqlite3_column_int64(stmt, 2);
		const char *hostport = (const char *)sqlite3_column_text(stmt, 3);
		chirp_jobid_t cid = sqlite3_column_int64(stmt, 4);
		CATCHJOB(C, id, tag, jwait(C, id, tag, sid, hostport, cid));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int jreap (confuga *C, chirp_jobid_t id, const char *tag, const char *hostport, chirp_jobid_t cid)
{
	static const char SQL[] =
		"UPDATE ConfugaJob"
		"	SET"
		"		state = 'REAPED',"
		"		time_reaped = (strftime('%s', 'now'))"
		"	WHERE id = ?;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	BUFFER_STACK_PRINT(B, 64, "[%" PRICHIRP_JOBID_T "]", cid);

	jdebug(D_DEBUG, id, tag, "reaping job on storage node");

	CATCHUNIX(chirp_reli_job_reap(hostport, buffer_tostring(B), STOPTIME));

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

static int job_reap (confuga *C)
{
	static const char SQL[] =
		"SELECT ConfugaJob.id, ConfugaJob.tag, StorageNode.hostport, ConfugaJob.cid"
		"	FROM ConfugaJob INNER JOIN Confuga.StorageNode ON ConfugaJob.sid = StorageNode.id"
		"	WHERE ConfugaJob.state = 'WAITED';";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		chirp_jobid_t id = sqlite3_column_int64(stmt, 0);
		const char *tag = (const char *)sqlite3_column_text(stmt, 1);
		const char *hostport = (const char *)sqlite3_column_text(stmt, 2);
		chirp_jobid_t cid = sqlite3_column_int64(stmt, 3);
		CATCHJOB(C, id, tag, jreap(C, id, tag, hostport, cid));
		C->operations++;
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int bindoutputs (confuga *C, chirp_jobid_t id, const char *tag)
{
	static const char SQL[] =
		/* This is an EXLUSIVE transaction because we will update the NS. */
		"BEGIN EXCLUSIVE TRANSACTION;"
		/* Update NS */
		"SELECT JobFile.serv_path, ConfugaOutputFile.fid, ConfugaOutputFile.size"
		"	FROM"
		"		ConfugaOutputFile"
		"		INNER JOIN JobFile ON ConfugaOutputFile.jid = JobFile.id AND ConfugaOutputFile.task_path = JobFile.task_path AND JobFile.type = 'OUTPUT'"
		"	WHERE ConfugaOutputFile.jid = ?;"
		"UPDATE ConfugaJob"
		"	SET"
		"		state = 'BOUND_OUTPUTS',"
		"		time_bound_outputs = (strftime('%s', 'now'))"
		"	WHERE id = ?;"
		"UPDATE Job"
		"	SET"
		"		exit_code = (SELECT ConfugaJobWaitResult.exit_code FROM ConfugaJobWaitResult WHERE ConfugaJobWaitResult.id = Job.id),"
		"		exit_signal = (SELECT ConfugaJobWaitResult.exit_signal FROM ConfugaJobWaitResult WHERE ConfugaJobWaitResult.id = Job.id),"
		"		exit_status = (SELECT ConfugaJobWaitResult.exit_status FROM ConfugaJobWaitResult WHERE ConfugaJobWaitResult.id = Job.id),"
		"		status = 'FINISHED',"
		"		time_finish = strftime('%s', 'now')"
		"	WHERE id = ?;"
		"DELETE FROM ConfugaJobWaitResult WHERE id = ?;"
		"END TRANSACTION;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	jdebug(D_DEBUG, id, tag, "binding outputs");

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		confuga_fid_t fid;
		const char *path;
		confuga_off_t size;
		path = (const char *)sqlite3_column_text(stmt, 0);
		assert(sqlite3_column_type(stmt, 1) == SQLITE_BLOB && (size_t)sqlite3_column_bytes(stmt, 1) == confugaF_size(fid));
		confugaF_set(C, &fid, sqlite3_column_blob(stmt, 1));
		size = (confuga_off_t)sqlite3_column_int64(stmt, 2);
		CATCH(confuga_update(C, path, fid, size, 0));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
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
	return rc;
}

static int job_complete (confuga *C)
{
	static const char SQL[] =
		"SELECT ConfugaJob.id, ConfugaJob.tag, ConfugaJobWaitResult.status, ConfugaJobWaitResult.error"
		"	FROM ConfugaJob JOIN ConfugaJobWaitResult On ConfugaJob.id = ConfugaJobWaitResult.id"
		"	WHERE ConfugaJob.state = 'REAPED'"
		"	ORDER BY RANDOM();"; /* to ensure no starvation, bindoutputs/reschedule/fail may result in a ROLLBACK that aborts this SELECT */

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		chirp_jobid_t id = sqlite3_column_int64(stmt, 0);
		const char *tag = (const char *)sqlite3_column_text(stmt, 1);
		const char *status = (const char *)sqlite3_column_text(stmt, 2);
		const char *error = (const char *)sqlite3_column_text(stmt, 3);
		if (strcmp(status, "FINISHED") == 0) {
			CATCHJOB(C, id, tag, bindoutputs(C, id, tag));
		} else if (strcmp(status, "KILLED") == 0) {
			reschedule(C, id, tag, ECHILD); /* someone else killed it? reschedule */
		} else if (strcmp(status, "ERRORED") == 0) {
			if (strstr(error, "No child processes")) {
				reschedule(C, id, tag, ESRCH); /* someone else killed it? reschedule */
			} else if (strstr(error, "No such file or directory")) {
				reschedule(C, id, tag, ENOENT); /* files were lost? */
			} else {
				fail(C, id, tag, error);
			}
		} else {
			assert(0);
		}
		C->operations++;
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int jkill (confuga *C, chirp_jobid_t id, const char *tag, const char *hostport, chirp_jobid_t cid)
{
	static const char SQL[] =
		"BEGIN TRANSACTION;"
		"UPDATE ConfugaJob"
		"	SET"
		"		state = 'KILLED',"
		"		time_killed = (strftime('%s', 'now'))"
		"	WHERE id = ?;"
		"DELETE FROM ConfugaJobWaitResult WHERE id = ?;"
		"END TRANSACTION;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	BUFFER_STACK_PRINT(B, 64, "[%" PRICHIRP_JOBID_T "]", cid);

	if (cid > 0) {
		assert(hostport);
		jdebug(D_DEBUG, id, tag, "killing job");
		rc = chirp_reli_job_kill(hostport, buffer_tostring(B), STOPTIME);
		if (rc == -1 && !(errno == EACCES /* already in a terminal state */ || errno == ESRCH /* apparently remote Chirp server database was reset */))
			CATCH(errno);
		jdebug(D_DEBUG, id, tag, "reaping job");
		rc = chirp_reli_job_reap(hostport, buffer_tostring(B), STOPTIME);
		if (rc == -1 && !(errno == EACCES /* already in a terminal state */ || errno == ESRCH /* apparently remote Chirp server database was reset */))
			CATCH(errno);
	}

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
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
	return rc;
}

static int job_kill (confuga *C)
{
	static const char SQL[] =
		"SELECT ConfugaJob.id, ConfugaJob.tag, StorageNode.hostport, ConfugaJob.cid"
		"	FROM"
		"		Job"
		"		INNER JOIN ConfugaJob ON Job.id = ConfugaJob.id"
		"		LEFT OUTER JOIN Confuga.StorageNode ON ConfugaJob.sid = StorageNode.id"
		"	WHERE (Job.status = 'KILLED' OR Job.status = 'ERRORED') AND ConfugaJob.state != 'KILLED'"
		"	ORDER BY RANDOM();"; /* to ensure no starvation, kill may result in a ROLLBACK that aborts this SELECT */

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		chirp_jobid_t id = sqlite3_column_int64(stmt, 0);
		const char *tag = (const char *)sqlite3_column_text(stmt, 1);
		const char *hostport = (const char *)sqlite3_column_text(stmt, 2);
		chirp_jobid_t cid = sqlite3_column_int64(stmt, 3);
		jkill(C, id, tag, hostport, cid);
		C->operations++;
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int job_stats (confuga *C)
{
	static const char SQL[] =
		"SELECT PRINTF('%s (%d)', ConfugaJob.state, COUNT(ConfugaJob.id))"
		"	FROM ConfugaJob"
		"	GROUP BY ConfugaJob.state"
		"	ORDER BY ConfugaJob.state;"
		"SELECT COUNT(*)"
		"	FROM Confuga.StorageNodeActive;"
		"SELECT COUNT(*)"
		"	FROM ConfugaJobAllocated;"
		"SELECT COUNT(*)"
		"	FROM ConfugaJobExecuting;"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	buffer_t B[1];
	time_t now = time(NULL);

	buffer_init(B);

	if (now < C->job_stats+30) {
		rc = 0;
		goto out;
	}
	C->job_stats = now;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		const char *state = (const char *)sqlite3_column_text(stmt, 0);
		buffer_putfstring(B, "%s; ", state);
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		buffer_putfstring(B, "Active SN (%d); ", sqlite3_column_int(stmt, 0));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		buffer_putfstring(B, "Allocated SN (%d); ", sqlite3_column_int(stmt, 0));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		buffer_putfstring(B, "Executing SN (%d); ", sqlite3_column_int(stmt, 0));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	if (buffer_pos(B))
		debug(D_DEBUG, "%s", buffer_tostring(B));

	rc = 0;
	goto out;
out:
	buffer_free(B);
	sqlite3_finalize(stmt);
	return rc;
}

CONFUGA_IAPI int confugaJ_schedule (confuga *C)
{
	int rc;

	job_stats(C);
	job_new(C);
	job_bind_inputs(C);
	job_schedule(C);
	job_replicate(C);
	job_create(C);
	job_commit(C);
	job_wait(C);
	job_reap(C);
	job_complete(C);
	job_kill(C);

	rc = 0;
	goto out;
out:
	return rc;
}

/* vim: set noexpandtab tabstop=8: */
