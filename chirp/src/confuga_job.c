/*
Copyright (C) 2014- The University of Notre Dame
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
#include <stdarg.h>

#define STOPTIME (time(NULL)+5)

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
	vdebug(level, buffer_tostring(&B), va);
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
		"	exit_signal INTEGER,"
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
		"CREATE TABLE ConfugaJobTransferAttempt ("
		"	jid INTEGER REFERENCES ConfugaJob (id),"
		"	tjid INTEGER REFERENCES ConfugaTransferJob (id),"
		"	PRIMARY KEY (jid, tjid));"
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
		sqlcatch(sqlite3_bind_blob(stmt, 1, fid.id, sizeof(fid.id), SQLITE_STATIC));
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
		//ATTACH '/dev/shm/confuga.transient/.__job.db' AS Job; ATTACH '/dev/shm/confuga.root/confuga.db' as Confuga;
		"WITH"
			/* We want every active SN, even if it has no input file. */
		"	StorageNodeAvailable AS ("
		"			SELECT StorageNodeActive.id"
		"				FROM Confuga.StorageNodeActive LEFT OUTER JOIN ConfugaJobAllocated ON StorageNodeActive.id = ConfugaJobAllocated.sid"
		"				GROUP BY StorageNodeActive.id"
		"				HAVING COUNT(ConfugaJobAllocated.id) < 1" /* TODO: allow more than one job on a SN */
		"	),"
		"	StorageNodeJobBytes AS ("
		"		SELECT FileReplicas.sid, ConfugaInputFile.jid, SUM(FileReplicas.size) AS size"
		"			FROM ConfugaInputFile JOIN Confuga.FileReplicas ON ConfugaInputFile.fid = FileReplicas.fid"
		"			GROUP BY FileReplicas.sid, ConfugaInputFile.jid"
		"	)"
		"SELECT StorageNodeAvailable.id, StorageNodeJobBytes.jid, StorageNodeJobBytes.size"
		"	FROM StorageNodeAvailable LEFT OUTER JOIN StorageNodeJobBytes ON StorageNodeAvailable.id = StorageNodeJobBytes.sid"
		"	WHERE StorageNodeJobBytes.jid = ? OR StorageNodeJobBytes.jid IS NULL"
		"	ORDER BY StorageNodeJobBytes.size DESC" /* Order Storage Nodes by ones with most colocated File bytes. */
		"	LIMIT 1;"
		"UPDATE ConfugaJob"
		"	SET"
		"		sid = ?,"
		"		state = 'SCHEDULED',"
		"		time_scheduled = (strftime('%s', 'now'))"
		"	WHERE id = ?;"
		"UPDATE Job"
		"	SET status = 'STARTED', time_start = strftime('%s', 'now')"
		"	WHERE id = ?;"
		"END TRANSACTION;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	confuga_sid_t sid;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	rc = sqlite3_step(stmt);
	if (rc == SQLITE_ROW) {
		sid = sqlite3_column_int64(stmt, 0);
		jdebug(D_CONFUGA, id, tag, "scheduling on " CONFUGA_SID_DEBFMT, sid);
	} else if (rc == SQLITE_DONE) {
		jdebug(D_DEBUG, id, tag, "could not schedule yet");
		THROW_QUIET(EAGAIN); /* come back later */
	} else {
		sqlcatch(rc);
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, sid));
	sqlcatch(sqlite3_bind_int64(stmt, 2, id));
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

static int job_schedule_fifo (confuga *C)
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

static int replicate_push_synchronous (confuga *C, chirp_jobid_t id, const char *tag, sqlite3_int64 sid)
{
	static const char SQL[] =
		"BEGIN TRANSACTION;"
		/* unreplicated files */
		"SELECT ConfugaInputFile.fid, ConfugaJob.tag"
		"	FROM"
		"		ConfugaJob"
		"		JOIN ConfugaInputFile ON ConfugaJob.id = ConfugaInputFile.jid"
		"		LEFT OUTER JOIN Confuga.Replica ON ConfugaInputFile.fid = Replica.fid AND ConfugaJob.sid = Replica.sid"
		"	WHERE ConfugaJob.id = ?1 AND Replica.sid IS NULL AND Replica.fid IS NULL;"
		"UPDATE ConfugaJob"
		"	SET"
		"		state = 'REPLICATED',"
		"		time_replicated = (strftime('%s', 'now'))"
		"	WHERE id = ?;"
		"END TRANSACTION;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	time_t start = time(0);
	int paused = 0;

	jdebug(D_DEBUG, id, tag, "replicating files synchronously");

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		confuga_fid_t fid;
		assert(sqlite3_column_type(stmt, 0) == SQLITE_BLOB && (size_t)sqlite3_column_bytes(stmt, 0) == sizeof(fid.id));
		memcpy(fid.id, sqlite3_column_blob(stmt, 0), sizeof(fid.id));
		CATCH(confugaR_replicate(C, fid, sid, (const char *)sqlite3_column_text(stmt, 1), STOPTIME));
		if (start+60 <= time(0)) {
			paused = 1; /* if we replicate for more than 2 minutes, come back later to finish */
			break;
		}
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	if (paused) {
		jdebug(D_DEBUG, id, tag, "exceeded one minute of replication, coming back later to finish");
	} else {
		sqlcatch(sqlite3_bind_int64(stmt, 1, id));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		jdebug(D_DEBUG, id, tag, "finished replicating files");
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
	return rc;
}

static int replicate_push_asynchronous (confuga *C, chirp_jobid_t id, const char *tag)
{
	static const char SQL[] =
		/* TODO transfer attempts, reschedule on threshold */
		"BEGIN TRANSACTION;"
		/* Check if job has unreplicated files: */
		"SELECT COUNT(*)"
		"	FROM"
		"		ConfugaJob"
		"		JOIN ConfugaInputFile ON ConfugaJob.id = ConfugaInputFile.jid"
		"		LEFT OUTER JOIN Confuga.Replica ON ConfugaInputFile.fid = Replica.fid AND ConfugaJob.sid = Replica.sid"
		"	WHERE ConfugaJob.id = ?1 AND Replica.fid IS NULL AND Replica.sid IS NULL;"
			/* If fully replicated, set to REPLICATED */
			"UPDATE ConfugaJob"
			"	SET"
			"		state = 'REPLICATED',"
			"		time_replicated = (strftime('%s', 'now'))"
			"	WHERE id = ?1;"
			/* return! */
		/* else try to replicate... */
		"INSERT INTO Confuga.TransferJob (state, fid, fsid, tag, tsid)"
		"	WITH"
				/* Storage Nodes ongoing transfer count. */
		"		StorageNodeTransferCount AS ("
		"			SELECT sid, COUNT(tjid) AS _count"
		"				FROM"
		"					("
		"						SELECT StorageNode.id AS sid, ActiveTransfers.id AS tjid FROM"
		"							(Confuga.StorageNode LEFT OUTER JOIN Confuga.ActiveTransfers ON StorageNode.id = ActiveTransfers.tsid)"
		"					UNION"
		"						SELECT StorageNode.id AS sid, ActiveTransfers.id AS tjid FROM"
		"							(Confuga.StorageNode LEFT OUTER JOIN Confuga.ActiveTransfers ON StorageNode.id = ActiveTransfers.fsid)"
		"					)"
		"				GROUP BY sid"
		"		),"
				/* This a StorageNode we are able to use to transfer a replica. If all of its TransferSlots are in use, it is excluded. */
		"		SourceStorageNode AS ("
		"			SELECT FileReplicas.fid, StorageNodeActive.id as sid"
		"				FROM"
		"					Confuga.StorageNodeActive"
		"					JOIN StorageNodeTransferCount ON StorageNodeActive.id = StorageNodeTransferCount.sid"
		"					JOIN Confuga.FileReplicas ON StorageNodeActive.id = FileReplicas.sid"
		"				WHERE (?2 == 0 OR _count < ?2)"
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
		"			UNION"
		"				SELECT File.id AS fid, ActiveTransfers.tsid AS sid"
		"					FROM Confuga.File JOIN Confuga.ActiveTransfers ON File.id = ActiveTransfers.fid"
		"		),"
				/* Files needed by the scheduled job which are not in PotentialReplicas. */
		"		NeededFiles AS ("
		"			SELECT ConfugaJob.id, ConfugaInputFile.*"
		"				FROM"
		"					ConfugaJob"
		"					JOIN ConfugaInputFile ON ConfugaJob.id = ConfugaInputFile.jid"
		"					LEFT OUTER JOIN PotentialReplicas ON ConfugaInputFile.fid = PotentialReplicas.fid AND ConfugaJob.sid = PotentialReplicas.sid"
		"				WHERE ConfugaJob.state = 'SCHEDULED' AND PotentialReplicas.fid IS NULL AND PotentialReplicas.sid IS NULL"
		"		)"
		"	SELECT 'NEW', NeededFiles.fid, RandomSourceStorageNode.sid, ConfugaJob.tag, ConfugaJob.sid"
		"		FROM"
		"			ConfugaJob"
		"			JOIN StorageNodeTransferCount ON ConfugaJob.sid = StorageNodeTransferCount.sid"
		"			JOIN NeededFiles ON ConfugaJob.id = NeededFiles.id"
		"			JOIN RandomSourceStorageNode ON NeededFiles.fid = RandomSourceStorageNode.fid"
		"			JOIN FileReplicas ON NeededFiles.fid = FileReplicas.fid"
		"		WHERE ConfugaJob.id = ?1 AND (?2 == 0 OR StorageNodeTransferCount._count < ?2)"
		"		ORDER BY FileReplicas.size DESC"
		"		LIMIT 1;"
		"END TRANSACTION;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	int finished = 0;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
	if (sqlite3_column_int(stmt, 0) == 0)
		finished = 1;
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	if (finished) {
		jdebug(D_DEBUG, id, tag, "all dependencies are replicated");
		sqlcatch(sqlite3_bind_int64(stmt, 1, id));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	if (!finished) {
		sqlcatch(sqlite3_bind_int64(stmt, 1, id));
		sqlcatch(sqlite3_bind_int64(stmt, 2, C->replication_n));
		do {
			sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);

			sqlite3_int64 changes = sqlite3_changes(db);
			assert(changes == 1 || changes == 0);

			if (changes > 0) {
				jdebug(D_DEBUG, id, tag, "scheduled transfer job %" PRId64, (int64_t)sqlite3_last_insert_rowid(db));
			} else {
				/* FIXME check for stagnant jobs */
				break;
			}
			sqlcatch(sqlite3_reset(stmt));
		} while (1);
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
	return rc;
}

static int job_replicate (confuga *C)
{
	static const char SQL[] =
		"SELECT ConfugaJob.id, ConfugaJob.tag, ConfugaJob.sid"
		"	FROM ConfugaJob"
		"	WHERE state = 'SCHEDULED'"
		"	ORDER BY time_scheduled ASC;"; /* XXX A broken job will cause this to never make progress. ---- to ensure no starvation, replicate may result in a ROLLBACK that aborts this SELECT */

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	time_t start = time(0);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		chirp_jobid_t id = sqlite3_column_int64(stmt, 0);
		const char *tag = (const char *)sqlite3_column_text(stmt, 1);
		confuga_sid_t sid = sqlite3_column_int64(stmt, 2);
		if (C->replication == CONFUGA_REPLICATION_PUSH_SYNCHRONOUS)
			CATCHJOB(C, id, tag, replicate_push_synchronous(C, id, tag, sid));
		else if (C->replication == CONFUGA_REPLICATION_PUSH_ASYNCHRONOUS)
			CATCHJOB(C, id, tag, replicate_push_asynchronous(C, id, tag));
		else assert(0);
		if (start+60 <= time(0)) {
			rc = SQLITE_DONE;
			break;
		}
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int encode (confuga *C, chirp_jobid_t id, const char *tag, buffer_t *B)
{
	static const char SQL[] =
		"SELECT executable, Option.value"
		"	FROM Job JOIN Confuga.Option"
		"	WHERE Job.id = ? AND Option.key = 'id';"
		"SELECT arg FROM JobArgument WHERE id = ? ORDER BY n;"
		"SELECT name, value FROM JobEnvironment WHERE id = ?;"
		"	SELECT 'INPUT', StorageNode.root || '/file/' || HEX(ConfugaInputFile.fid), task_path, 'LINK'"
		"		FROM"
		"			ConfugaInputFile"
		"			INNER JOIN ConfugaJob ON ConfugaInputFile.jid = ConfugaJob.id"
		"			INNER JOIN Confuga.StorageNode ON ConfugaJob.sid = StorageNode.id"
		"		WHERE ConfugaInputFile.jid = ?"
		"UNION ALL"
		"	SELECT 'OUTPUT', StorageNode.root || '/file/%s', task_path, 'LINK'"
		"		FROM"
		"			JobFile"
		"			INNER JOIN ConfugaJob ON JobFile.id = ConfugaJob.id"
		"			INNER JOIN Confuga.StorageNode ON ConfugaJob.sid = StorageNode.id"
		"		WHERE JobFile.id = ? AND JobFile.type = 'OUTPUT';";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	int first;

	CATCHUNIX(buffer_putliteral(B, "{"));

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
	CATCHUNIX(buffer_putliteral(B, "\"executable\":"));
	chirp_sqlite3_column_jsonify(db, stmt, 0, B);
	CATCHUNIX(buffer_putliteral(B, ",\"tag\":"));
	chirp_sqlite3_column_jsonify(db, stmt, 1, B);
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	first = 1;
	CATCHUNIX(buffer_putliteral(B, ",\"arguments\":["));
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		if (first)
			first = 0;
		else
			CATCHUNIX(buffer_putliteral(B, ","));
		chirp_sqlite3_column_jsonify(db, stmt, 0, B);
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	CATCHUNIX(buffer_putliteral(B, "]"));

	first = 1;
	CATCHUNIX(buffer_putliteral(B, ",\"environment\":{"));
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		if (first)
			first = 0;
		else
			CATCHUNIX(buffer_putliteral(B, ","));
		chirp_sqlite3_column_jsonify(db, stmt, 0, B);
		CATCHUNIX(buffer_putliteral(B, ":"));
		chirp_sqlite3_column_jsonify(db, stmt, 1, B);
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	CATCHUNIX(buffer_putliteral(B, "}"));

	first = 1;
	CATCHUNIX(buffer_putliteral(B, ",\"files\":["));
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)id));
	sqlcatch(sqlite3_bind_int64(stmt, 2, (sqlite3_int64)id));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		if (first)
			first = 0;
		else
			CATCHUNIX(buffer_putliteral(B, ","));
		CATCHUNIX(buffer_putliteral(B, "{"));
		CATCHUNIX(buffer_putliteral(B, "\"type\":"));
		chirp_sqlite3_column_jsonify(db, stmt, 0, B);
		CATCHUNIX(buffer_putliteral(B, ",\"serv_path\":"));
		chirp_sqlite3_column_jsonify(db, stmt, 1, B);
		CATCHUNIX(buffer_putliteral(B, ",\"task_path\":"));
		chirp_sqlite3_column_jsonify(db, stmt, 2, B);
		CATCHUNIX(buffer_putliteral(B, ",\"binding\":"));
		chirp_sqlite3_column_jsonify(db, stmt, 3, B);
		CATCHUNIX(buffer_putliteral(B, "}"));
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

static int create (confuga *C, chirp_jobid_t id, const char *tag, const char *hostport)
{
	static const char SQL[] =
		"BEGIN TRANSACTION;"
		/* call to encode(...) */
		"UPDATE ConfugaJob"
		"	SET"
		"		state = 'CREATED',"
		"		cid = ?,"
		"		time_created = (strftime('%s', 'now'))"
		"	WHERE id = ?;"
		"END TRANSACTION;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	buffer_t B;
	chirp_jobid_t cid;

	buffer_init(&B);
	jdebug(D_DEBUG, id, tag, "creating job on storage node");

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	CATCH(encode(C, id, tag, &B));
	debug(D_DEBUG, "json = `%s'", buffer_tostring(&B));

	CATCHUNIX(chirp_reli_job_create(hostport, buffer_tostring(&B), &cid, STOPTIME));

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, cid));
	sqlcatch(sqlite3_bind_int64(stmt, 2, id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	buffer_free(&B);
	sqlite3_finalize(stmt);
	sqlend(db);
	return rc;
}

static int job_create (confuga *C)
{
	static const char SQL[] =
		"SELECT ConfugaJob.id, ConfugaJob.tag, StorageNode.hostport"
		"	FROM ConfugaJob INNER JOIN Confuga.StorageNode ON ConfugaJob.sid = StorageNode.id"
		"	WHERE state = 'REPLICATED'"
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
		CATCHJOB(C, id, tag, create(C, id, tag, hostport));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int commit (confuga *C, chirp_jobid_t id, const char *tag, const char *hostport, chirp_jobid_t cid)
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

	CATCHUNIX(chirp_reli_job_commit(hostport, buffer_tostring(&B), STOPTIME));

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
		"	WHERE state = 'CREATED';";

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
		CATCHJOB(C, id, tag, commit(C, id, tag, hostport, cid));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int wait (confuga *C, chirp_jobid_t id, const char *tag, const char *hostport, chirp_jobid_t cid)
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

			sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
			sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
			sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

			/* UPDATE ConfugaOutputFile */
			json_value *error = jsonA_getname(job, "error", json_string);
			json_value *exit_code = jsonA_getname(job, "exit_code", json_integer);
			json_value *exit_signal = jsonA_getname(job, "exit_signal", json_integer);
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
							json_value *size = jsonA_getname(file, "size", json_integer);
							if (task_path && serv_path && type) {
								if (strcmp(type->u.string.ptr, "OUTPUT") == 0 && size) {
									confuga_fid_t fid;
									const char *sp = serv_path->u.string.ptr;

									/* extract the File ID from interpolated serv_path */
									if (strlen(sp) > sizeof(fid.id)*2) {
										size_t k;
										const char *idx = strchr(sp, '\0')-sizeof(fid.id)*2;
										for (k = 0; k < sizeof(fid.id); k += 1, idx += 2) {
											char byte[3] = {idx[0], idx[1], '\0'};
											char *endptr;
											unsigned long value = strtoul(byte, &endptr, 16);
											if (endptr == &byte[2]) {
												fid.id[k] = value;
											} else {
												CATCH(EINVAL);
											}
										}
									}

									jdebug(D_DEBUG, id, tag, "adding ConfugaOutputFile fid = " CONFUGA_FID_PRIFMT " size = %" PRICONFUGA_OFF_T " task_path = `%s'", CONFUGA_FID_PRIARGS(fid), (confuga_off_t)size->u.integer, task_path->u.string.ptr);
									sqlcatch(sqlite3_reset(stmt));
									sqlcatch(sqlite3_bind_int64(stmt, 1, id));
									sqlcatch(sqlite3_bind_text(stmt, 2, task_path->u.string.ptr, -1, SQLITE_STATIC));
									sqlcatch(sqlite3_bind_blob(stmt, 3, fid.id, sizeof(fid.id), SQLITE_STATIC));
									sqlcatch(sqlite3_bind_int64(stmt, 4, size->u.integer));
									sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
								}
							} else {
								CATCH(EINVAL);
							}
						} else {
							CATCH(EINVAL);
						}
					}
				}
			}
			sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

			sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
			sqlcatch(sqlite3_bind_int64(stmt, 1, id));
			if (error)
				sqlcatch(sqlite3_bind_text(stmt, 2, error->u.string.ptr, -1, SQLITE_STATIC));
			if (exit_code)
				sqlcatch(sqlite3_bind_int64(stmt, 3, exit_code->u.integer));
			if (exit_signal)
				sqlcatch(sqlite3_bind_int64(stmt, 4, exit_signal->u.integer));
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
		"SELECT ConfugaJob.id, ConfugaJob.tag, StorageNode.hostport, ConfugaJob.cid"
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
		const char *hostport = (const char *)sqlite3_column_text(stmt, 2);
		chirp_jobid_t cid = sqlite3_column_int64(stmt, 3);
		CATCHJOB(C, id, tag, wait(C, id, tag, hostport, cid));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int reap (confuga *C, chirp_jobid_t id, const char *tag, const char *hostport, chirp_jobid_t cid)
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

	CATCHUNIX(chirp_reli_job_reap(hostport, buffer_tostring(&B), STOPTIME));

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
		"	WHERE state = 'WAITED';";

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
		CATCHJOB(C, id, tag, reap(C, id, tag, hostport, cid));
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
		"INSERT OR IGNORE INTO Confuga.File (id, size)"
		"	SELECT ConfugaOutputFile.fid, ConfugaOutputFile.size"
		"	FROM"
		"		ConfugaOutputFile"
		"		INNER JOIN JobFile ON ConfugaOutputFile.jid = JobFile.id AND ConfugaOutputFile.task_path = JobFile.task_path AND JobFile.type = 'OUTPUT'"
		"	WHERE ConfugaOutputFile.jid = ?;"
		"INSERT OR IGNORE INTO Confuga.Replica (fid, sid)"
		"	SELECT ConfugaOutputFile.fid, ConfugaJob.sid"
		"	FROM"
		"		ConfugaOutputFile"
		"		INNER JOIN JobFile ON ConfugaOutputFile.jid = JobFile.id AND ConfugaOutputFile.task_path = JobFile.task_path AND JobFile.type = 'OUTPUT'"
		"		INNER JOIN ConfugaJob ON ConfugaOutputFile.jid = ConfugaJob.id"
		"	WHERE ConfugaOutputFile.jid = ?;"
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
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, id));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		confuga_fid_t fid;
		const char *path;
		confuga_off_t size;
		path = (const char *)sqlite3_column_text(stmt, 0);
		assert(sqlite3_column_type(stmt, 1) == SQLITE_BLOB && (size_t)sqlite3_column_bytes(stmt, 1) == sizeof(fid.id));
		memcpy(fid.id, sqlite3_column_blob(stmt, 1), sizeof(fid.id));
		size = (confuga_off_t)sqlite3_column_int64(stmt, 2);
		CATCH(confuga_update(C, path, fid, size));
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
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int kill (confuga *C, chirp_jobid_t id, const char *tag, const char *hostport, chirp_jobid_t cid)
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

	if (hostport) {
		jdebug(D_DEBUG, id, tag, "killing job");
		rc = chirp_reli_job_kill(hostport, buffer_tostring(&B), STOPTIME);
		if (rc == -1 && !(errno == EACCES /* already in a terminal state */ || errno == ESRCH /* apparently remote Chirp server database was reset */))
			CATCH(errno);
		jdebug(D_DEBUG, id, tag, "reaping job");
		rc = chirp_reli_job_reap(hostport, buffer_tostring(&B), STOPTIME);
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
		"	WHERE (Job.status = 'KILLED' OR Job.status = 'ERRORED') AND ConfugaJob.state != 'KILLED' AND ConfugaJob.cid IS NOT NULL"
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
		kill(C, id, tag, hostport, cid);
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

CONFUGA_IAPI int confugaJ_schedule (confuga *C)
{
	int rc;

	CATCH(job_new(C));
	CATCH(job_bind_inputs(C));
	if (C->scheduler == CONFUGA_SCHEDULER_FIFO)
		CATCH(job_schedule_fifo(C));
	else assert(0);
	CATCH(job_replicate(C));
	CATCH(job_create(C));
	CATCH(job_commit(C));
	CATCH(job_wait(C));
	CATCH(job_reap(C));
	CATCH(job_complete(C));
	CATCH(job_kill(C));

	rc = 0;
	goto out;
out:
	return rc;
}

/* vim: set noexpandtab tabstop=4: */
