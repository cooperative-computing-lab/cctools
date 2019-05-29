/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "confuga_fs.h"

#include "chirp_reli.h"
#include "chirp_sqlite.h"
#include "chirp_types.h"

#include "catalog_query.h"
#include "catch.h"
#include "debug.h"
#include "json.h"
#include "json_aux.h"
#include "nvpair.h"
#include "sha1.h"
#include "stringtools.h"

#include <sys/socket.h>

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define STOPTIME (time(NULL)+30)

struct confuga_replica {
	confuga *C;
	confuga_fid_t fid;
	struct confuga_host host; /* Storage Node (SN) hosting the replica */
	char path[CHIRP_PATH_MAX]; /* path to the replica on the SN */
	struct chirp_file *stream;
};

struct confuga_file {
	confuga *C;
	confuga_sid_t sid;
	struct confuga_host host;
	char path[CONFUGA_PATH_MAX]; /* path to open file */
	struct chirp_file *stream; /* open chirp stream */
	sha1_context_t context; /* running hash */
	confuga_off_t size; /* running size */
};

CONFUGA_IAPI int confugaR_delete (confuga *C, confuga_sid_t sid, confuga_fid_t fid)
{
	static const char SQL[] =
		"SAVEPOINT confugaR_delete;"
		"INSERT OR IGNORE INTO Confuga.DeadReplica (fid, sid)"
		"	VALUES (?, ?)"
		";"
		"DELETE FROM Confuga.Replica"
		"	WHERE fid = ? AND sid = ?"
		";"
		"DELETE FROM Confuga.TransferJob"
		"	WHERE TransferJob.fid = ?1 AND NOT EXISTS (SELECT 1 FROM Confuga.Replica WHERE fid = ?1)"
		";"
		"DELETE FROM Confuga.File"
		"	WHERE File.id = ?1 AND NOT EXISTS (SELECT 1 FROM Confuga.Replica WHERE fid = ?1)"
		";"
		"RELEASE SAVEPOINT confugaR_delete;"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	debug(D_DEBUG, "deleting Replica fid = " CONFUGA_FID_PRIFMT " sid = " CONFUGA_SID_PRIFMT, CONFUGA_FID_PRIARGS(fid), sid);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_blob(stmt, 1, confugaF_id(fid), confugaF_size(fid), SQLITE_STATIC));
	sqlcatch(sqlite3_bind_int64(stmt, 2, sid));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_blob(stmt, 1, confugaF_id(fid), confugaF_size(fid), SQLITE_STATIC));
	sqlcatch(sqlite3_bind_int64(stmt, 2, sid));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	if (sqlite3_changes(db))
		debug(D_DEBUG, "deleted Replica fid = " CONFUGA_FID_PRIFMT " sid = " CONFUGA_SID_PRIFMT, CONFUGA_FID_PRIARGS(fid), sid);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_blob(stmt, 1, confugaF_id(fid), confugaF_size(fid), SQLITE_STATIC));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_blob(stmt, 1, confugaF_id(fid), confugaF_size(fid), SQLITE_STATIC));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	if (sqlite3_changes(db))
		debug(D_DEBUG, "deleted File fid = " CONFUGA_FID_PRIFMT, CONFUGA_FID_PRIARGS(fid));
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlendsavepoint(confugaR_delete);
	return rc;
}


CONFUGA_IAPI int confugaR_register (confuga *C, confuga_fid_t fid, confuga_off_t size, confuga_sid_t sid)
{
	static const char SQL[] =
		"SAVEPOINT confugaR_register;"
		"INSERT OR IGNORE INTO Confuga.File (id, size)"
		"   VALUES (?, ?);"
		"INSERT OR IGNORE INTO Confuga.Replica (fid, sid)"
		"   SELECT ?, Confuga.StorageNode.id"
		"   FROM Confuga.StorageNode"
		"   WHERE id = ?;"
		"RELEASE SAVEPOINT confugaR_register;"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_blob(stmt, 1, confugaF_id(fid), confugaF_size(fid), SQLITE_STATIC));
	sqlcatch(sqlite3_bind_int64(stmt, 2, size));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	if (sqlite3_changes(db))
		debug(D_DEBUG, "created new file fid = " CONFUGA_FID_PRIFMT " size = %" PRICONFUGA_OFF_T, CONFUGA_FID_PRIARGS(fid), size);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_blob(stmt, 1, confugaF_id(fid), confugaF_size(fid), SQLITE_STATIC));
	sqlcatch(sqlite3_bind_int64(stmt, 2, sid));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	if (sqlite3_changes(db))
		debug(D_DEBUG, "created new replica fid = " CONFUGA_FID_PRIFMT " sid = " CONFUGA_SID_PRIFMT, CONFUGA_FID_PRIARGS(fid), sid);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlendsavepoint(confugaR_register);
	return rc;
}

CONFUGA_IAPI int confugaR_replicate (confuga *C, confuga_fid_t fid, confuga_sid_t sid, const char *tag, time_t stoptime)
{
	static const char SQL[] =
		"SAVEPOINT confugaR_replicate;"
		/* Check for Replica. */
		"SELECT 1"
		"	FROM Confuga.Replica"
		"	WHERE fid = ? AND sid = ?;"
		/* Get the hostport/root of the SN we're replicating to. */
		"SELECT hostport, root, PRINTF('%s/open/%s', root, UPPER(HEX(RANDOMBLOB(16))))"
		"	FROM Confuga.StorageNode"
		"	WHERE id = ?;"
		/* Get current Storage Nodes hosting the File. */
		"SELECT FileReplicas.size, StorageNodeActive.id, StorageNodeActive.hostport, StorageNodeActive.root"
		"	FROM"
		"		Confuga.FileReplicas"
		"		JOIN Confuga.StorageNodeActive ON FileReplicas.sid = StorageNodeActive.id"
		"	WHERE fid = ?;"
		/* Insert new Replica. */
		"INSERT INTO Confuga.Replica (fid, sid) VALUES (?, ?);"
		/* Insert a fake TransferJob for records... */
		"INSERT INTO Confuga.TransferJob (state, fid, fsid, tsid, progress, time_new, time_commit, time_complete, tag)"
		"	VALUES ('COMPLETED', ?1, ?2, ?3, ?4, ?5, ?5, strftime('%s', 'now'), ?6);"
		"RELEASE SAVEPOINT confugaR_replicate;"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	struct confuga_host host_to;
	char replica_open[CONFUGA_PATH_MAX];
	char replica_closed[CONFUGA_PATH_MAX];
	time_t start;
	confuga_sid_t fsid = 0;
	confuga_off_t size;

	debug(D_DEBUG, "synchronously replicating " CONFUGA_FID_DEBFMT " to " CONFUGA_SID_DEBFMT, CONFUGA_FID_PRIARGS(fid), sid);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_blob(stmt, 1, confugaF_id(fid), confugaF_size(fid), SQLITE_STATIC));
	sqlcatch(sqlite3_bind_int64(stmt, 2, sid));
	rc = sqlite3_step(stmt);
	if (rc == SQLITE_ROW) {
		rc = 0;
		goto out;
	} else if (rc != SQLITE_DONE) {
		sqlcatch(rc);
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, sid));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);

	string_nformat(host_to.hostport, sizeof(host_to.hostport), "%s", (const char *)sqlite3_column_text(stmt, 0));
	string_nformat(host_to.root, sizeof(host_to.root), "%s", (const char *)sqlite3_column_text(stmt, 1));
	string_nformat(replica_open, sizeof(replica_open), "%s", (const char *)sqlite3_column_text(stmt, 2));
	string_nformat(replica_closed, sizeof(replica_closed), "%s/file/" CONFUGA_FID_PRIFMT, host_to.root, CONFUGA_FID_PRIARGS(fid));

	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_blob(stmt, 1, confugaF_id(fid), confugaF_size(fid), SQLITE_STATIC));
	if (chirp_reli_access(host_to.hostport, replica_closed, R_OK, STOPTIME) == 0)
		goto replicated; /* already there, just not in DB yet */
	/* else try to replicate... */
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		INT64_T result;
		struct confuga_host host_from;
		char replica_from[CONFUGA_PATH_MAX];
		time_t timeout = time(0)+60; /* wait at least 1 minute */

		size = (confuga_off_t)sqlite3_column_int64(stmt, 0);
		timeout += size/(50*1024); /* at least 50 KBps */
		debug(D_DEBUG, "file size is %" PRICONFUGA_OFF_T "; adding %" PRIu64 " to timeout", size, (uint64_t)size/(50*1024));

		start = time(NULL);
		fsid = sqlite3_column_int64(stmt, 1);

		string_nformat(host_from.hostport, sizeof(host_from.hostport), "%s", (const char *)sqlite3_column_text(stmt, 2));
		string_nformat(host_from.root, sizeof(host_from.root), "%s", (const char *)sqlite3_column_text(stmt, 3));
		string_nformat(replica_from, sizeof(replica_from), "%s/file/" CONFUGA_FID_PRIFMT, host_from.root, CONFUGA_FID_PRIARGS(fid));

		result = chirp_reli_thirdput(host_from.hostport, replica_from, host_to.hostport, replica_open, timeout);
		if (result >= 0) {
			rc = chirp_reli_rename(host_to.hostport, replica_open, replica_closed, STOPTIME);
			if (rc == 0) {
				goto replicated;
			} else if (rc == -1 && errno == ENOENT) {
				/* previous reli_rename worked? */
				if (chirp_reli_access(host_to.hostport, replica_closed, R_OK, STOPTIME) == 0)
					goto replicated;
			}
		}
		debug(D_DEBUG, "= %" PRId64 " (errno = %d `%s')", result, errno, strerror(errno));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	CATCH(EIO);
replicated:
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_blob(stmt, 1, confugaF_id(fid), confugaF_size(fid), SQLITE_STATIC));
	sqlcatch(sqlite3_bind_int64(stmt, 2, sid));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	assert(sqlite3_changes(db));
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	if (fsid) {
		/* fsid is 0 if it was already there... (access) */
		debug(D_DEBUG, CONFUGA_FID_DEBFMT " from " CONFUGA_SID_DEBFMT " to " CONFUGA_SID_DEBFMT " size=%" PRICONFUGA_OFF_T, CONFUGA_FID_PRIARGS(fid), fsid, sid, size);
		sqlcatch(sqlite3_bind_blob(stmt, 1, confugaF_id(fid), confugaF_size(fid), SQLITE_STATIC));
		sqlcatch(sqlite3_bind_int64(stmt, 2, fsid));
		sqlcatch(sqlite3_bind_int64(stmt, 3, sid));
		sqlcatch(sqlite3_bind_int64(stmt, 4, size));
		sqlcatch(sqlite3_bind_int64(stmt, 5, start));
		sqlcatch(sqlite3_bind_text(stmt, 6, tag, -1, SQLITE_STATIC));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		assert(sqlite3_changes(db));
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlendsavepoint(confugaR_replicate);
	return rc;
}

CONFUGA_API int confuga_replica_open (confuga *C, confuga_fid_t fid, confuga_replica **replicap, time_t stoptime)
{
	static const char SQL[] =
		"DROP TABLE IF EXISTS ConfugaResults;"
		"CREATE TEMPORARY TABLE ConfugaResults AS" /* so we don't hold read locks */
		"	SELECT Confuga.StorageNodeActive.hostport, Confuga.StorageNodeActive.root"
		"		FROM Confuga.Replica JOIN Confuga.StorageNodeActive ON Confuga.Replica.sid = Confuga.StorageNodeActive.id"
		"		WHERE Confuga.Replica.fid = ?"
		"		ORDER BY RANDOM();" /* random replica */
		"SELECT * FROM ConfugaResults;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	int n = 0;

	debug(D_CONFUGA, "replica_open(fid = '" CONFUGA_FID_PRIFMT "')", CONFUGA_FID_PRIARGS(fid));

	*replicap = NULL;
	confuga_replica *replica = malloc(sizeof(struct confuga_replica));
	if (replica == NULL)
		CATCH(ENOMEM);

	replica->C =C;
	replica->fid = fid;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_blob(stmt, 1, confugaF_id(fid), confugaF_size(fid), SQLITE_STATIC));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		n += 1;

		string_nformat(replica->host.hostport, sizeof(replica->host.hostport), "%s", (const char *) sqlite3_column_text(stmt, 0));
		string_nformat(replica->host.root, sizeof(replica->host.root), "%s", (const char *) sqlite3_column_text(stmt, 1));
		string_nformat(replica->path, sizeof(replica->path), "%s/file/" CONFUGA_FID_PRIFMT, replica->host.root, CONFUGA_FID_PRIARGS(fid));

		replica->stream = chirp_reli_open(replica->host.hostport, replica->path, O_RDONLY, 0, STOPTIME);
		if (replica->stream) {
			debug(D_CONFUGA, "opened replica %s/%s", replica->host.hostport, replica->path);
			*replicap = replica;
			rc = 0;
			goto out;
		}
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	if (n == 0)
		CATCH(ENOENT); /* no replicas */
	else
		CATCH(EIO); /* couldn't open a replica */
out:
	if (rc)
		free(replica);
	sqlite3_finalize(stmt);
	sqlite3_exec(db, "DROP TABLE IF EXISTS ConfugaResults;", NULL, NULL, NULL);
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));
	return rc;
}

CONFUGA_API int confuga_replica_pread (confuga_replica *replica, void *buffer, size_t size, size_t *n, confuga_off_t offset, time_t stoptime)
{
	int rc;
	if (replica->stream == NULL)
		CATCH(EINVAL);
	debug(D_CONFUGA, "replica_pread(fid = '" CONFUGA_FID_PRIFMT "', size = %zu, offset = %" PRIuCONFUGA_OFF_T ")", CONFUGA_FID_PRIARGS(replica->fid), size, offset);
	CATCHUNIX(chirp_reli_pread(replica->stream, buffer, size, offset, stoptime));
	*n = rc;
	rc = 0;
out:
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));
	return rc;
}

CONFUGA_API int confuga_replica_close (confuga_replica *replica, time_t stoptime)
{
	int rc;
	if (replica->stream == NULL)
		CATCH(EINVAL);
	debug(D_CONFUGA, "replica_close(fid = '" CONFUGA_FID_PRIFMT "')", CONFUGA_FID_PRIARGS(replica->fid));
	CATCHUNIX(chirp_reli_close(replica->stream, stoptime));
	replica->stream = NULL;
out:
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));
	return rc;
}

CONFUGA_API int confuga_file_create (confuga *C, confuga_file **filep, time_t stoptime)
{
	static const char SQL[] =
		"DROP TABLE IF EXISTS ConfugaFileTargets;"
		"CREATE TEMPORARY TABLE ConfugaFileTargets AS" /* so we don't hold read locks */
		"	SELECT StorageNodeActive.*, PRINTF('%s/open/%s', root, UPPER(HEX(RANDOMBLOB(16)))) AS _open, COUNT(FileReplicas.fid) AS _count, SUM(FileReplicas.size) AS _bytes"
		"		FROM"
		"			Confuga.StorageNodeActive"
		"			LEFT OUTER JOIN Confuga.FileReplicas ON StorageNodeActive.id = FileReplicas.sid"
		"		GROUP BY StorageNodeActive.id;"
		"SELECT id, hostport, root, _open"
		"	FROM ConfugaFileTargets"
		/* 1. Prefer nodes with lower than normal replica count (group exponentially). */
		/* 2. Prefer nodes with more space available (group exponentially). */
		/* 3. Prefer nodes that have fewer replica bytes stored. */
		"	ORDER BY FLOOR(LOG(_count+1)) ASC, FLOOR(LOG(avail+1)) DESC, _bytes ASC;"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	*filep = NULL;

	confuga_file *file = malloc(sizeof(struct confuga_file));
	if (file == NULL)
		CATCH(ENOMEM);

	debug(D_CONFUGA, "file_create(...)");

	file->C = C;
	strncpy(file->path, "", sizeof(file->path));
	file->size = 0;
	sha1_init(&file->context);
	file->stream = NULL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		file->sid = sqlite3_column_int64(stmt, 0);
		snprintf(file->host.hostport, sizeof(file->host.hostport), "%s", (const char *) sqlite3_column_text(stmt, 1));
		snprintf(file->host.root, sizeof(file->host.root), "%s", (const char *) sqlite3_column_text(stmt, 2));
		strcpy(file->path, (const char *) sqlite3_column_text(stmt, 3));
		debug(D_DEBUG, "creating file on free SN chirp://%s%s", file->host.hostport, file->host.root);
		file->stream = chirp_reli_open(file->host.hostport, file->path, O_CREAT|O_EXCL|O_WRONLY, S_IRUSR, STOPTIME);
		if (file->stream) {
			debug(D_CONFUGA, "opened file stream %s%s", file->host.hostport, file->path);
			*filep = file;
			rc = 0;
			goto out;
		}
		/* this storage node is no good, let's move on... */
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	debug(D_CONFUGA, "there is no Storage Node available?");
	CATCH(EIO);
out:
	if (rc)
		free(file);
	sqlite3_finalize(stmt);
	sqlite3_exec(db, "DROP TABLE IF EXISTS ConfugaFileTargets;", NULL, NULL, NULL);
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));
	return rc;
}

CONFUGA_API int confuga_file_write (confuga_file *file, const void *buffer, size_t length, size_t *n, time_t stoptime)
{
	int rc;
	if (file->stream == NULL)
		CATCH(EINVAL);
	debug(D_CONFUGA, "file_write(stream = '%s%s', length = %zu)", file->host.hostport, file->path, length);
	CATCHUNIX(chirp_reli_pwrite(file->stream, buffer, length, file->size, stoptime));
	sha1_update(&file->context, buffer, rc);
	file->size += rc;
	*n = rc;
	rc = 0;
	goto out;
out:
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));
	return rc;
}

CONFUGA_API int confuga_file_truncate (confuga_file *file, confuga_off_t length, time_t stoptime)
{
	int rc;

	if (file->stream == NULL)
		CATCH(EINVAL);
	if (0 < length && length < file->size)
		CATCH(EINVAL);

	debug(D_CONFUGA, "file_truncate(stream = '%s%s', length = %" PRIuCONFUGA_OFF_T ")", file->host.hostport, file->path, length);
	CATCHUNIX(chirp_reli_ftruncate(file->stream, length, stoptime));
	if (length == 0) {
		file->size = 0;
		sha1_init(&file->context);
	} else {
		/* now update internal structure */
		length -= file->size;
		file->size += length;
		while (length > 0) {
			/* ANSI C standard requires this be initialized with 0.
			*
			* Also note, despite its large size, the executable size does not increase
			* as this is put in the "uninitialized" .bss section. [Putting it in
			* .rodata would increase the executable size.]
			*/
			static const unsigned char zeroes[1<<20];

			confuga_off_t chunksize = sizeof(zeroes) < length ? sizeof(zeroes) : length;
			sha1_update(&file->context, zeroes, chunksize);
			length -= chunksize;
		}
	}
	rc = 0;
	goto out;
out:
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));
	return rc;
}

CONFUGA_API int confuga_file_close (confuga_file *file, confuga_fid_t *fid, confuga_off_t *size, time_t stoptime)
{
	int rc;
	confuga *C = file->C;
	confuga_file fcopy = *file;
	char replica[CONFUGA_PATH_MAX];
	int concrete = 0;

	file = realloc(file, 0);

	if (fcopy.stream) {
		debug(D_CONFUGA, "file_close(stream = '%s%s')", fcopy.host.hostport, fcopy.path);
		CATCHUNIX(chirp_reli_close(fcopy.stream, stoptime));
		fcopy.stream = NULL;
		sha1_final(fid->id, &fcopy.context);
		*size = fcopy.size;
	}

	CATCHUNIX(snprintf(replica, sizeof(replica), "%s/file/" CONFUGA_FID_PRIFMT, fcopy.host.root, CONFUGA_FID_PRIARGS(*fid)));
	CATCHUNIX(chirp_reli_rename(fcopy.host.hostport, fcopy.path, replica, stoptime));
	concrete = 1;
	CATCH(confugaR_register(C, *fid, fcopy.size, fcopy.sid));

	rc = 0;
	goto out;
out:
	if (rc && !concrete) {
		chirp_reli_unlink(fcopy.host.hostport, fcopy.path, stoptime);
	}
	debug(D_CONFUGA, "= %d (%s) [fid = " CONFUGA_FID_PRIFMT ", size = %" PRIuCONFUGA_OFF_T "]", rc, strerror(rc), CONFUGA_FID_PRIARGS(*fid), *size);
	return rc;
}

CONFUGA_API int confuga_setrep (confuga *C, confuga_fid_t fid, int nreps)
{
	static const char SQL[] =
		"UPDATE Confuga.File"
		"    SET minimum_replicas = ?"
		"    WHERE id = ?";

	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	int rc;

	debug(D_CONFUGA, "setrep(" CONFUGA_FID_DEBFMT ", %d)", CONFUGA_FID_PRIARGS(fid), nreps);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, nreps));
	sqlcatch(sqlite3_bind_blob(stmt, 2, confugaF_id(fid), confugaF_size(fid), SQLITE_STATIC));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	if (sqlite3_changes(db) == 0)
		CATCH(EINVAL); /* invalid StorageNode, File ID */
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	debug(D_CONFUGA, "= %d (%s)", rc, strerror(rc));
	return rc;
}

/* Replica GC, Replica Health, Replication, SN Health */
static int do_upkeep (confuga *C)
{
	int rc;

	/* Replica GC:
	 *
	 * SELECT File.id, Replica.sid, last_used
	 *     FROM (SELECT File.id, Replica.sid, last_used
	 *               FROM File JOIN Replica ON File.id = Replica.fid
	 *               GROUP BY File.id
	 *               HAVING COUNT(Replica.sid) > File.minimum_replicas)
	 *     WHERE last_used <= datetime('now', '-1 week')
	 *     ORDER BY last_used
	 *     LIMIT 1;
	 *
	 * Find all replicas that have no associated File (deleted by NM) and haven't been used in 1 month.
	 *
	 */

	/* Replica Health:
	 *
	 * SELECT fid, sid
	 *     FROM Replica
	 *     WHERE time_health <= strftime('%s', 'now', '-7 days')
	 *     ORDER BY time_health
	 *
	 * INSERT INTO ReplicaHealthCheckIntent
	 *     VALUES (fid, sid, 0);
	 */

	rc = 0;
	goto out;
out:
	return rc;
}

/* Schedule replication of degraded files with unsatisfied minimum_replicas.
 *
 * TODO: If there is an error, the retry should have some delay.
 *
 * Note:
 *   o The file must be at least 60 seconds old.
 */
static int schedule_replication (confuga *C)
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
		"CREATE TEMPORARY VIEW IF NOT EXISTS TransferSchedule__schedule_replication AS"
		"	WITH"
				/* This a StorageNode we are able to use to transfer a replica. If it is currently transferring a file, it is excluded. */
		"		StorageNodeActiveRandom AS ("
		"			SELECT StorageNodeAuthenticated.*, RANDOM() AS _r"
		"				FROM StorageNodeAuthenticated"
		"		),"
		"		SourceStorageNode AS ("
		"			SELECT Replica.fid, StorageNodeActiveRandom.id AS sid, MIN(StorageNodeActiveRandom._r)"
		"				FROM Confuga.Replica JOIN StorageNodeActiveRandom ON Replica.sid = StorageNodeActiveRandom.id"
		"				WHERE NOT EXISTS (SELECT fsid FROM Confuga.ActiveTransfers WHERE fsid = StorageNodeActiveRandom.id)"
		"				GROUP BY Replica.fid"
		"		),"
				/* This contains all the Replica of a File AND ongoing transfers of the File to some StorageNode */
		"		Replicas AS ("
		"				SELECT FileReplicas.id AS fid, FileReplicas.sid"
		"					FROM"
		"						Confuga.FileReplicas"
		"						JOIN Confuga.StorageNodeActive ON FileReplicas.sid = StorageNodeActive.id"
		"			UNION ALL"
		"				SELECT File.id AS fid, ActiveTransfers.tsid AS sid"
		"					FROM Confuga.File JOIN Confuga.ActiveTransfers ON File.id = ActiveTransfers.fid"
		"		),"
				/* These are degraded files, insufficient replicas exist. */
		"		DegradedFile AS ("
		"			SELECT File.id, File.size, COUNT(Replicas.sid) AS count, File.minimum_replicas AS min"
		"				FROM Confuga.File LEFT OUTER JOIN Replicas ON File.id = Replicas.fid"
		"				WHERE File.time_create < (strftime('%s', 'now')-60)"
		"				GROUP BY File.id"
		"				HAVING COUNT(Replicas.sid) < File.minimum_replicas"
						/* We want to focus on degraded files which have low replica counts. */
		"				ORDER BY count ASC"
						/* This is an optimization because the complete SELECT query is limited to 1. */
		"				LIMIT 1"
		"		)"
		"	SELECT 'NEW', 'HEALTH', DegradedFile.id, SourceStorageNode.sid, TargetStorageNode.id, '(replication)'"
		"		FROM"
		"			DegradedFile"
		"			JOIN SourceStorageNode ON DegradedFile.id = SourceStorageNode.fid"
		"			JOIN StorageNodeActive AS TargetStorageNode"
				/* Originally, TargetStorageNode was a VIEW in the WITH clause. It JOINed on File so we could come up with a Target for each File. This was too expensive so the join is moved here, on DegradedFile. */
		"		WHERE NOT EXISTS (SELECT sid FROM Replicas WHERE fid = DegradedFile.id AND sid = TargetStorageNode.id) AND TargetStorageNode.avail > DegradedFile.size"
		"		GROUP BY DegradedFile.id"
				/* Create a new Replica for a File which has a low minimum first. */
		"		ORDER BY FLOOR(LOG(TargetStorageNode.avail+1)) DESC"
				/* This limit is important because making a transfer job affects the next creation of subsequent transfer jobs. */
		"		LIMIT 1;"
		"SELECT COUNT(*) FROM TransferSchedule__schedule_replication;"
		"BEGIN IMMEDIATE TRANSACTION;"
		"INSERT INTO Confuga.TransferJob (state, source, fid, fsid, tsid, tag)"
		"	SELECT * FROM TransferSchedule__schedule_replication;"
		"END TRANSACTION;"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

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

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	do {
		/* continue inserting until we stop making TransferJobs */
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
		C->operations++;
	} while (sqlite3_changes(db));
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

static int fail (confuga *C, chirp_jobid_t id, const char *error)
{
	static const char SQL[] =
		"UPDATE Confuga.TransferJob"
		"	SET"
		"		error = ?,"
		"		state = 'ERRORED',"
		"		time_error = strftime('%s', 'now')"
		"	WHERE id = ?;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	debug(D_DEBUG, "transfer job error: `%s'", error);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_text(stmt, 1, error, -1, SQLITE_STATIC));
	sqlcatch(sqlite3_bind_int64(stmt, 2, id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static void handle_error (confuga *C, chirp_jobid_t id, int error)
{
	assert(error);
	switch (error) {
		case EAGAIN:
			/* This is either a SQL database lock error or the Chirp server is busy (also with a locked Job database). */
			break;
		case EINTR:
			/* temporary error */
			break;
#ifdef ECONNRESET
		case ECONNRESET:
			break;
#endif
#ifdef ETIMEDOUT
		case ETIMEDOUT:
			break;
#endif
		case ESRCH:
			/* somehow the job was lost on the remote Chirp server, probably Job DB got wiped? */
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
			//reschedule(C, id, error);
			//break;
		default:
			fail(C, id, strerror(error));
			break;
	}
}

#define CATCHJOB(expr) \
	do {\
		rc = (expr);\
		if (rc) {\
			handle_error(C, id, rc);\
		}\
	} while (0)

static int create (confuga *C, chirp_jobid_t id, const char *fhostport, const char *ffile, const char *fticket, const char *fdebug, const char *thostport, const char *topen, const char *tag)
{
	static const char SQL[] =
		"UPDATE Confuga.TransferJob"
		"	SET"
		"		cid = ?,"
		"		open = ?,"
		"		state = 'CREATED',"
		"		time_create = strftime('%s', 'now')"
		"	WHERE id = ?;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	chirp_jobid_t cid;
	buffer_t B[1];
	buffer_init(B);

	debug(D_DEBUG, "transfer job %" PRICHIRP_JOBID_T ": creating job", id);

	CATCHUNIX(buffer_putliteral(B, "{"));

	CATCHUNIX(buffer_putliteral(B, "\"executable\":\"@put\""));
	CATCHUNIX(buffer_putfstring(B, ",\"tag\":\"%s\"", tag));

	CATCHUNIX(buffer_putliteral(B, ",\"arguments\":["));
	CATCHUNIX(buffer_putliteral(B, "\"@put\""));
	CATCHUNIX(buffer_putliteral(B, ",\"")); jsonA_escapestring(B, thostport); CATCHUNIX(buffer_putliteral(B, "\""));
	CATCHUNIX(buffer_putliteral(B, ",\"file\""));
	CATCHUNIX(buffer_putliteral(B, ",\"")); jsonA_escapestring(B, topen); CATCHUNIX(buffer_putliteral(B, "\""));
	CATCHUNIX(buffer_putliteral(B, "]"));

	CATCHUNIX(buffer_putliteral(B, ",\"environment\":{\"CHIRP_CLIENT_TICKETS\":\"./confuga.ticket\"}"));

	CATCHUNIX(buffer_putliteral(B, ",\"files\":["));
	CATCHUNIX(buffer_putliteral(B, "{\"task_path\":\"file\",\"serv_path\":\""));
		jsonA_escapestring(B, ffile);
		CATCHUNIX(buffer_putliteral(B, "\",\"type\":\"INPUT\",\"binding\":\"LINK\"}"));
	CATCHUNIX(buffer_putliteral(B, ",{\"task_path\":\"./confuga.ticket\",\"serv_path\":\""));
		jsonA_escapestring(B, fticket);
		CATCHUNIX(buffer_putliteral(B, "\",\"type\":\"INPUT\",\"binding\":\"LINK\"}"));
	CATCHUNIX(buffer_putliteral(B, ",{\"task_path\":\".chirp.debug\",\"serv_path\":\""));
		jsonA_escapestring(B, fdebug);
		CATCHUNIX(buffer_putliteral(B, "\",\"type\":\"OUTPUT\",\"binding\":\"LINK\"}"));
	CATCHUNIX(buffer_putliteral(B, "]"));

	CATCHUNIX(buffer_putliteral(B, "}"));

	debug(D_DEBUG, "json = `%s'", buffer_tostring(B));
	CATCHUNIX(chirp_reli_job_create(fhostport, buffer_tostring(B), &cid, STOPTIME));

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, cid));
	sqlcatch(sqlite3_bind_text(stmt, 2, topen, -1, SQLITE_STATIC));
	sqlcatch(sqlite3_bind_int64(stmt, 3, id));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	buffer_free(B);
	return rc;
}

static int transfer_create (confuga *C)
{
	static const char SQL[] =
		"SELECT"
		"		TransferJob.id,"
		"		fsn.hostport,"
		"		PRINTF('%s/file/%s', fsn.root, UPPER(HEX(TransferJob.fid))),"
		"		PRINTF('%s/ticket', fsn.root),"
		"		PRINTF('%s/debug.%%j', fsn.root),"
		"		tsn.hostport,"
		"		PRINTF('%s/open/%s', tsn.root, UPPER(HEX(RANDOMBLOB(16)))),"
		"		State.value"
		"	FROM"
		"		Confuga.State,"
		"		Confuga.TransferJob"
		"		JOIN Confuga.StorageNode AS fsn ON TransferJob.fsid = fsn.id"
		"		JOIN Confuga.StorageNode AS tsn ON TransferJob.tsid = tsn.id"
		"	WHERE TransferJob.state = 'NEW' AND State.key = 'id'" /* TODO: AND TransferJob.last_attempt + TransferJob.attempts^2 < strftime('%s', 'now') */
		"	ORDER BY RANDOM()" /* to ensure no starvation, create may result in a ROLLBACK that aborts this SELECT */
		";";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		chirp_jobid_t id = sqlite3_column_int64(stmt, 0);
		const char *fhostport = (const char *)sqlite3_column_text(stmt, 1);
		const char *ffile = (const char *)sqlite3_column_text(stmt, 2);
		const char *fticket = (const char *)sqlite3_column_text(stmt, 3);
		const char *fdebug = (const char *)sqlite3_column_text(stmt, 4);
		const char *thostport = (const char *)sqlite3_column_text(stmt, 5);
		const char *topen = (const char *)sqlite3_column_text(stmt, 6);
		const char *tag = (const char *)sqlite3_column_text(stmt, 7);

		CATCHJOB(create(C, id, fhostport, ffile, fticket, fdebug, thostport, topen, tag));
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

static int commit (confuga *C, confuga_sid_t sid, const char *hostport, const char *tjids, const char *cids)
{
	static const char SQL[] =
		"UPDATE Confuga.TransferJob"
		"	SET"
		"		state = 'COMMITTED',"
		"		time_commit = strftime('%s', 'now')"
		"	WHERE id = ? AND state = 'CREATED'"
		";"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	json_value *J = NULL;
	unsigned i;

	J = json_parse(tjids, strlen(tjids));
	assert(J && jistype(J, json_array));

	debug(D_DEBUG, "transfer jobs %s: committing", tjids);

	CATCHUNIX(chirp_reli_job_commit(hostport, cids, STOPTIME));

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	for (i = 0; i < J->u.array.length; i++) {
		json_value *id = J->u.array.values[i];
		assert(jistype(id, json_integer));
		sqlcatch(sqlite3_reset(stmt));
		sqlcatch(sqlite3_bind_int64(stmt, 1, id->u.integer));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	json_value_free(J);
	sqlite3_finalize(stmt);
	return rc;
}

static int transfer_commit (confuga *C)
{
	static const char SQL[] =
		"SELECT StorageNode.id, StorageNode.hostport, PRINTF('[%s]', GROUP_CONCAT(TransferJob.id, ', ')), PRINTF('[%s]', GROUP_CONCAT(TransferJob.cid, ','))"
		"	FROM Confuga.TransferJob JOIN Confuga.StorageNode ON TransferJob.fsid = StorageNode.id"
		"	WHERE TransferJob.state = 'CREATED'"
		"	GROUP BY StorageNode.id"
		"	ORDER BY RANDOM()" /* to ensure no starvation, complete may result in a ROLLBACK that aborts this SELECT */
		";";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		confuga_sid_t sid = sqlite3_column_int64(stmt, 0);
		const char *hostport = (const char *)sqlite3_column_text(stmt, 1);
		const char *tjids = (const char *)sqlite3_column_text(stmt, 2);
		const char *cids = (const char *)sqlite3_column_text(stmt, 3);
		commit(C, sid, hostport, tjids, cids);
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

static int lookuptjid (confuga *C, confuga_sid_t sid, chirp_jobid_t cid, chirp_jobid_t *id)
{
	static const char SQL[] =
		"SELECT id"
		"	FROM Confuga.TransferJob"
		"	WHERE cid = ?1 AND fsid = ?2"
		";"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_int64(stmt, 1, cid));
	sqlcatch(sqlite3_bind_int64(stmt, 2, sid));
	rc = sqlite3_step(stmt);
	if (rc == SQLITE_ROW) {
		*id = sqlite3_column_int64(stmt, 0);
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	} else if (rc == SQLITE_DONE) {
		*id = -1;
	} else {
		sqlcatch(rc);
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	return rc;
}

static int waitall (confuga *C, confuga_sid_t fsid, const char *fhostport)
{
	static const char SQL[] =
		"BEGIN TRANSACTION;"
		"UPDATE Confuga.TransferJob"
		"	SET state = 'WAITED',"
		"		error = ?2,"
		"		exit_code = ?3,"
		"		exit_signal = ?4,"
		"		exit_status = ?5,"
		"		status = ?6"
		"	WHERE id = ?1 AND state = 'COMMITTED'"
		";"
		"END TRANSACTION;"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	char *status = NULL;
	unsigned i;
	json_value *J = NULL;

	debug(D_DEBUG, "waiting for transfer jobs on " CONFUGA_SID_DEBFMT, fsid);
	CATCHUNIX(chirp_reli_job_wait(fhostport, 0, 0, &status, STOPTIME));
	assert(status);
	assert(strlen(status) == (size_t)rc);
	debug(D_DEBUG, "status = `%s'", status);

	J = json_parse(status, strlen(status));
	if (J == NULL)
		CATCH(EINVAL);
	assert(jistype(J, json_array));

	if (J->u.array.length == 0) {
		rc = 0;
		goto out;
	}

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	for (i = 0; i < J->u.array.length; i++) {
		json_value *job = J->u.array.values[i];
		assert(jistype(job, json_object));
		chirp_jobid_t id;

		json_value *cid = jsonA_getname(job, "id", json_integer);
		CATCH(lookuptjid(C, fsid, cid->u.integer, &id));
		if (id == -1) {
			continue; /* not a transfer job */
		}
		debug(D_CONFUGA, "transfer job %" PRICHIRP_JOBID_T " job finished", id);
		C->operations++;

		json_value *error = jsonA_getname(job, "error", json_string);
		json_value *exit_code = jsonA_getname(job, "exit_code", json_integer);
		json_value *exit_signal = jsonA_getname(job, "exit_signal", json_string);
		json_value *exit_status = jsonA_getname(job, "exit_status", json_string);
		json_value *status = jsonA_getname(job, "status", json_string);

		sqlcatch(sqlite3_reset(stmt));
		sqlcatch(sqlite3_clear_bindings(stmt));
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
		if (!sqlite3_changes(db)) {
			debug(D_DEBUG, "transfer job %" PRICHIRP_JOBID_T " job not set to WAITED!", id);
		}
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	json_value_free(J);
	sqlite3_finalize(stmt);
	sqlend(db);
	return rc;
}

static int transfer_wait (confuga *C)
{
	static const char SQL[] =
		"SELECT DISTINCT fsn.id, fsn.hostport"
		"	FROM"
		"		Confuga.TransferJob"
		"		JOIN Confuga.StorageNode AS fsn ON TransferJob.fsid = fsn.id"
		"	WHERE TransferJob.state = 'COMMITTED'"
		"	ORDER BY RANDOM()" /* to ensure no starvation, complete may result in a ROLLBACK that aborts this SELECT */
		";";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		confuga_sid_t fsid = sqlite3_column_int64(stmt, 0);
		const char *fhostport = (const char *)sqlite3_column_text(stmt, 1);
		waitall(C, fsid, fhostport);
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int reap (confuga *C, confuga_sid_t sid, const char *hostport, const char *tjids, const char *cids)
{
	static const char SQL[] =
		"UPDATE Confuga.TransferJob"
		"	SET"
		"		state = 'REAPED',"
		"		time_commit = strftime('%s', 'now')"
		"	WHERE id = ? AND state = 'WAITED'"
		";"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	json_value *J = NULL;
	unsigned i;

	J = json_parse(tjids, strlen(tjids));
	assert(J && jistype(J, json_array));

	debug(D_DEBUG, "transfer jobs %s: reaping", tjids);

	CATCHUNIX(chirp_reli_job_reap(hostport, cids, STOPTIME));

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	for (i = 0; i < J->u.array.length; i++) {
		json_value *id = J->u.array.values[i];
		assert(jistype(id, json_integer));
		sqlcatch(sqlite3_reset(stmt));
		sqlcatch(sqlite3_bind_int64(stmt, 1, id->u.integer));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	json_value_free(J);
	sqlite3_finalize(stmt);
	return rc;
}

static int transfer_reap (confuga *C)
{
	static const char SQL[] =
		"SELECT StorageNode.id, StorageNode.hostport, PRINTF('[%s]', GROUP_CONCAT(TransferJob.id, ', ')), PRINTF('[%s]', GROUP_CONCAT(TransferJob.cid, ','))"
		"	FROM Confuga.TransferJob JOIN Confuga.StorageNode ON TransferJob.fsid = StorageNode.id"
		"	WHERE TransferJob.state = 'WAITED'"
		"	GROUP BY StorageNode.id"
		"	ORDER BY RANDOM()" /* to ensure no starvation, complete may result in a ROLLBACK that aborts this SELECT */
		";";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		confuga_sid_t sid = sqlite3_column_int64(stmt, 0);
		const char *hostport = (const char *)sqlite3_column_text(stmt, 1);
		const char *tjids = (const char *)sqlite3_column_text(stmt, 2);
		const char *cids = (const char *)sqlite3_column_text(stmt, 3);
		reap(C, sid, hostport, tjids, cids);
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

static int complete (confuga *C, chirp_jobid_t id, const char *hostport, const char *open, const char *file)
{
	static const char SQL[] =
		"BEGIN TRANSACTION;"
		"INSERT OR IGNORE INTO Confuga.Replica (fid, sid)"
		"	SELECT TransferJob.fid, TransferJob.tsid"
		"	FROM Confuga.TransferJob"
		"	WHERE TransferJob.id = ?;"
		"UPDATE Confuga.TransferJob"
		"	SET"
		"		state = 'COMPLETED',"
		"		progress = (SELECT size FROM Confuga.File WHERE File.id = TransferJob.id),"
		"		time_complete = strftime('%s', 'now')"
		"	WHERE id = ?;"
		"END TRANSACTION;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	debug(D_DEBUG, "transfer job %" PRICHIRP_JOBID_T ": completing", id);

	rc = chirp_reli_rename(hostport, open, file, STOPTIME);
	if (rc == -1 && errno == ENOENT) {
		/* previous rename succeeded? but we were not able to update the SQL db... */
		if (chirp_reli_access(hostport, file, R_OK, STOPTIME) == -1)
			CATCH(ENOENT); /* rename errno */
	} else {
		CATCHUNIX(rc);
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

static int transfer_complete (confuga *C)
{
	static const char SQL[] =
		"UPDATE Confuga.TransferJob"
		"	SET state = 'ERRORED'"
		"	WHERE state = 'REAPED' AND NOT (status = 'FINISHED' AND exit_status = 'EXITED' AND exit_code = 0);"
		"SELECT TransferJob.id, StorageNode.hostport, TransferJob.open, PRINTF('%s/file/%s', StorageNode.root, UPPER(HEX(TransferJob.fid)))"
		"	FROM Confuga.TransferJob JOIN Confuga.StorageNode ON TransferJob.tsid = StorageNode.id"
		"	WHERE TransferJob.state = 'REAPED'"
		"	ORDER BY RANDOM()" /* to ensure no starvation, complete may result in a ROLLBACK that aborts this SELECT */
		";";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		chirp_jobid_t id = sqlite3_column_int64(stmt, 0);
		const char *hostport = (const char *)sqlite3_column_text(stmt, 1);
		const char *open = (const char *)sqlite3_column_text(stmt, 2);
		const char *file = (const char *)sqlite3_column_text(stmt, 3);
		CATCHJOB(complete(C, id, hostport, open, file));
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

static int transfer_stats (confuga *C)
{
	static const char SQL[] =
		"SELECT PRINTF('%s (%d)', TransferJob.state, COUNT(TransferJob.id))"
		"	FROM TransferJob"
		"	GROUP BY TransferJob.state"
		"	ORDER BY TransferJob.state"
		";";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	buffer_t B[1];
	time_t now = time(NULL);

	buffer_init(B);

	if (now < C->transfer_stats+30) {
		rc = 0;
		goto out;
	}
	C->transfer_stats = now;

	buffer_putliteral(B, "TJ: ");

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		const char *state = (const char *)sqlite3_column_text(stmt, 0);
		buffer_putfstring(B, "%s; ", state);
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	debug(D_DEBUG, "%s", buffer_tostring(B));

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	buffer_free(B);
	return rc;
}

static int progress (confuga *C, chirp_jobid_t id, const char *thostport, const char *topen)
{
	static const char SQL[] =
		"UPDATE Confuga.TransferJob"
		"   SET progress = ?"
		"   WHERE id = ?"
		";"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	struct chirp_stat info;

	debug(D_DEBUG, "transfer job %" PRICHIRP_JOBID_T ": checking progress...", id);
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	CATCHUNIXIGNORE(chirp_reli_stat(thostport, topen, &info, time(NULL)+2), ENOENT);
	if (rc == 0) {
		debug(D_DEBUG, "... is %" PRICONFUGA_OFF_T, (confuga_off_t)info.cst_size);
		sqlcatch(sqlite3_bind_int64(stmt, 1, info.cst_size));
		sqlcatch(sqlite3_bind_int64(stmt, 2, id));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	} else if (rc == -1 && errno == ENOENT) {
		debug(D_DEBUG, "... not created yet");
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int transfer_progress (confuga *C)
{
	static const char SQL[] =
		"SELECT TransferJob.id, tsn.hostport, TransferJob.open"
		"	FROM TransferJob JOIN Confuga.StorageNode AS tsn ON TransferJob.tsid = tsn.id"
		"	WHERE TransferJob.state = 'COMMITTED'"
			/* Use ORDER BY so that we hit the same storage node repeatedly so we don't lose a connection. */
			/* TODO Even better would by a batch operation like getlongdir on /open and go through results. */
		"	ORDER BY tsn.id"
		";"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		chirp_jobid_t id = sqlite3_column_int64(stmt, 0);
		const char *thostport = (const char *)sqlite3_column_text(stmt, 1);
		const char *topen = (const char *)sqlite3_column_text(stmt, 2);
		CATCHJOB(progress(C, id, thostport, topen));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int unlinkthedead (confuga *C)
{
	static const char SQL[] =
		"BEGIN TRANSACTION;"
		/* Undo any intents to unlink a Replica where the Replica has been recreated. */
		"DELETE FROM Confuga.DeadReplica"
		"	WHERE EXISTS (SELECT NULL FROM Confuga.Replica WHERE DeadReplica.fid = Replica.fid AND DeadReplica.sid = Replica.sid)"
		";"
		/* unlink a DeadReplica only when the StorageNode is not executing a job! A job may create a new replica with the same RepID! */
		"SELECT DeadReplica.fid, DeadReplica.sid, StorageNodeAuthenticated.hostport, PRINTF('%s/file/%s', StorageNodeAuthenticated.root, UPPER(HEX(DeadReplica.fid)))"
		"	FROM Confuga.DeadReplica"
		"		JOIN Confuga.StorageNodeAuthenticated ON DeadReplica.sid = StorageNodeAuthenticated.id"
		"		LEFT OUTER JOIN ConfugaJobExecuting ON StorageNodeAuthenticated.id = ConfugaJobExecuting.sid"
		"	WHERE ConfugaJobExecuting.id IS NULL"
		"	ORDER BY RANDOM()"
		"	LIMIT 1"
		";"
		"DELETE FROM Confuga.DeadReplica"
		"	WHERE fid = ?1 AND sid = ?2"
		";"
		"END TRANSACTION;"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	sqlite3_stmt *select = NULL;
	sqlite3_stmt *delete = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &select, &current));
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &delete, &current));

	while ((rc = sqlite3_step(select)) == SQLITE_ROW) {
		confuga_fid_t fid;
		confuga_sid_t sid;
		const char *hostport;
		const char *path;
		CATCH(confugaF_set(C, &fid, sqlite3_column_blob(select, 0)));
		sid = sqlite3_column_int64(select, 1);
		hostport = (const char *)sqlite3_column_text(select, 2);
		path = (const char *)sqlite3_column_text(select, 3);

		debug(D_DEBUG, "unlinking dead replica fid = " CONFUGA_FID_PRIFMT " sid = " CONFUGA_SID_PRIFMT, CONFUGA_FID_PRIARGS(fid), sid);
		CATCHUNIX(chirp_reli_unlink(hostport, path, STOPTIME));

		sqlcatch(sqlite3_bind_blob(delete, 1, confugaF_id(fid), confugaF_size(fid), SQLITE_STATIC));
		sqlcatch(sqlite3_bind_int64(delete, 2, sid));
		sqlcatchcode(sqlite3_step(delete), SQLITE_DONE);

		sqlcatch(sqlite3_reset(select));
		sqlcatch(sqlite3_reset(delete));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(select); select = NULL);
	sqlcatch(sqlite3_finalize(delete); delete = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlite3_finalize(select);
	sqlite3_finalize(delete);
	sqlend(db);
	return rc;
}

CONFUGA_IAPI int confugaR_manager (confuga *C)
{
	int rc;

	schedule_replication(C);

	transfer_stats(C);
	transfer_create(C);
	transfer_commit(C);
	transfer_wait(C);
	transfer_reap(C);
	transfer_complete(C);
	transfer_progress(C);

	unlinkthedead(C);

	(void)do_upkeep;

	rc = 0;
	goto out;
out:
	return rc;
}

/* vim: set noexpandtab tabstop=4: */
