/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "confuga_fs.h"

#include "chirp_reli.h"
#include "chirp_sqlite.h"
#include "chirp_types.h"

#include "catalog_query.h"
#include "catch.h"
#include "copy_stream.h"
#include "debug.h"
#include "jx.h"
#include "pattern.h"
#include "random.h"
#include "sha1.h"

#include <sys/stat.h>

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define STOPTIME_SHORT  (time(NULL)+5)
#define TICKET_DURATION (12*60*60)

CONFUGA_IAPI int confugaS_catalog (confuga *C, const char *catalog)
{
	int rc;

	if (catalog) {
		C->catalog_hosts = catalog;
	} else {
		C->catalog_hosts = CATALOG_HOST;
	}

	rc = 0;
	goto out;
out:
	return rc;
}

CONFUGA_IAPI int confugaS_catalog_sync (confuga *C)
{
	static const char SQL[] =
		"BEGIN IMMEDIATE TRANSACTION;"
		"UPDATE Confuga.StorageNode"
		"    SET"
				/* Confuga */
		"		hostport = ?,"
				/* Catalog */
		"		address = ?,"
		"		avail = ?,"
		"		backend = ?,"
		"		bytes_read = ?,"
		"		bytes_written = ?,"
		"		cpu = ?,"
		"		cpus = ?,"
		"		lastheardfrom = ?,"
		"		load1 = ?,"
		"		load5 = ?,"
		"		load15 = ?,"
		"		memory_avail = ?,"
		"		memory_total = ?,"
		"		minfree = ?,"
		"		name = ?,"
		"		opsys = ?,"
		"		opsysversion = ?,"
		"		owner = ?,"
		"		port = ?,"
		"		starttime = ?,"
		"		total = ?,"
		"		total_ops = ?,"
		"		url = ?,"
		"		uuid = ?,"
		"		version = ?"
		"    WHERE"
		"		uuid = ?"
				/* backwards compatibility... */
		"		OR uuid IS NULL AND (hostport = ? || ':' || ? OR hostport = ? || ':' || ? OR 'chirp://' || hostport = ?)"
		";"
		"END TRANSACTION;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	time_t stoptime = time(NULL)+15;
	struct catalog_query *Q = NULL;
	struct jx *j = NULL;
	char *host = NULL;
	char *port = NULL;

	debug(D_DEBUG|D_CONFUGA, "syncing with catalog");

	Q = catalog_query_create(C->catalog_hosts, 0, stoptime);
	CATCH(Q == NULL ? errno : 0);

	/* FIXME sqlcatch is silent about EAGAIN, what should we do? */

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((j = catalog_query_read(Q, stoptime))) {
		const char *type = jx_lookup_string(j, "type");
		if (type && strcmp(type, "chirp") == 0) {
			char hostport[256+8] = "";
			int n = 1;
			const char *jaddr = jx_lookup_string(j, "address");
			const char *jname = jx_lookup_string(j, "name");
			int jport = jx_lookup_integer(j, "port");
			const char *jurl = jx_lookup_string(j, "url");
			const char *juuid = jx_lookup_string(j, "uuid");

			/* UPDATE */

			/* Confuga fields */
			if (jurl && pattern_match(jurl, "^chirp://([^:]+)%:(%d+)", &host, &port) >= 0) {
				CATCHUNIX(snprintf(hostport, sizeof hostport, "%s:%s", host, port));
				host = realloc(host, 0);
				port = realloc(port, 0);
			} else if (jname && jport) {
				CATCHUNIX(snprintf(hostport, sizeof hostport, "%s:%d", jname, jport));
			} else if (jaddr && jport) {
				CATCHUNIX(snprintf(hostport, sizeof hostport, "%s:%d", jaddr, jport));
			} else strcpy(hostport, "");
			sqlcatch(sqlite3_bind_text(stmt, n++, hostport, -1, SQLITE_TRANSIENT));

			/* Catalog fields */
			sqlcatch(sqlite3_bind_text(stmt, n++, jaddr, -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "avail")));
			sqlcatch(sqlite3_bind_text(stmt, n++, jx_lookup_string(j, "backend"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "bytes_read")));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "bytes_written")));
			sqlcatch(sqlite3_bind_text(stmt, n++, jx_lookup_string(j, "cpu"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "cpus")));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "lastheardfrom")));
			sqlcatch(sqlite3_bind_double(stmt, n++, jx_lookup_double(j, "load1")));
			sqlcatch(sqlite3_bind_double(stmt, n++, jx_lookup_double(j, "load5")));
			sqlcatch(sqlite3_bind_double(stmt, n++, jx_lookup_double(j, "load15")));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "memory_avail")));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "memory_total")));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "minfree")));
			sqlcatch(sqlite3_bind_text(stmt, n++, jname, -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, jx_lookup_string(j, "opsys"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, jx_lookup_string(j, "opsysversion"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, jx_lookup_string(j, "owner"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jport));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "starttime")));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "total")));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "total_ops")));
			sqlcatch(sqlite3_bind_text(stmt, n++, jurl, -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, juuid, -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, jx_lookup_string(j, "version"), -1, SQLITE_TRANSIENT));

			/* WHERE hostport = ? */
			sqlcatch(sqlite3_bind_text(stmt, n++, juuid, -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, jname, -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jport));
			sqlcatch(sqlite3_bind_text(stmt, n++, jaddr, -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jport));
			sqlcatch(sqlite3_bind_text(stmt, n++, jurl, -1, SQLITE_TRANSIENT));

			sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
			C->operations++;
			sqlcatch(sqlite3_reset(stmt));
			sqlcatch(sqlite3_clear_bindings(stmt));
		}
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	if (Q)
		catalog_query_delete(Q);
	sqlite3_finalize(stmt);
	sqlend(db);
	free(host);
	free(port);
	return rc;
}

static int sn_ticket (confuga *C)
{
	static const char SQL[] =
		"SELECT id, hostport, root"
		"	FROM Confuga.StorageNodeActive"
		"	WHERE hostport IS NOT NULL AND (ticket IS NULL OR ticket != ? OR time_ticket < strftime('%s', 'now', '-8 hours'))"
		"	ORDER BY RANDOM()"
		"	LIMIT 1"
		";"
		"UPDATE Confuga.StorageNode"
		"	SET"
		"		ticket = ?2,"
		"		time_ticket = (strftime('%s', 'now'))"
		"	WHERE id = ?1"
		";"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	sqlite3_stmt *select = NULL;
	sqlite3_stmt *update = NULL;
	const char *current = SQL;
	FILE *stream = NULL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &select, &current));
	sqlcatch(sqlite3_bind_blob(select, 1, C->ticket, sizeof(C->ticket), SQLITE_STATIC));
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &update, &current));
	sqlcatch(sqlite3_bind_blob(update, 2, C->ticket, sizeof(C->ticket), SQLITE_STATIC));

	while ((rc = sqlite3_step(select)) == SQLITE_ROW) {
		confuga_sid_t sid = sqlite3_column_int64(select, 0);
		const char *hostport = (const char *)sqlite3_column_text(select, 1);
		const char *root = (const char *)sqlite3_column_text(select, 2);
		time_t stoptime = STOPTIME_SHORT;
		char ticket[PATH_MAX];
		char path[CHIRP_PATH_MAX];
		struct stat info;

		CATCHUNIX(snprintf(ticket, sizeof(ticket), "%s/ticket", C->root));
		CATCHUNIX(chirp_reli_ticket_register(hostport, ticket, "self", TICKET_DURATION, stoptime));

		/* The list permission is necessary because chirp_fs_local_scheduler.c:geturl does a stat. */
		CATCHUNIX(snprintf(path, sizeof(path), "%s/file", root));
		CATCHUNIX(chirp_reli_ticket_modify(hostport, ticket, path, "lr", stoptime));

		/* Add write permission because a putfile may need retried. */
		CATCHUNIX(snprintf(path, sizeof(path), "%s/open", root));
		CATCHUNIX(chirp_reli_ticket_modify(hostport, ticket, path, "pw", stoptime));

		CATCHUNIX(snprintf(path, sizeof(path), "%s/ticket", root));
		stream = fopen(ticket, "r");
		CATCHUNIX(stream ? 0 : -1);
		CATCHUNIX(fstat(fileno(stream), &info));
		CATCHUNIX(chirp_reli_putfile(hostport, path, stream, S_IRUSR, info.st_size, stoptime));
		CATCHUNIX(fclose(stream));
		stream = NULL;

		sqlcatch(sqlite3_bind_int64(update, 1, sid));
		sqlcatchcode(sqlite3_step(update), SQLITE_DONE);
		debug(D_CONFUGA, "Storage Node " CONFUGA_SID_DEBFMT " (%s/%s) ticket registered", sid, hostport, root);
		C->operations += 1;
		sqlcatch(sqlite3_reset(update));
		sqlcatch(sqlite3_reset(select));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(select); select = NULL);
	sqlcatch(sqlite3_finalize(update); update = NULL);

	rc = 0;
	goto out;
out:
	if (stream)
		fclose(stream);
	sqlite3_finalize(stmt);
	sqlite3_finalize(select);
	sqlite3_finalize(update);
	return rc;
}

static int sn_build (confuga *C)
{
	static const confuga_fid_t empty = {CONFUGA_FID_EMPTY};
	static const char SQL[] =
		"SELECT id, hostport, root"
		"	FROM Confuga.StorageNodeAuthenticated"
		"	WHERE hostport IS NOT NULL AND state = 'BUILDING'"
		"	ORDER BY RANDOM()"
		"	LIMIT 1"
		";"
		"UPDATE Confuga.StorageNode"
		"	SET state = 'ONLINE'"
		"	WHERE id = ?1"
		";"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	sqlite3_stmt *select = NULL;
	sqlite3_stmt *update = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &select, &current));
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &update, &current));

	while ((rc = sqlite3_step(select)) == SQLITE_ROW) {
		confuga_sid_t sid = sqlite3_column_int64(select, 0);
		const char *hostport = (const char *)sqlite3_column_text(select, 1);
		const char *root = (const char *)sqlite3_column_text(select, 2);
		char template[CHIRP_PATH_MAX];
		time_t stoptime = STOPTIME_SHORT;
		char whoami[128];

		debug(D_CONFUGA, "building %s/%s", hostport, root);

		CATCHUNIX(chirp_reli_whoami(hostport, whoami, sizeof(whoami), stoptime));

		CATCHUNIXIGNORE(chirp_reli_mkdir_recursive(hostport, root, S_IRWXU, stoptime), EEXIST);

		CATCHUNIX(snprintf(template, sizeof(template), "%s/file", root));
		CATCHUNIXIGNORE(chirp_reli_mkdir(hostport, template, S_IRWXU, stoptime), EEXIST);
		CATCHUNIX(chirp_reli_setacl(hostport, template, whoami, "rwldpa", stoptime));

		CATCHUNIX(snprintf(template, sizeof(template), "%s/file/" CONFUGA_FID_PRIFMT, root, CONFUGA_FID_PRIARGS(empty)));
		CATCHUNIXIGNORE(chirp_reli_putfile_buffer(hostport, template, "", S_IRUSR, 0, stoptime), EEXIST);
		CATCH(confugaR_register(C, empty, 0, sid));

		CATCHUNIX(snprintf(template, sizeof(template), "%s/open", root));
		CATCHUNIXIGNORE(chirp_reli_mkdir(hostport, template, S_IRWXU, stoptime), EEXIST);
		CATCHUNIX(chirp_reli_setacl(hostport, template, whoami, "rwldpa", stoptime));

		CATCHUNIX(snprintf(template, sizeof(template), "%s/tickets", root));
		CATCHUNIXIGNORE(chirp_reli_mkdir(hostport, template, S_IRWXU, stoptime), EEXIST);
		CATCHUNIX(chirp_reli_setacl(hostport, template, whoami, "rwldpa", stoptime));

		sqlcatch(sqlite3_bind_int64(update, 1, sid));
		sqlcatchcode(sqlite3_step(update), SQLITE_DONE);
		debug(D_CONFUGA, "Storage Node " CONFUGA_SID_DEBFMT " (%s/%s) ONLINE", sid, hostport, root);
		C->operations += 1;
		sqlcatch(sqlite3_reset(update));
		sqlcatch(sqlite3_reset(select));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(select); select = NULL);
	sqlcatch(sqlite3_finalize(update); update = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlite3_finalize(select);
	sqlite3_finalize(update);
	return rc;
}

static int sn_set_password (confuga *C)
{
	static const char SQL[] =
		"SELECT id, hostport, root"
		"	FROM Confuga.StorageNodeAlive"
		"	WHERE password IS NULL"
		"	ORDER BY RANDOM()"
		"	LIMIT 1"
		";"
		"UPDATE Confuga.StorageNode"
		"	SET password = ?2"
		"	WHERE id = ?1"
		";"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	sqlite3_stmt *select = NULL;
	sqlite3_stmt *update = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &select, &current));
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &update, &current));

	while ((rc = sqlite3_step(select)) == SQLITE_ROW) {
		confuga_sid_t sid = sqlite3_column_int64(select, 0);
		const char *hostport = (const char *)sqlite3_column_text(select, 1);
		const char *root = (const char *)sqlite3_column_text(select, 2);
		time_t stoptime = STOPTIME_SHORT;
		char path[CHIRP_PATH_MAX];
		unsigned char password[20];
		unsigned char digest[SHA1_DIGEST_LENGTH];

		random_array(password, sizeof password);
		sha1_buffer(password, sizeof password, digest);

		CATCHUNIXIGNORE(chirp_reli_mkdir_recursive(hostport, root, S_IRWXU, stoptime), EEXIST);
		CATCHUNIX(snprintf(path, sizeof(path), "%s/password", root));
		rc = UNIXRC(chirp_reli_putfile_buffer(hostport, path, password, S_IRUSR, sizeof password, stoptime));
		if (rc == 0) {
		} else {
			CATCH(rc);
		}

		sqlcatch(sqlite3_bind_int64(update, 1, sid));
		sqlcatch(sqlite3_bind_blob(update, 2, digest, sizeof digest, SQLITE_STATIC));
		sqlcatchcode(sqlite3_step(update), SQLITE_DONE);
		debug(D_CONFUGA, "Storage Node " CONFUGA_SID_DEBFMT " (%s/%s) password set", sid, hostport, root);
		C->operations += 1;
		sqlcatch(sqlite3_reset(update));
		sqlcatch(sqlite3_reset(select));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(select); select = NULL);
	sqlcatch(sqlite3_finalize(update); update = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlite3_finalize(select);
	sqlite3_finalize(update);
	return rc;
}

/* Ideally this would be done anytime the Confuga HN (re)connects to a storage node. */
static int sn_authenticate (confuga *C)
{
	static const char SQL[] =
		"SELECT id, hostport, root, password"
		"	FROM Confuga.StorageNodeAlive"
		"	WHERE password IS NOT NULL AND (NOT authenticated OR time_authenticated < strftime('%s', 'now', '-15 minutes'))"
		"	ORDER BY RANDOM()"
		";"
		"UPDATE Confuga.StorageNode"
		"	SET"
		"		authenticated = 1,"
		"		time_authenticated = (strftime('%s', 'now'))"
		"	WHERE id = ?1"
		";"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	sqlite3_stmt *select = NULL;
	sqlite3_stmt *update = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &select, &current));
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &update, &current));

	while ((rc = sqlite3_step(select)) == SQLITE_ROW) {
		confuga_sid_t sid = sqlite3_column_int64(select, 0);
		const char *hostport = (const char *)sqlite3_column_text(select, 1);
		const char *root = (const char *)sqlite3_column_text(select, 2);
		const void *password = sqlite3_column_blob(select, 3);
		time_t stoptime = STOPTIME_SHORT;
		char path[CHIRP_PATH_MAX];
		unsigned char digest[CHIRP_DIGEST_MAX];

		assert(sqlite3_column_bytes(select, 3) == SHA1_DIGEST_LENGTH);
		CATCHUNIX(snprintf(path, sizeof(path), "%s/password", root));
		rc = chirp_reli_hash(hostport, path, "sha1", digest, stoptime);
		if (rc >= 0) {
			if (memcmp(password, digest, SHA1_DIGEST_LENGTH) == 0) {
				sqlcatch(sqlite3_bind_int64(update, 1, sid));
				sqlcatchcode(sqlite3_step(update), SQLITE_DONE);
				sqlcatch(sqlite3_reset(update));
				debug(D_CONFUGA, "Storage Node " CONFUGA_SID_DEBFMT " (%s/%s) password correct", sid, hostport, root);
				C->operations += 1;
			} else {
				debug(D_CONFUGA, "Storage Node " CONFUGA_SID_DEBFMT " (%s/%s) password failure", sid, hostport, root);
				/* FIXME what to do? */
			}
		} else {
			CATCH(errno);
			/* FIXME what to do? */
		}
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(select); select = NULL);
	sqlcatch(sqlite3_finalize(update); update = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlite3_finalize(update);
	sqlite3_finalize(select);
	return rc;
}

static int sn_removing (confuga *C)
{
	static const char SQL[] =
		"WITH"
		"	DepartingStorageNode AS ("
		"		SELECT Confuga.StorageNodeAuthenticated.id"
		"			FROM"
		"				Confuga.StorageNodeAuthenticated"
		"				LEFT OUTER JOIN Confuga.ActiveTransfers AS fat ON StorageNodeAuthenticated.id = fat.fsid"
		"				LEFT OUTER JOIN Confuga.ActiveTransfers AS tat ON StorageNodeAuthenticated.id = tat.tsid"
		"				LEFT OUTER JOIN ConfugaJobAllocated ON StorageNodeAuthenticated.id = ConfugaJobAllocated.sid"
		"			WHERE StorageNodeAuthenticated.state = 'REMOVING' AND fat.fsid IS NULL AND tat.tsid IS NULL AND ConfugaJobAllocated.sid IS NULL"
		"	),"
		"	DegradedFile AS ("
		"		SELECT File.id"
		"			FROM"
		"				Confuga.File"
		"				LEFT OUTER JOIN (Confuga.Replica JOIN Confuga.StorageNodeActive ON Replica.sid = StorageNodeActive.id) ON File.id = Replica.fid"
		"			GROUP BY File.id"
		"			HAVING COUNT(Replica.sid) < MIN(3, File.minimum_replicas)"
		"	)"
		"SELECT DepartingStorageNode.id, Replica.fid"
		"	FROM"
		"		DepartingStorageNode JOIN Confuga.Replica ON DepartingStorageNode.id = Replica.sid"
		"	WHERE Replica.fid NOT IN (SELECT DegradedFile.id FROM DegradedFile)"
		"	ORDER BY RANDOM()"
		";"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		confuga_sid_t id = sqlite3_column_int64(stmt, 0);
		confuga_fid_t fid;
		CATCH(confugaF_set(C, &fid, sqlite3_column_blob(stmt, 1)));
		CATCH(confugaR_delete(C, id, fid));
		C->operations++;
	}
	sqlcatchcode(rc, SQLITE_DONE);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int sn_remove (confuga *C)
{
	static const char SQL[] =
		"BEGIN TRANSACTION;"
		"SELECT StorageNode.id, StorageNode.hostport, StorageNode.root"
		"	FROM"
		"		Confuga.StorageNode"
		"		LEFT OUTER JOIN Confuga.Replica ON StorageNode.id = Replica.sid"
		"		LEFT OUTER JOIN Confuga.DeadReplica ON StorageNode.id = DeadReplica.sid"
		"		LEFT OUTER JOIN Confuga.ActiveTransfers AS fat ON StorageNode.id = fat.fsid"
		"		LEFT OUTER JOIN Confuga.ActiveTransfers AS tat ON StorageNode.id = tat.tsid"
		"		LEFT OUTER JOIN ConfugaJobAllocated ON StorageNode.id = ConfugaJobAllocated.sid"
		"	WHERE StorageNode.state = 'REMOVING' AND Replica.sid IS NULL AND DeadReplica.sid IS NULL AND fat.fsid IS NULL AND tat.tsid IS NULL AND ConfugaJobAllocated.sid IS NULL"
		"	ORDER BY RANDOM()"
		";"
		"DELETE FROM Confuga.TransferJob"
		"	WHERE (fsid = ?1 OR tsid = ?1) AND NOT EXISTS (SELECT id FROM Confuga.ActiveTransfers WHERE fsid = ?1 OR tsid = ?1);"
		"DELETE FROM Confuga.StorageNode"
		"	WHERE id = ?;"
		"END TRANSACTION;"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	sqlite3_stmt *select = NULL;
	sqlite3_stmt *delete1 = NULL;
	sqlite3_stmt *delete2 = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &select, &current));
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &delete1, &current));
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &delete2, &current));

	while ((rc = sqlite3_step(select)) == SQLITE_ROW) {
		confuga_sid_t id = sqlite3_column_int64(select, 0);
		const char *hostport = (const char *)sqlite3_column_text(select, 1);
		const char *root = (const char *)sqlite3_column_text(select, 2);
		sqlcatch(sqlite3_bind_int64(delete1, 1, id));
		sqlcatchcode(sqlite3_step(delete1), SQLITE_DONE);
		sqlcatch(sqlite3_bind_int64(delete2, 1, id));
		sqlcatchcode(sqlite3_step(delete2), SQLITE_DONE);
		debug(D_CONFUGA, "Storage Node " CONFUGA_SID_DEBFMT " (%s/%s) removed from cluster", id, hostport, root);
		C->operations++;
	}
	sqlcatchcode(rc, SQLITE_DONE);

	sqlcatch(sqlite3_finalize(delete1); delete1 = NULL);
	sqlcatch(sqlite3_finalize(delete2); delete2 = NULL);
	sqlcatch(sqlite3_finalize(select); select = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlite3_finalize(delete2);
	sqlite3_finalize(delete1);
	sqlite3_finalize(select);
	sqlend(db);
	return rc;
}

CONFUGA_IAPI int confugaS_manager (confuga *C)
{
	sn_build(C);
	sn_ticket(C);
	sn_set_password(C);
	sn_authenticate(C);
	sn_removing(C);
	sn_remove(C);
	return 0;
}

static int addbyaddr (confuga *C, const char *address, const char *root, const char *password)
{
	static const char SQL[] =
		"INSERT INTO Confuga.StorageNode (hostport, root, password, state)"
		"	SELECT ?1, ?2, ?3, 'BUILDING'"
		"		WHERE NOT EXISTS (SELECT id FROM Confuga.StorageNode WHERE hostport = ?1)"
		";"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	root = !root || strlen(root) == 0 ? CONFUGA_SN_ROOT_DEFAULT : root;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_text(stmt, 1, address, -1, SQLITE_STATIC));
	sqlcatch(sqlite3_bind_text(stmt, 2, root, -1, SQLITE_STATIC));
	if (password) {
		unsigned char digest[SHA1_DIGEST_LENGTH];
		sha1_buffer(password, strlen(password), digest);
		sqlcatch(sqlite3_bind_blob(stmt, 3, digest, sizeof digest, SQLITE_TRANSIENT));
	}
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	if (sqlite3_changes(db) == 1) {
		debug(D_CONFUGA, "Storage Node " CONFUGA_SID_DEBFMT " (%s/%s) added to cluster", (confuga_sid_t)sqlite3_last_insert_rowid(db), address, root);
		C->operations += 1;
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

static int addbyuuid (confuga *C, const char *uuid, const char *root, const char *password)
{
	static const char SQL[] =
		"INSERT INTO Confuga.StorageNode (uuid, root, password)"
		"	VALUES (?, ?, ?)"
		";"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	root = !root || strlen(root) == 0 ? CONFUGA_SN_ROOT_DEFAULT : root;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_text(stmt, 1, uuid, -1, SQLITE_STATIC));
	sqlcatch(sqlite3_bind_text(stmt, 2, root, -1, SQLITE_STATIC));
	if (password) {
		unsigned char digest[SHA1_DIGEST_LENGTH];
		sha1_buffer(password, strlen(password), digest);
		sqlcatch(sqlite3_bind_blob(stmt, 3, digest, sizeof digest, SQLITE_TRANSIENT));
	}
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	if (sqlite3_changes(db) == 1) {
		debug(D_CONFUGA, "Storage Node " CONFUGA_SID_DEBFMT " (%s) added to cluster", (confuga_sid_t)sqlite3_last_insert_rowid(db), uuid);
		C->operations++;
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

CONFUGA_API int confuga_nodes (confuga *C, const char *nodes)
{
	int rc;
	FILE *file = NULL;
	char *node = NULL;
	const char *rest;
	char *hostport = NULL;
	char *root = NULL;
	size_t n;

	if (pattern_match(nodes, "^node:(.*)", &node) >= 0) {
		/* do nothing */
	} else if (pattern_match(nodes, "^file:(.*)", &node) >= 0) {
		file = fopen(node, "r");
		CATCHUNIX(file ? 0 : -1);
		CATCHUNIX(copy_stream_to_buffer(file, &node, NULL));
	} else CATCH(EINVAL);

	rest = node;
	while (pattern_match(rest, "^[%s,]*chirp://([^/,%s]+)([^,%s]*)()", &hostport, &root, &n) >= 0) {
		addbyaddr(C, hostport, root, NULL);
		rest += n;
		hostport = realloc(hostport, 0);
		root = realloc(root, 0);
		C->operations++;
	}

	rc = 0;
	goto out;
out:
	if (file)
		fclose(file);
	free(node);
	free(hostport);
	free(root);
	return rc;
}

CONFUGA_API int confuga_snadd (confuga *C, const char *id, const char *root, const char *password, int flag)
{
	int rc;
	int opmask = CONFUGA_SN_UUID | CONFUGA_SN_ADDR;

	if ((flag & opmask) == opmask || (flag & opmask) == 0)
		CATCH(EINVAL);

	if (flag & CONFUGA_SN_UUID) {
		CATCH(addbyuuid(C, id, root, password));
	} else if (flag & CONFUGA_SN_ADDR) {
		CATCH(addbyaddr(C, id, root, password));
	} else assert(0);

	rc = 0;
	goto out;
out:
	return rc;
}

CONFUGA_API int confuga_snrm (confuga *C, const char *id, int flag)
{
	static const char SQL[] =
		"BEGIN TRANSACTION;"
		"SELECT id, hostport, root"
		"	FROM Confuga.StorageNode"
		"	WHERE uuid = ? OR hostport = ?"
		";"
		"UPDATE Confuga.StorageNode"
		"	SET state = 'REMOVING'"
		"	WHERE id = ?"
		";"
		"END TRANSACTION;"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	sqlite3_stmt *select = NULL;
	sqlite3_stmt *update = NULL;
	const char *current = SQL;

	int opmask = CONFUGA_SN_UUID | CONFUGA_SN_ADDR;

	if ((flag & opmask) == opmask || (flag & opmask) == 0)
		CATCH(EINVAL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &select, &current));
	if (flag & CONFUGA_SN_UUID)
		sqlcatch(sqlite3_bind_text(select, 1, id, -1, SQLITE_STATIC));
	else if (flag & CONFUGA_SN_ADDR)
		sqlcatch(sqlite3_bind_text(select, 2, id, -1, SQLITE_STATIC));
	else assert(0);
	sqlcatchcode(sqlite3_step(select), SQLITE_ROW);
	confuga_sid_t sid = sqlite3_column_int64(select, 0);
	const char *hostport = (const char *)sqlite3_column_text(select, 1);
	const char *root = (const char *)sqlite3_column_text(select, 2);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &update, &current));
	sqlcatch(sqlite3_bind_int64(update, 1, sid));
	sqlcatchcode(sqlite3_step(update), SQLITE_DONE);
	if (sqlite3_changes(db) == 1) {
		debug(D_CONFUGA, "Storage Node " CONFUGA_SID_DEBFMT " (%s/%s) to be removed from cluster", sid, hostport, root);
		C->operations++;
	}

	sqlcatch(sqlite3_finalize(update); update = NULL);
	sqlcatch(sqlite3_finalize(select); select = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlite3_finalize(select);
	sqlite3_finalize(update);
	sqlend(db);
	return rc;
}

/* vim: set noexpandtab tabstop=8: */
