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
#include "jx.h"
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
		char *port;
		snprintf(C->catalog_host, sizeof(C->catalog_host), "%s", catalog);
		port = strchr(C->catalog_host, ':');
		if (port) {
			*port = '\0';
			port += 1;
			C->catalog_port = strtoul(port, NULL, 10);
		} else {
			C->catalog_port = CATALOG_PORT_DEFAULT;
		}
	} else {
		snprintf(C->catalog_host, sizeof(C->catalog_host), "%s", CATALOG_HOST_DEFAULT);
		C->catalog_port = CATALOG_PORT_DEFAULT;
	}

	rc = 0;
	goto out;
out:
	return rc;
}

CONFUGA_IAPI int confugaS_catalog_sync (confuga *C)
{
	static const char SQL[] =
		"SELECT COUNT(*) FROM Confuga.StorageNode WHERE strftime('%s', 'now', '-2 minutes') <= lastheardfrom;"
		"BEGIN IMMEDIATE TRANSACTION;"
		"UPDATE Confuga.StorageNode"
		"    SET address = ?, avail = ?, backend = ?, bytes_read = ?, bytes_written = ?, cpu = ?, cpus = ?, lastheardfrom = ?, load1 = ?, load5 = ?, load15 = ?, memory_avail = ?, memory_total = ?, minfree = ?, name = ?, opsys = ?, opsysversion = ?, owner = ?, port = ?, starttime = ?, total = ?, total_ops = ?, url = ?, version = ?"
		"    WHERE hostport = ? || ':' || ? OR"
		"          hostport = ? || ':' || ? OR"
		"          'chirp://' || hostport = ?;"
		"END TRANSACTION;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	time_t stoptime = time(NULL)+15;
	struct catalog_query *Q = NULL;
	struct jx *j = NULL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
	if (sqlite3_column_int(stmt, 0) > 0) {
		rc = 0;
		goto out;
	}
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	debug(D_DEBUG|D_CONFUGA, "syncing with catalog");

	Q = catalog_query_create(C->catalog_host, C->catalog_port, stoptime);
	CATCH(Q == NULL ? errno : 0);

	/* FIXME sqlcatch is silent about EAGAIN, what should we do? */

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((j = catalog_query_read(Q, stoptime))) {
		const char *type = jx_lookup_string(j, "type");
		if (type && strcmp(type, "chirp") == 0) {
			int n = 1;
			/* UPDATE */
			sqlcatch(sqlite3_bind_text(stmt, n++, jx_lookup_string(j, "address"), -1, SQLITE_TRANSIENT));
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
			sqlcatch(sqlite3_bind_text(stmt, n++, jx_lookup_string(j, "name"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, jx_lookup_string(j, "opsys"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, jx_lookup_string(j, "opsysversion"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, jx_lookup_string(j, "owner"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "port")));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "starttime")));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "total")));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "total_ops")));
			sqlcatch(sqlite3_bind_text(stmt, n++, jx_lookup_string(j, "url"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, jx_lookup_string(j, "version"), -1, SQLITE_TRANSIENT));
			/* WHERE hostport = ? */
			sqlcatch(sqlite3_bind_text(stmt, n++, jx_lookup_string(j, "name"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "port")));
			sqlcatch(sqlite3_bind_text(stmt, n++, jx_lookup_string(j, "address"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_int64(stmt, n++, jx_lookup_integer(j, "port")));
			sqlcatch(sqlite3_bind_text(stmt, n++, jx_lookup_string(j, "url"), -1, SQLITE_TRANSIENT));
			sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
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
	return rc;
}

static int sn_ticket (confuga *C, const struct confuga_host *host)
{
	static const char SQL[] =
		"UPDATE Confuga.StorageNode"
		"	SET ticket = ?2"
		"	WHERE hostport = ?1;"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;
	time_t stoptime = STOPTIME_SHORT;

	char ticket[PATH_MAX];
	char path[CHIRP_PATH_MAX];
	struct stat info;
	FILE *stream = NULL;

	CATCHUNIX(snprintf(ticket, sizeof(ticket), "%s/ticket", C->root));
	CATCHUNIX(chirp_reli_ticket_register(host->hostport, ticket, "self", TICKET_DURATION, stoptime));

	/* The list permission is necessary because chirp_fs_local_scheduler.c:geturl does a stat. */
	CATCHUNIX(snprintf(path, sizeof(path), "%s/file", host->root));
	CATCHUNIX(chirp_reli_ticket_modify(host->hostport, ticket, path, "lr", stoptime));

	/* Add write permission because a putfile may need retried. */
	CATCHUNIX(snprintf(path, sizeof(path), "%s/open", host->root));
	CATCHUNIX(chirp_reli_ticket_modify(host->hostport, ticket, path, "pw", stoptime));

	CATCHUNIX(snprintf(path, sizeof(path), "%s/ticket", host->root));
	stream = fopen(ticket, "r");
	CATCHUNIX(stream ? 0 : -1);
	CATCHUNIX(fstat(fileno(stream), &info));
	CATCHUNIX(chirp_reli_putfile(host->hostport, path, stream, S_IRUSR, info.st_size, stoptime));

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_text(stmt, 1, host->hostport, -1, SQLITE_STATIC));
	sqlcatch(sqlite3_bind_blob(stmt, 2, C->ticket, sizeof(C->ticket), SQLITE_STATIC));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	if (stream)
		fclose(stream);
	sqlite3_finalize(stmt);
	return rc;
}

static int sn_init (confuga *C, confuga_sid_t sid, const struct confuga_host *host)
{
	static const confuga_fid_t empty = {CONFUGA_FID_EMPTY};
	static const char SQL[] =
		"UPDATE Confuga.StorageNode"
		"	SET initialized = 1"
		"	WHERE hostport = ?1;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	char template[CHIRP_PATH_MAX];
	time_t stoptime = STOPTIME_SHORT;
	char whoami[128];

	debug(D_CONFUGA, "initializing %s/%s", host->hostport, host->root);

	CATCHUNIX(chirp_reli_whoami(host->hostport, whoami, sizeof(whoami), stoptime));

	CATCHUNIXIGNORE(chirp_reli_mkdir_recursive(host->hostport, host->root, S_IRWXU, stoptime), EEXIST);

	CATCHUNIX(snprintf(template, sizeof(template), "%s/file", host->root));
	CATCHUNIXIGNORE(chirp_reli_mkdir(host->hostport, template, S_IRWXU, stoptime), EEXIST);
	CATCHUNIX(chirp_reli_setacl(host->hostport, template, whoami, "rwldpa", stoptime));

	CATCHUNIX(snprintf(template, sizeof(template), "%s/file/" CONFUGA_FID_PRIFMT, host->root, CONFUGA_FID_PRIARGS(empty)));
	CATCHUNIXIGNORE(chirp_reli_putfile_buffer(host->hostport, template, "", S_IRUSR, 0, stoptime), EEXIST);
	CATCH(confugaR_register(C, empty, 0, sid));

	CATCHUNIX(snprintf(template, sizeof(template), "%s/open", host->root));
	CATCHUNIXIGNORE(chirp_reli_mkdir(host->hostport, template, S_IRWXU, stoptime), EEXIST);
	CATCHUNIX(chirp_reli_setacl(host->hostport, template, whoami, "rwldpa", stoptime));

	CATCHUNIX(snprintf(template, sizeof(template), "%s/tickets", host->root));
	CATCHUNIXIGNORE(chirp_reli_mkdir(host->hostport, template, S_IRWXU, stoptime), EEXIST);
	CATCHUNIX(chirp_reli_setacl(host->hostport, template, whoami, "rwldpa", stoptime));

	debug(D_DEBUG, "setting `%s' to initialized", host->hostport);
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_text(stmt, 1, host->hostport, -1, SQLITE_STATIC));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	debug(D_CONFUGA, "%s/%s initialized", host->hostport, host->root);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

CONFUGA_IAPI int confugaS_node_insert (confuga *C, const char *hostport, const char *root)
{
	static const char SQL[] =
		"INSERT OR IGNORE INTO Confuga.StorageNode (hostport, root)"
		"	VALUES (?1, ?2);";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	if (strlen(root) == 0)
		root = "/";

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_text(stmt, 1, hostport, -1, SQLITE_STATIC));
	sqlcatch(sqlite3_bind_text(stmt, 2, root, -1, SQLITE_STATIC));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	if (sqlite3_changes(db) == 1)
		debug(D_CONFUGA, "Storage Node " CONFUGA_SID_DEBFMT " (chirp://%s/%s) online", (confuga_sid_t)sqlite3_last_insert_rowid(db), hostport, root);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

CONFUGA_IAPI int confugaS_setup (confuga *C)
{
	static const char SQL[] =
		"DROP TABLE IF EXISTS ConfugaResults;"
		"CREATE TEMPORARY TABLE ConfugaResults AS" /* so we don't hold read locks */
		"	SELECT id, hostport, root, initialized, ticket"
		"		FROM Confuga.StorageNodeAlive;"
		"SELECT * FROM ConfugaResults;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		confuga_sid_t sid;
		struct confuga_host host;
		sid = sqlite3_column_int64(stmt, 0);
		CATCHUNIX(snprintf(host.hostport, sizeof(host.hostport), "%s", (const char *) sqlite3_column_text(stmt, 1)));
		CATCHUNIX(snprintf(host.root, sizeof(host.root), "%s", (const char *) sqlite3_column_text(stmt, 2)));
		if (!sqlite3_column_int(stmt, 3)) {
			sn_init(C, sid, &host);
		}
		const unsigned char *ticket = sqlite3_column_blob(stmt, 4);
		assert(sqlite3_column_bytes(stmt, 4) == 0 || sqlite3_column_bytes(stmt, 4) == sizeof(C->ticket));
		if (ticket == NULL || !(memcmp(ticket, C->ticket, sizeof(C->ticket)) == 0)) {
			sn_ticket(C, &host);
		}
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlite3_exec(db, "DROP TABLE IF EXISTS ConfugaResults;", NULL, NULL, NULL);
	return rc;
}

/* vim: set noexpandtab tabstop=4: */
