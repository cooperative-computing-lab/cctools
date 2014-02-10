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
#include "nvpair.h"
#include "sha1.h"

#include <sys/stat.h>

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
	struct nvpair *nv = NULL;

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
	while ((nv = catalog_query_read(Q, stoptime))) {
		const char *type = nvpair_lookup_string(nv, "type");
		if (type && strcmp(type, "chirp") == 0) {
			int n = 1;
			/* UPDATE */
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "address"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "avail"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "backend"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "bytes_read"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "bytes_written"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "cpu"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "cpus"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "lastheardfrom"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "load1"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "load5"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "load15"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "memory_avail"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "memory_total"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "minfree"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "name"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "opsys"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "opsysversion"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "owner"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "port"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "starttime"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "total"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "total_ops"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "url"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "version"), -1, SQLITE_TRANSIENT));
			/* WHERE hostport = ? */
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "name"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "port"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "address"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "port"), -1, SQLITE_TRANSIENT));
			sqlcatch(sqlite3_bind_text(stmt, n++, nvpair_lookup_string(nv, "url"), -1, SQLITE_TRANSIENT));
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

#define STOPTIME_SHORT  (time(NULL)+2)

static int sn_init (confuga *C, const struct confuga_host *host)
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

	char ticket[CHIRP_PATH_MAX];
	char template[CHIRP_PATH_MAX];
	FILE *stream = NULL;
	struct stat info;
	time_t stoptime = STOPTIME_SHORT;
	char whoami[128];

	debug(D_CONFUGA, "initializing %s/%s", host->hostport, host->root);

	CATCHUNIX(chirp_reli_whoami(host->hostport, whoami, sizeof(whoami), stoptime));

	CATCHUNIXIGNORE(chirp_reli_mkdir_recursive(host->hostport, host->root, S_IRWXU, stoptime), EEXIST);

	CATCHUNIX(snprintf(ticket, sizeof(ticket), "%s/ticket", C->root));
	CATCHUNIX(chirp_reli_ticket_register(host->hostport, ticket, "self", 3600, stoptime));

	CATCHUNIX(snprintf(template, sizeof(template), "%s/file", host->root));
	CATCHUNIXIGNORE(chirp_reli_mkdir(host->hostport, template, S_IRWXU, stoptime), EEXIST);
	CATCHUNIX(chirp_reli_setacl(host->hostport, template, whoami, "rwldpa", stoptime));

	CATCHUNIX(snprintf(template, sizeof(template), "%s/file/" CONFUGA_FID_PRIFMT, host->root, CONFUGA_FID_PRIARGS(empty)));
	CATCHUNIXIGNORE(chirp_reli_putfile_buffer(host->hostport, template, "", S_IRUSR, 0, stoptime), EEXIST);
	//confugaR_register(C, empty, 0, host); /* FIXME CATCH fails if we can't get a database lock... */

	CATCHUNIX(snprintf(template, sizeof(template), "%s/open", host->root));
	CATCHUNIXIGNORE(chirp_reli_mkdir(host->hostport, template, S_IRWXU, stoptime), EEXIST);
	CATCHUNIX(chirp_reli_setacl(host->hostport, template, whoami, "rwldpa", stoptime));
	CATCHUNIX(chirp_reli_setacl(host->hostport, template, "hostname:*.nd.edu", "p", stoptime)); /* FIXME */
	CATCHUNIX(chirp_reli_setacl(host->hostport, template, "hostname:localhost", "p", stoptime)); /* FIXME */
	CATCHUNIX(chirp_reli_setacl(host->hostport, template, "address:127.0.0.1", "p", stoptime)); /* FIXME */
	CATCHUNIX(chirp_reli_ticket_modify(host->hostport, ticket, template, "p", stoptime));

	CATCHUNIX(snprintf(template, sizeof(template), "%s/tickets", host->root));
	CATCHUNIXIGNORE(chirp_reli_mkdir(host->hostport, template, S_IRWXU, stoptime), EEXIST);
	CATCHUNIX(chirp_reli_setacl(host->hostport, template, whoami, "rwldpa", stoptime));

	CATCHUNIX(snprintf(template, sizeof(template), "%s/ticket", host->root));
	stream = fopen(ticket, "r");
	CATCHUNIX(stream ? 0 : -1);
	CATCHUNIX(fstat(fileno(stream), &info));
	CATCHUNIX(chirp_reli_putfile(host->hostport, template, stream, S_IRUSR, info.st_size, stoptime));

	debug(D_DEBUG, "setting `%s' to initialized", host->hostport);
	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_text(stmt, 1, host->hostport, -1, SQLITE_TRANSIENT));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	debug(D_CONFUGA, "%s/%s initialized", host->hostport, host->root);

	rc = 0;
	goto out;
out:
	if (stream)
		fclose(stream);
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
	sqlcatch(sqlite3_bind_text(stmt, 1, hostport, -1, SQLITE_TRANSIENT));
	sqlcatch(sqlite3_bind_text(stmt, 2, root, -1, SQLITE_TRANSIENT));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	if (sqlite3_changes(db) == 1)
		debug(D_CONFUGA, "Storage Node %d (chirp://%s/%s) online", (int)sqlite3_last_insert_rowid(db), hostport, root);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

#include "auth_all.h"
CONFUGA_IAPI int confugaS_setup (confuga *C)
{
	static const char SQL[] =
		"DROP TABLE IF EXISTS ConfugaResults;"
		"CREATE TEMPORARY TABLE ConfugaResults AS" /* so we don't hold read locks */
		"	SELECT hostport, root"
		"		FROM Confuga.StorageNodeActive"
		"		WHERE initialized = 0;"
		"SELECT * FROM ConfugaResults;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	auth_register_all(); /* TODO */

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		struct confuga_host host;
		snprintf(host.hostport, sizeof(host.hostport), "%s", (const char *) sqlite3_column_text(stmt, 0));
		snprintf(host.root, sizeof(host.root), "%s", (const char *) sqlite3_column_text(stmt, 1));
		sn_init(C, &host);
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
