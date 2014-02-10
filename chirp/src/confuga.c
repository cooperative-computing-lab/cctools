/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/* TODO
 *
 * o Interface to read File/Replica/SN metadata.
 * o Replica GC.
 * o Replica Health.
 * o Dynamically generated tickets for file transfers.
 * o Bind task failures in special .confuga/job/id/files/...
 * o Limit # of operations for each create/commit/wait/etc.
 */

#include "confuga_fs.h"

#include "copy_stream.c"
#include "create_dir.h"
#include "debug.h"
#include "pattern.h"

#include "catch.h"
#include "chirp_sqlite.h"

#include <sys/stat.h>
#ifdef HAS_SYS_STATFS_H
#include <sys/statfs.h>
#endif
#ifdef HAS_SYS_STATVFS_H
#include <sys/statvfs.h>
#endif
#if CCTOOLS_OPSYS_CYGWIN || CCTOOLS_OPSYS_DARWIN || CCTOOLS_OPSYS_FREEBSD
#define statfs64 statfs
#endif
#if CCTOOLS_OPSYS_SUNOS
#define statfs statvfs
#define statfs64 statvfs64
#endif

#include <assert.h>
#include <limits.h>
#include <string.h>
#include <time.h>

static void trace (void *ud, const char *sql)
{
#if 0
	(void)ud;
	debug(D_DEBUG, "SQL: `%s'", sql);
#else
	fputs(sql, (FILE *)ud);
#endif
}

static int db_init (confuga *C)
{
	static const char Initialize[] =
		"PRAGMA foreign_keys = ON;"
		"PRAGMA journal_mode = WAL;"
		"BEGIN TRANSACTION;"
		"CREATE TABLE Confuga.Option ("
		"	key TEXT PRIMARY KEY,"
		"	value TEXT NOT NULL);"
		"INSERT INTO Confuga.Option VALUES"
		"	('id', (PRINTF('confuga:%s', UPPER(HEX(RANDOMBLOB(20))))));"
		"CREATE TABLE Confuga.File ("
		"	id BLOB PRIMARY KEY," /* SHA1 binary hash */
		"	links INTEGER NOT NULL DEFAULT 0," /* number of links in the NS pointing to this file (at last check) */
		"	size INTEGER NOT NULL,"
		"	minimum_replicas INTEGER NOT NULL DEFAULT 1,"
		"	time_create DATETIME NOT NULL DEFAULT (strftime('%s', 'now')),"
		"	time_health DATETIME);" /* last check of NS that updated `links` */
		"CREATE TABLE Confuga.Replica ("
		"	fid BLOB NOT NULL REFERENCES File (id)," /* do we actually want this? maybe replicas are added for files that don't exist yet */
		"	sid INTEGER NOT NULL REFERENCES StorageNode (id),"
		"	time_create DATETIME NOT NULL DEFAULT (strftime('%s', 'now')),"
		"	time_health DATETIME," /* last confirmation that the replica exists and has the correct checksum */
		"	PRIMARY KEY (fid, sid));"
		"CREATE TABLE Confuga.StorageNode ("
		"	id INTEGER PRIMARY KEY,"
		/* Confuga fields (some overlap with catalog_server fields) */
		"	hostport TEXT UNIQUE NOT NULL,"
		"	initialized INTEGER NOT NULL DEFAULT 0,"
		"	root TEXT NOT NULL DEFAULT '/.confuga',"
		"	time_create DATETIME NOT NULL DEFAULT (strftime('%s', 'now')),"
		"	time_delete DATETIME,"
		"	time_update DATETIME NOT NULL DEFAULT (strftime('%s', 'now')),"
		/* catalog_server fields */
		"	address TEXT,"
		"	avail INTEGER,"
		"	backend TEXT,"
		"	bytes_read INTEGER,"
		"	bytes_written INTEGER,"
		"	cpu TEXT,"
		"	cpus INTEGER,"
		"	lastheardfrom DATETIME,"
		"	load1 REAL,"
		"	load5 REAL,"
		"	load15 REAL,"
		"	memory_avail TEXT,"
		"	memory_total TEXT,"
		"	minfree INTEGER,"
		"	name TEXT,"
		"	opsys TEXT,"
		"	opsysversion TEXT,"
		"	owner TEXT,"
		"	port INTEGER,"
		"	starttime DATETIME,"
		"	total INTEGER,"
		"	total_ops INTEGER,"
		"	url TEXT,"
		"	version TEXT);"
		"CREATE TRIGGER Confuga.StorageNode_UpdateTrigger AFTER UPDATE ON StorageNode"
		"	BEGIN"
		"		UPDATE StorageNode SET time_update = datetime('now') WHERE id = NEW.id;"
		"	END;"
		"CREATE VIEW Confuga.StorageNodeActive AS"
		"	SELECT *"
		"		FROM StorageNode"
		"		WHERE lastheardfrom IS NOT NULL AND strftime('%s', 'now', '-5 minutes') <= lastheardfrom;"
		"CREATE VIEW Confuga.FileReplicas AS"
		"	SELECT * FROM File JOIN Replica ON File.id = Replica.fid;"
		"CREATE TABLE Confuga.TransferJob ("
		"	id INTEGER PRIMARY KEY AUTOINCREMENT," /* AUTOINCREMENT to ensure ROWID is never reallocated (i.e. forever unique) */
		"	cid INTEGER," /* job id on fsid */
		"	fid BLOB NOT NULL REFERENCES File (id),"
		"	fsid INTEGER NOT NULL REFERENCES StorageNode (id)," /* from SN */
		"	open TEXT," /* open file ID on tsid */
		"	progress INTEGER,"
		"	state TEXT NOT NULL REFERENCES TransferJobState (state),"
		"	tag TEXT NOT NULL DEFAULT '(unknown)',"
		"	time_commit DATETIME,"
		"	time_complete DATETIME,"
		"	time_error DATETIME,"
		"	time_new DATETIME NOT NULL DEFAULT (strftime('%s', 'now')),"
		"	tsid INTEGER NOT NULL REFERENCES StorageNode (id)," /* to SN */
		/* cid status */
		"	error TEXT,"
		"	exit_code INTEGER,"
		"	exit_signal INTEGER,"
		"	exit_status TEXT," /* REFERENCES ExitStatus (status)," */
		"	status TEXT);" /* REFERENCES JobStatus (status));" */
		"CREATE TABLE Confuga.TransferJobState (state TEXT PRIMARY KEY);"
		"INSERT INTO Confuga.TransferJobState (state) VALUES"
		"	('NEW'),"
		"	('CREATED'),"
		"	('COMMITTED'),"
		"	('WAITED'),"
		"	('REAPED'),"
		"	('COMPLETED'),"
		"	('ERRORED');"
		IMMUTABLE("Confuga.TransferJobState")
		"CREATE VIEW Confuga.ActiveTransfers AS"
		"	SELECT *"
		"		FROM TransferJob"
		"		WHERE TransferJob.state != 'ERRORED' AND TransferJob.state != 'COMPLETED';"
		"END TRANSACTION;";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	char uri[PATH_MAX];

	if (snprintf(uri, PATH_MAX, "file://%s/confuga.db?mode=rwc", C->root) >= PATH_MAX)
		fatal("root path `%s' too long", C->root);

	sqlite3_busy_timeout(db, 5000);

	debug(D_DEBUG, "attaching database `%s'", uri);
	sqlcatch(sqlite3_prepare_v2(db, "ATTACH DATABASE ? AS Confuga;", -1, &stmt, NULL));
	sqlcatch(sqlite3_bind_text(stmt, 1, uri, -1, SQLITE_STATIC));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	debug(D_DEBUG, "initializing Confuga");
	do {
		char *errmsg;
		rc = sqlite3_exec(db, Initialize, NULL, NULL, &errmsg); /* Ignore any errors. */
		if (rc) {
			if (!strstr(sqlite3_errmsg(db), "already exists"))
				debug(D_DEBUG, "[%s:%d] sqlite3 error: %d `%s': %s", __FILE__, __LINE__, rc, sqlite3_errstr(rc), sqlite3_errmsg(db));
			sqlite3_exec(db, "ROLLBACK TRANSACTION;", NULL, NULL, NULL);
		}
		sqlite3_free(errmsg);
	} while (rc == SQLITE_BUSY);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

CONFUGA_IAPI int confugaI_dbload (confuga *C, sqlite3 *attachdb)
{
	int rc;
	sqlite3 *db = attachdb;

#if 0
	char logp[PATH_MAX];
	snprintf(logp, sizeof(logp), "/tmp/sqlite-vfs.%d.log", getpid());
	FILE *log = fopen(logp, "a");
	setvbuf(log, NULL, _IONBF, 0);
	if (!attachdb) {
		extern int vfstrace_register(const char *zTraceName, const char *zOldVfsName, int (*)(const char *, void *), void *pOutArg, int makeDefault);
		vfstrace_register("trace", NULL, (int (*)(const char*,void*))fputs, log, 1);
	}
#endif

	if (db == NULL)
		CATCH(sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_URI|SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, NULL));
	assert(db);
	C->db = db;

#if 0
	sqlite3_trace(C->db, trace, log);
#else
	(void)trace;
#endif

	CATCH(db_init(C));

	rc = 0;
	goto out;
out:
	if (rc) {
		if (attachdb == NULL)
			sqlite3_close(db);
		C->db = NULL;
	}
	return rc;
}

CONFUGA_IAPI int confugaI_dbclose (confuga *C)
{
	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL; /* unused */

	do {
		debug(D_DEBUG, "disconnecting from sqlite3 db");
		rc = sqlite3_close_v2(db);
		usleep(1000000);
	} while (rc == SQLITE_BUSY);
	sqlcatch(rc);
	assert(rc == SQLITE_OK);

	debug(D_DEBUG, "disconnected from sqlite3 db");
	C->db = NULL;

	rc = 0;
	goto out;
out:
	return rc;
}

static int setup_ticket (confuga *C)
{
	int rc;
	char path[PATH_MAX];
	FILE *ticket = NULL;
	FILE *key = NULL;

	CATCHUNIX(snprintf(path, sizeof(path), "%s/ticket", C->root));
	if (access(path, R_OK) == 0)
		return 0;

	ticket = fopen(path, "w");
	CATCHUNIX(ticket ? 0 : -1);
	key = popen("openssl genrsa " stringify(CONFUGA_TICKET_BITS) " 2> /dev/null", "r");
	CATCHUNIX(key ? 0 : -1);
	CATCHUNIX(copy_stream_to_stream(key, ticket));

	rc = 0;
	goto out;
out:
	if (ticket)
		fclose(ticket);
	if (key)
		pclose(key);
	return rc;
}

static int setroot (confuga *C, const char *root)
{
	int rc;
	char namespace[CONFUGA_PATH_MAX] = "";

	strncpy(C->root, root, sizeof(C->root)-1);
	CATCHUNIX(create_dir(C->root, S_IRWXU) ? 0 : -1);
	snprintf(namespace, sizeof(namespace)-1, "%s/root", C->root);
	CATCHUNIX(create_dir(namespace, S_IRWXU) ? 0 : -1);
	CATCH(confugaI_dbload(C, NULL));

	rc = 0;
	goto out;
out:
	return rc;
}

static int parse_uri (confuga *C, const char *uri)
{
	int rc;
	char *root = NULL;
	char *options = NULL;
	char *option = NULL;
	char *value = NULL;
	char *subvalue = NULL;

	if (pattern_match(uri, "confuga://(..-)%?(.*)", &root, &options) >= 0) {
		const char *rest = options;
		size_t n;
		CATCH(setroot(C, root));

		while (pattern_match(rest, "(%w+)=([^&]*)&?()", &option, &value, &n) >= 0) {
			if (strcmp(option, "concurrency") == 0) {
				if (pattern_match(value, "^(%d+)$", &subvalue) >= 0)
					CATCH(confuga_concurrency(C, strtoul(subvalue, NULL, 10)));
				else CATCH(EINVAL);
			} else if (strcmp(option, "scheduler") == 0) {
				if (pattern_match(value, "^fifo%-?(%d*)$", &subvalue) >= 0) {
					CATCH(confuga_scheduler_strategy(C, CONFUGA_SCHEDULER_FIFO, strtoul(subvalue, NULL, 10)));
				} else CATCH(EINVAL);
			} else if (strcmp(option, "replication") == 0) {
				if (pattern_match(value, "^push%-sync%-?(%d*)$", &subvalue) >= 0) {
					CATCH(confuga_replication_strategy(C, CONFUGA_REPLICATION_PUSH_SYNCHRONOUS, strtoul(subvalue, NULL, 10)));
				} else if (pattern_match(value, "^push%-async%-?(%d*)$", &subvalue) >= 0) {
					CATCH(confuga_replication_strategy(C, CONFUGA_REPLICATION_PUSH_ASYNCHRONOUS, strtoul(subvalue, NULL, 10)));
				} else CATCH(EINVAL);
			} else if (strcmp(option, "nodes") == 0) {
				CATCH(confuga_nodes(C, value));
			} else {
				debug(D_NOTICE|D_CONFUGA, "unknown URI option `%s'", option);
				CATCH(EINVAL);
			}
			rest += n-1;
			option = realloc(option, 0);
			value = realloc(value, 0);
			subvalue = realloc(subvalue, 0);
		}
		if (strlen(rest)) {
			debug(D_NOTICE|D_CONFUGA, "unparseable URI at `%s'", rest);
			CATCH(EINVAL);
		}
	} else {
		CATCH(setroot(C, uri));
	}

	rc = 0;
	goto out;
out:
	free(root);
	free(options);
	free(option);
	free(value);
	free(subvalue);
	return rc;
}

CONFUGA_API int confuga_connect (confuga **Cp, const char *uri, const char *catalog)
{
	int rc;
	confuga *C = NULL;

	debug(D_CONFUGA, "connecting to %s", uri);
	debug(D_DEBUG, "using sqlite version %s", sqlite3_libversion());

	C = malloc(sizeof(confuga));
	if (C == NULL) CATCH(ENOMEM);
	memset(C, 0, sizeof(*C));
	C->concurrency = 0;
	C->scheduler = CONFUGA_SCHEDULER_FIFO;
	C->scheduler_n = 1;
	C->replication = CONFUGA_REPLICATION_PUSH_ASYNCHRONOUS;
	C->replication_n = UINT64_MAX;

	CATCH(parse_uri(C, uri));

	CATCH(setup_ticket(C));
	CATCH(confugaS_catalog(C, catalog));

	*Cp = C;
	rc = 0;
	goto out;
out:
	if (rc)
		free(C);
	return rc;
}

CONFUGA_API int confuga_nodes (confuga *C, const char *nodes)
{
	int rc;
	FILE *file = NULL;
	char *node = NULL;
	const char *rest;
	char *hostname = NULL;
	char *root = NULL;
	size_t n;

	if (pattern_match(nodes, "^node:(.*)", &node) >= 0) {
		/* do nothing */
	} else if (pattern_match(nodes, "^file:(.*)", &node) >= 0) {
		file = fopen(node, "r");
		CATCHUNIX(file ? 0 : -1);
		CATCHUNIX(copy_stream_to_buffer(file, &node));
	} else CATCH(EINVAL);

	rest = node;
	while (pattern_match(rest, "^[%s,]*chirp://([^/,%s]+)([^,%s]*)()", &hostname, &root, &n) >= 0) {
		CATCH(confugaS_node_insert(C, hostname, root));
		rest += n;
		hostname = realloc(hostname, 0);
		root = realloc(hostname, 0);
	}

	rc = 0;
	goto out;
out:
	if (file)
		fclose(file);
	free(node);
	free(hostname);
	free(root);
	return rc;
}

CONFUGA_API int confuga_concurrency (confuga *C, uint64_t n)
{
	debug(D_CONFUGA, "setting concurrency to %" PRIu64, n);
	C->concurrency = n;
	return 0;
}

CONFUGA_API int confuga_scheduler_strategy (confuga *C, int strategy, uint64_t n)
{
	debug(D_CONFUGA, "setting scheduler strategy to %d-%"PRIu64, strategy, n);
	C->scheduler = strategy;
	C->scheduler_n = n;
	return 0;
}

CONFUGA_API int confuga_replication_strategy (confuga *C, int strategy, uint64_t n)
{
	debug(D_CONFUGA, "setting replication strategy to %d-%"PRIu64, strategy, n);
	C->replication = strategy;
	C->replication_n = n;
	return 0;
}

CONFUGA_API int confuga_disconnect (confuga *C)
{
	int rc;

	debug(D_CONFUGA, "disconnecting from confuga://%s", C->root);

	CATCH(confugaI_dbclose(C));
	free(C);

	rc = 0;
	goto out;
out:
	return rc;
}

CONFUGA_API int confuga_daemon (confuga *C)
{
	int rc;

	time_t catalog_sync = 0;
	time_t node_setup = 0;

	while (1) {
		time_t now = time(NULL);

		if (catalog_sync+15 <= now) {
			confugaS_catalog_sync(C);
			catalog_sync = now;
		}

		if (node_setup+60 <= now) {
			confugaS_setup(C);
			node_setup = now;
		}

		confugaJ_schedule(C);
		confugaR_manager(C);

		usleep(10000);
	}

	rc = 0;
	goto out;
out:
	return rc;
}

CONFUGA_API int confuga_statfs (confuga *C, struct confuga_statfs *info)
{
	static const char StatusFS[] =
		"SELECT SUM(avail), SUM(total) FROM Confuga.StorageNodeActive"
		"    WHERE time_delete IS NULL;"
		"SELECT SUM(total) FROM"
		"    (SELECT File.size*COUNT(Replica.sid) AS total"
		"     FROM Confuga.File JOIN Confuga.Replica ON File.id = Replica.fid"
		"     GROUP BY Replica.fid);";

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current;

	debug(D_CONFUGA, "statfs(\"confuga://%s\")", C->root);

	/* So the idea here is that total is the sum of all the total bytes for all
	 * storage nodes. We want to communicate the total space used by Confuga at
	 * some level so we say the number of free bytes is equal to the total
	 * bytes minus bytes *used by Confuga*. But, since the Storage Node disks
	 * may be used by other services, we use avail to indicate the number of
	 * actual free bytes. This actually fits the traditional idea of statfs
	 * pretty well.
	 */
	sqlcatch(sqlite3_prepare_v2(db, StatusFS, sizeof(StatusFS), &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
	info->bsize = 1;
	info->blocks = sqlite3_column_int64(stmt, 1);
	info->bavail = sqlite3_column_int64(stmt, 0);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
	info->bfree = info->blocks - sqlite3_column_int64(stmt, 0);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	/* we can use the host values for the NS related fields */
	struct statfs64 linfo;
	CATCHUNIX(statfs64(C->root, &linfo));
	info->files = linfo.f_files;
	info->ffree = linfo.f_ffree;

	info->type = 0x46554741;
	info->flag = 0;

	debug(D_CONFUGA, "= " CONFUGA_STATFS_DEBFMT, CONFUGA_STATFS_PRIARGS(*info));

	rc = 0;
	goto out;
out:
	return rc;
}

/* vim: set noexpandtab tabstop=4: */
