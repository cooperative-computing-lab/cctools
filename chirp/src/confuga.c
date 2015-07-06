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

#include "auth_all.h"
#include "buffer.h"
#include "copy_stream.h"
#include "create_dir.h"
#include "debug.h"
#include "full_io.h"
#include "pattern.h"
#include "sha1.h"
#include "stringtools.h"

#include "catch.h"
#include "chirp_protocol.h"
#include "chirp_sqlite.h"

#include <sys/stat.h>
#ifdef HAS_SYS_STATFS_H
#	include <sys/statfs.h>
#endif
#ifdef HAS_SYS_STATVFS_H
#	include <sys/statvfs.h>
#endif
#if CCTOOLS_OPSYS_CYGWIN || CCTOOLS_OPSYS_DARWIN || CCTOOLS_OPSYS_FREEBSD
#	include <sys/mount.h>
#	include <sys/param.h>
#	define statfs64 statfs
#elif CCTOOLS_OPSYS_SUNOS
#	define statfs statvfs
#	define statfs64 statvfs64
#endif

#include <assert.h>
#include <limits.h>
#include <math.h>
#include <string.h>
#include <time.h>

#define TICKET_REFRESH (6*60*60)

static void trace (void *ud, const char *sql)
{
#if 0
	(void)ud;
	debug(D_DEBUG, "SQL: `%s'", sql);
#else
	fputs(sql, (FILE *)ud);
#endif
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

static void s_log (sqlite3_context *context, int argc, sqlite3_value **argv)
{
	if (argc == 1) {
		sqlite3_result_double(context, log(sqlite3_value_double(argv[0])));
	} else {
		sqlite3_result_null(context);
	}
}

static void s_floor (sqlite3_context *context, int argc, sqlite3_value **argv)
{
	if (argc == 1) {
		sqlite3_result_double(context, floor(sqlite3_value_double(argv[0])));
	} else {
		sqlite3_result_null(context);
	}
}

static void s_url_truncate (sqlite3_context *context, int argc, sqlite3_value **argv)
{
	if (argc == 1) {
		size_t n;
		BUFFER_STACK(B, CHIRP_PATH_MAX); /* limit url list size */
		const char *urls = (const char *)sqlite3_value_text(argv[0]);
		char *url = NULL;

		while (pattern_match(urls, "%s*(%S+)()", &url, &n) >= 0) {
			if ((buffer_pos(B)+strlen(url)) >= CHIRP_PATH_MAX)
				break;
			if (buffer_pos(B))
				buffer_putliteral(B, "\t");
			buffer_putstring(B, url);
			urls += n;
			url = realloc(url, 0);
		}

		assert(buffer_pos(B));
		sqlite3_result_text(context, buffer_tostring(B), -1, SQLITE_TRANSIENT);
		url = realloc(url, 0);
	} else {
		sqlite3_result_null(context);
	}
}

static int db_init (confuga *C)
{
	static const char SQL[] =
		"PRAGMA foreign_keys = ON;"
		"PRAGMA journal_mode = WAL;"
		"CREATE TEMPORARY TABLE IF NOT EXISTS ConfugaRuntimeOption ("
		"	key TEXT PRIMARY KEY,"
		"	value TEXT NOT NULL"
		");"
		/* Create the database in one transaction. If it already exists, it will fail. */
		"BEGIN TRANSACTION;"
		"CREATE TABLE Confuga.Option ("
		"	key TEXT PRIMARY KEY,"
		"	value TEXT NOT NULL"
		") WITHOUT ROWID;" /* SQLite optimization */
		"INSERT INTO Confuga.Option VALUES"
		"	('id', (PRINTF('confuga:%s', UPPER(HEX(RANDOMBLOB(20))))))" /* Confuga FS GUID */
		"	;"
		"CREATE TABLE Confuga.File ("
		"	id BLOB PRIMARY KEY," /* SHA1 binary hash */
		"	links INTEGER NOT NULL DEFAULT 0," /* number of links in the NS pointing to this file (at last check) */
		"	size INTEGER NOT NULL,"
		"	minimum_replicas INTEGER NOT NULL DEFAULT 1,"
		"	time_create DATETIME NOT NULL DEFAULT (strftime('%s', 'now')),"
		"	time_health DATETIME" /* last check of NS that updated `links` */
		") WITHOUT ROWID;" /* SQLite optimization */
		"INSERT INTO Confuga.File (id, size, minimum_replicas) VALUES"
		"	(X'da39a3ee5e6b4b0d3255bfef95601890afd80709', 0, 9223372036854775807)" /* empty file */
		"	;"
		"CREATE TABLE Confuga.Replica ("
		"	fid BLOB NOT NULL REFERENCES File (id),"
		"	sid INTEGER NOT NULL REFERENCES StorageNode (id),"
		"	time_create DATETIME NOT NULL DEFAULT (strftime('%s', 'now')),"
		"	time_health DATETIME," /* last confirmation that the replica exists and has the correct checksum */
		"	PRIMARY KEY (fid, sid)"
		") WITHOUT ROWID;" /* SQLite optimization */
		"CREATE TABLE Confuga.StorageNode ("
		"	id INTEGER PRIMARY KEY,"
		/* Confuga fields (some overlap with catalog_server fields) */
		"	hostport TEXT UNIQUE NOT NULL,"
		"	initialized INTEGER NOT NULL DEFAULT 0,"
		"	root TEXT NOT NULL DEFAULT '/.confuga',"
		"	ticket BLOB," /* ticket currently used by SN */
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
		"	version TEXT"
		");"
		"CREATE TRIGGER Confuga.StorageNode_UpdateTrigger AFTER UPDATE ON StorageNode"
		"	BEGIN"
		"		UPDATE StorageNode SET time_update = datetime('now') WHERE id = NEW.id;"
		"	END;"
		"CREATE VIEW Confuga.StorageNodeAlive AS"
		"	SELECT *"
		"		FROM StorageNode"
		"		WHERE lastheardfrom IS NOT NULL AND strftime('%s', 'now', '-15 minutes') <= lastheardfrom;"
		"CREATE VIEW Confuga.StorageNodeActive AS"
		"	SELECT *"
		"		FROM StorageNodeAlive"
		"		WHERE initialized != 0;"
		"CREATE VIEW Confuga.FileReplicas AS"
		"	SELECT * FROM File JOIN Replica ON File.id = Replica.fid;"
		"CREATE TABLE Confuga.TransferJob ("
		"	id INTEGER PRIMARY KEY AUTOINCREMENT," /* AUTOINCREMENT to ensure ROWID is never reallocated (i.e. forever unique) */
		"	cid INTEGER," /* job id on fsid */
		"	fid BLOB NOT NULL REFERENCES File (id),"
		"	fsid INTEGER NOT NULL REFERENCES StorageNode (id)," /* from SN */
		"	open TEXT," /* open file ID on tsid */
		"	progress INTEGER,"
		"	source TEXT NOT NULL REFERENCES TransferJobSource (source),"
		"	source_id INTEGER," /* ConfugaJob id */
		"	state TEXT NOT NULL REFERENCES TransferJobState (state),"
		"	tag TEXT NOT NULL DEFAULT '(unknown)',"
		"	time_create DATETIME,"
		"	time_commit DATETIME,"
		"	time_complete DATETIME,"
		"	time_error DATETIME,"
		"	time_new DATETIME NOT NULL DEFAULT (strftime('%s', 'now')),"
		"	tsid INTEGER NOT NULL REFERENCES StorageNode (id)," /* to SN */
		/* cid status */
		"	error TEXT,"
		"	exit_code INTEGER,"
		"	exit_signal TEXT,"
		"	exit_status TEXT," /* REFERENCES ExitStatus (status)," */
		"	status TEXT"
		");" /* REFERENCES JobStatus (status));" */
		"CREATE UNIQUE INDEX Confuga.TransferJobIndex ON TransferJob (cid, fsid);"
		"CREATE TABLE Confuga.TransferJobState ("
		"	state TEXT PRIMARY KEY,"
		"	active INTEGER NOT NULL"
		") WITHOUT ROWID;"
		"INSERT INTO Confuga.TransferJobState (state, active) VALUES"
		"	('NEW', 1),"
		"	('CREATED', 1),"
		"	('COMMITTED', 1),"
		"	('WAITED', 1),"
		"	('REAPED', 1),"
		"	('COMPLETED', 0),"
		"	('ERRORED', 0)"
		"	;"
		IMMUTABLE("Confuga.TransferJobState")
		"CREATE TABLE Confuga.TransferJobSource ("
		"	source TEXT PRIMARY KEY"
		") WITHOUT ROWID;"
		"INSERT INTO Confuga.TransferJobSource (source) VALUES"
		"	('HEALTH'),"
		"	('JOB')"
		"	;"
		IMMUTABLE("Confuga.TransferJobSource")
		"CREATE VIEW Confuga.ActiveTransfers AS"
		"	SELECT TransferJob.*"
		"		FROM TransferJob JOIN TransferJobState ON TransferJob.state = TransferJobState.state"
		"		WHERE TransferJobState.active = 1;"
		"END TRANSACTION;"
		;

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

	sqlcatch(sqlite3_create_function(db, "floor", 1, SQLITE_UTF8, NULL, &s_floor, NULL, NULL));
	sqlcatch(sqlite3_create_function(db, "log", 1, SQLITE_UTF8, NULL, &s_log, NULL, NULL));
	sqlcatch(sqlite3_create_function(db, "url_truncate", 1, SQLITE_UTF8, NULL, &s_url_truncate, NULL, NULL));

	debug(D_DEBUG, "initializing Confuga");
	do {
		char *errmsg;
		rc = sqlite3_exec(db, SQL, NULL, NULL, &errmsg); /* Ignore any errors. */
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
	static const char SQL[] =
		"SELECT value FROM Confuga.Option WHERE key = 'id';"
		;

	int rc;
	sqlite3 *db = attachdb;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

#if 0
	/* This is debugging code that uses SQLite's vfstrace module. */
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

#if 1
	sqlite3_profile(db, profile, NULL);
#else
	(void)profile;
#endif

#if 0
	sqlite3_trace(C->db, trace, log);
#else
	(void)trace;
#endif

	CATCH(db_init(C));

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	if ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		debug(D_CONFUGA, "Confuga ID: %s", (const char *)sqlite3_column_text(stmt, 0));
	} else {
		sqlcatch(rc);
		abort();
	}
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	if (rc) {
		if (attachdb == NULL)
			sqlite3_close(db);
		C->db = NULL;
	}
	sqlite3_finalize(stmt);
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
	char *buffer = NULL;
	size_t len;

	debug(D_CONFUGA, "creating new authentication ticket");

	key = popen("openssl genrsa " stringify(CONFUGA_TICKET_BITS) " 2> /dev/null", "r");
	CATCHUNIX(key ? 0 : -1);
	CATCHUNIX(copy_stream_to_buffer(key, &buffer, &len));

	sha1_buffer(buffer, len, (unsigned char *)&C->ticket);

	CATCHUNIX(snprintf(path, sizeof(path), "%s/ticket", C->root));
	ticket = fopen(path, "w");
	CATCHUNIX(ticket ? 0 : -1);
	CATCHUNIX(full_fwrite(ticket, buffer, len));

	rc = 0;
	goto out;
out:
	if (ticket)
		fclose(ticket);
	if (key)
		pclose(key);
	free(buffer);
	return rc;
}

static int setroot (confuga *C, const char *root)
{
	int rc;

	strncpy(C->root, root, sizeof(C->root)-1);
	CATCHUNIX(create_dir(C->root, S_IRWXU) ? 0 : -1);
	CATCH(confugaN_init(C));
	CATCH(confugaI_dbload(C, NULL));

	rc = 0;
	goto out;
out:
	return rc;
}

static int parse_uri (confuga *C, const char *uri, int *explicit_auth)
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

		while (pattern_match(rest, "([%w-]+)=([^&]*)&?()", &option, &value, &n) >= 0) {
			if (strcmp(option, "auth") == 0) {
				const char *current = value;
				size_t p;
				while (pattern_match(current, "^(%a+),?()", &subvalue, &p) >= 0) {
					if (!auth_register_byname(subvalue)) {
						debug(D_NOTICE, "auth mechanism '%s' is unknown", subvalue);
						CATCH(EINVAL);
					}
					*explicit_auth = 1;
					current += p;
					subvalue = realloc(subvalue, 0);
				}
			} else if (strcmp(option, "concurrency") == 0) {
				if (pattern_match(value, "^(%d+)$", &subvalue) >= 0)
					CATCH(confuga_concurrency(C, strtoul(subvalue, NULL, 10)));
				else CATCH(EINVAL);
			} else if (strcmp(option, "pull-threshold") == 0) {
				if (pattern_match(value, "^(%d+[kKmMgGtTpP]?)[bB]?$", &subvalue) >= 0) {
					CATCH(confuga_pull_threshold(C, string_metric_parse(subvalue)));
				} else CATCH(EINVAL);
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
			} else if (strcmp(option, "tickets") == 0) {
				auth_ticket_load(value);
			} else {
				debug(D_NOTICE|D_CONFUGA, "unknown URI option `%s'", option);
				CATCH(EINVAL);
			}
			rest += n;
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
	int explicit_auth = 0;

	debug(D_CONFUGA, "connecting to %s", uri);
	debug(D_DEBUG, "using sqlite version %s", sqlite3_libversion());

	C = malloc(sizeof(confuga));
	if (C == NULL) CATCH(ENOMEM);
	memset(C, 0, sizeof(*C));

	C->concurrency = 0; /* unlimited */
	C->pull_threshold = (1<<27); /* 128MB */
	C->replication = CONFUGA_REPLICATION_PUSH_ASYNCHRONOUS;
	C->replication_n = 1; /* max one push async job per node */
	C->scheduler = CONFUGA_SCHEDULER_FIFO;
	C->scheduler_n = 0; /* unlimited */

	auth_clear();

	CATCH(parse_uri(C, uri, &explicit_auth));

	if (!explicit_auth) {
		auth_register_all();
	}

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
		CATCHUNIX(copy_stream_to_buffer(file, &node, NULL));
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
	debug(D_CONFUGA, "setting scheduler strategy to %d-%" PRIu64, strategy, n);
	C->scheduler = strategy;
	C->scheduler_n = n;
	return 0;
}

CONFUGA_API int confuga_pull_threshold (confuga *C, uint64_t n)
{
	debug(D_CONFUGA, "setting pull threshold %" PRIu64, n);
	C->pull_threshold = n;
	return 0;
}

CONFUGA_API int confuga_replication_strategy (confuga *C, int strategy, uint64_t n)
{
	debug(D_CONFUGA, "setting replication strategy to %d-%" PRIu64, strategy, n);
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
	time_t ticket_generated = 0;

	while (1) {
		time_t now = time(NULL);

		if (ticket_generated+TICKET_REFRESH <= now) {
			CATCH(setup_ticket(C));
			ticket_generated = now;
		}

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
	struct statfs64 fsinfo;
	CATCHUNIX(statfs64(C->root, &fsinfo));
	info->files = fsinfo.f_files;
	info->ffree = fsinfo.f_ffree;

	info->type = 0x46554741;
	info->flag = 0;

	debug(D_CONFUGA, "= " CONFUGA_STATFS_DEBFMT, CONFUGA_STATFS_PRIARGS(*info));

	rc = 0;
	goto out;
out:
	return rc;
}

/* vim: set noexpandtab tabstop=4: */
