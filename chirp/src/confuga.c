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
#include "compat-at.h"
#include "buffer.h"
#include "create_dir.h"
#include "debug.h"
#include "full_io.h"
#include "pattern.h"
#include "sha1.h"
#include "shell.h"
#include "stringtools.h"

#include "catch.h"
#include "chirp_protocol.h"
#include "chirp_sqlite.h"

#include <fcntl.h>
#include <sys/select.h>
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

#if CCTOOLS_OPSYS_CYGWIN || CCTOOLS_OPSYS_DARWIN || CCTOOLS_OPSYS_FREEBSD || CCTOOLS_OPSYS_DRAGONFLY
	/* Cygwin does not have 64-bit I/O, while FreeBSD/Darwin has it by default. */
#	define stat64 stat
#	define fstat64 fstat
#	define ftruncate64 ftruncate
#	define statfs64 statfs
#	define fstatfs64 fstatfs
#	define fstatat64 fstatat
#elif defined(CCTOOLS_OPSYS_SUNOS)
	/* Solaris has statfs, but it doesn't work! Use statvfs instead. */
#	define statfs statvfs
#	define fstatfs fstatvfs
#	define statfs64 statvfs64
#	define fstatfs64 fstatvfs64
#endif

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


static int dbupgrade (confuga *C)
{
	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	int version;

	/* This "alter table" protocol comes from:
	 *
	 *   https://www.sqlite.org/lang_altertable.html#otheralter
	 */
	sqlcatch(sqlite3_exec(db, "PRAGMA foreign_keys = OFF; BEGIN TRANSACTION;", NULL, NULL, NULL));

	rc = sqlite3_prepare_v2(db, "SELECT value FROM Confuga.State WHERE key = 'db-version';", -1, &stmt, NULL);
	if (rc) {
		sqlcatch(sqlite3_prepare_v2(db, "SELECT 1 FROM Confuga.Option;", -1, &stmt, NULL));
		sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
		version = 0;
	} else {
		sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
		version = sqlite3_column_int64(stmt, 0);
		sqlcatch(sqlite3_finalize(stmt); stmt = NULL);
	}

	if (version == CONFUGA_DB_VERSION) {
		goto done;
	} else if (version > CONFUGA_DB_VERSION) {
		fatal("This version of Confuga is too old for this database version (%d)", version);
	}

	switch (version) {
		case 0: {
			static const char SQL[] =
				"CREATE TABLE Confuga.State ("
				"	key TEXT PRIMARY KEY,"
				"	value NOT NULL"
				") WITHOUT ROWID;" /* SQLite optimization */
				"INSERT INTO Confuga.State (key, value)"
				"	SELECT key, value FROM Confuga.Option;"
				"DROP TABLE Confuga.Option;"
				"CREATE TABLE Confuga.DeadReplica ("
				"	fid BLOB NOT NULL,"
				"	sid INTEGER NOT NULL REFERENCES StorageNode (id),"
				"	PRIMARY KEY (fid, sid)"
				");"
				;

			debug(D_DEBUG, "upgrading db to v1");
			sqlcatchexec(db,SQL);
		}
				/* falls through */
		case 1: {
			static const char SQL[] =
				"DROP VIEW Confuga.StorageNodeActive;"
				"DROP VIEW Confuga.StorageNodeAlive;"
				"DROP TRIGGER Confuga.StorageNode_UpdateTrigger;"
				"CREATE TABLE Confuga.NewStorageNode ("
				"	id INTEGER PRIMARY KEY,"
				"	authenticated INTEGER DEFAULT 0 NOT NULL,"
				"	hostport TEXT,"
				"	password BLOB,"
				"	root TEXT NOT NULL DEFAULT '" CONFUGA_SN_ROOT_DEFAULT "',"
				"	state TEXT NOT NULL DEFAULT 'BUILDING' REFERENCES StorageNodeState (state),"
				"	ticket BLOB,"
				"	time_authenticated DATETIME,"
				"	time_create DATETIME NOT NULL DEFAULT (strftime('%s', 'now')),"
				"	time_delete DATETIME,"
				"	time_lastcontact DATETIME,"
				"	time_ticket DATETIME,"
				"	time_update DATETIME NOT NULL DEFAULT (strftime('%s', 'now')),"
				"	uuid TEXT UNIQUE,"
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
				"INSERT INTO Confuga.NewStorageNode (id, hostport, state, root, ticket, time_create, time_delete, time_update, address, avail, backend, bytes_read, bytes_written, cpu, cpus, lastheardfrom, load1, load5, load15, memory_avail, memory_total, minfree, name, opsys, opsysversion, owner, port, starttime, total, total_ops, url, version)"
				"       SELECT id, hostport, CASE WHEN initialized THEN 'ONLINE' ELSE 'BUILDING' END, root, ticket, time_create, time_delete, time_update, address, avail, backend, bytes_read, bytes_written, cpu, cpus, lastheardfrom, load1, load5, load15, memory_avail, memory_total, minfree, name, opsys, opsysversion, owner, port, starttime, total, total_ops, url, version FROM Confuga.StorageNode"
				";"
				"DROP TABLE Confuga.StorageNode;"
				"ALTER TABLE Confuga.NewStorageNode RENAME TO StorageNode;"
				"CREATE TRIGGER Confuga.StorageNode_Trigger1"
				"	AFTER UPDATE ON StorageNode"
				"	FOR EACH ROW"
				"	BEGIN"
				"		UPDATE StorageNode SET time_update = (strftime('%s', 'now')) WHERE id = NEW.id;"
				"	END;"
				"CREATE TRIGGER Confuga.StorageNode_Trigger2"
				"	AFTER UPDATE OF hostport ON StorageNode"
				"	FOR EACH ROW"
				"	WHEN (OLD.hostport != NEW.hostport)"
				"	BEGIN"
				"		UPDATE StorageNode SET authenticated = 0 WHERE id = NEW.id;"
				"	END;"
				"CREATE TRIGGER Confuga.StorageNode_Trigger3"
				"	BEFORE UPDATE OF root ON StorageNode"
				"	FOR EACH ROW"
				"	BEGIN"
				"		SELECT RAISE(ABORT, 'cannot update immutable column \"root\" of StorageNode');"
				"	END;"
				"CREATE TABLE Confuga.StorageNodeState ("
				"	state TEXT PRIMARY KEY,"
				"	active INTEGER NOT NULL"
				") WITHOUT ROWID;"
				"INSERT INTO Confuga.StorageNodeState (state, active) VALUES"
				"	('BUILDING', 0),"
				"	('FAULTED', 0),"
				"	('OFFLINE', 0),"
				"	('ONLINE', 1),"
				"	('REMOVING', 0)"
				"	;"
				"CREATE VIEW Confuga.StorageNodeAlive AS"
				"	SELECT StorageNode.*"
				"		FROM StorageNode"
				"		WHERE uuid IS NOT NULL AND lastheardfrom IS NOT NULL AND strftime('%s', 'now', '-15 minutes') <= lastheardfrom;"
				"CREATE VIEW Confuga.StorageNodeAuthenticated AS"
				"	SELECT StorageNodeAlive.*"
				"		FROM StorageNodeAlive"
				"		WHERE authenticated AND strftime('%s', 'now', '-15 minutes') < time_authenticated;"
				"CREATE VIEW Confuga.StorageNodeActive AS"
				"	SELECT StorageNodeAuthenticated.*"
				"		FROM StorageNodeAuthenticated JOIN StorageNodeState ON StorageNodeAuthenticated.state = StorageNodeState.state"
				"		WHERE StorageNodeState.active;"
				;

			debug(D_DEBUG, "upgrading db to v2");
			sqlcatchexec(db,SQL);
		}
				/* falls through */
		default: {
			static const char SQL[] =
				"INSERT OR REPLACE INTO Confuga.State (key, value)"
				"	VALUES ('db-version', " xstr(CONFUGA_DB_VERSION) ");"
				;
			sqlcatchexec(db,SQL);
			break;
		}
	}

done:
	sqlcatch(sqlite3_prepare_v2(db, "PRAGMA Confuga.foreign_key_check;", -1, &stmt, NULL));
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		const char *tblname = (const char *)sqlite3_column_text(stmt, 0);
		int rowid = sqlite3_column_int64(stmt, 1);
		const char *ref =(const char *)sqlite3_column_text(stmt, 2);
		debug(D_DEBUG, "foreign key failure: %s[%d] references table %s", tblname, rowid, ref);
		CATCH(EIO);
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_exec(db, "END TRANSACTION; PRAGMA foreign_keys=ON;", NULL, NULL, NULL));

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	sqlend(db);
	if (rc) {
		fatal("failed to upgrade database: %s", strerror(rc));
	}
	return rc;
}

static int dbload (confuga *C)
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
		"CREATE TABLE Confuga.State ("
		"	key TEXT PRIMARY KEY,"
		"	value NOT NULL"
		") WITHOUT ROWID;" /* SQLite optimization */
		"INSERT INTO Confuga.State VALUES"
		"	('id', (PRINTF('confuga:%s', UPPER(HEX(RANDOMBLOB(20))))))," /* Confuga FS GUID */
		"	('db-version', " xstr(CONFUGA_DB_VERSION) ")"
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
		"CREATE TABLE Confuga.DeadReplica ("
		"   fid BLOB NOT NULL,"
		"   sid INTEGER NOT NULL REFERENCES StorageNode (id),"
		"	PRIMARY KEY (fid, sid)"
		");"
		"CREATE TABLE Confuga.StorageNode ("
		"	id INTEGER PRIMARY KEY,"
		/* Confuga fields (some overlap with Catalog fields) */
		"	authenticated INTEGER DEFAULT 0 NOT NULL,"
		"	hostport TEXT,"
		"	password BLOB,"
		"	root TEXT NOT NULL DEFAULT '" CONFUGA_SN_ROOT_DEFAULT "',"
		"	state TEXT NOT NULL DEFAULT 'BUILDING' REFERENCES StorageNodeState (state),"
		"	ticket BLOB," /* ticket currently used by SN */
		"	time_authenticated DATETIME,"
		"	time_create DATETIME NOT NULL DEFAULT (strftime('%s', 'now')),"
		"	time_delete DATETIME,"
		"	time_lastcontact DATETIME,"
		"	time_ticket DATETIME,"
		"	time_update DATETIME NOT NULL DEFAULT (strftime('%s', 'now')),"
		"	uuid TEXT UNIQUE,"
		/* Catalog fields */
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
		"CREATE TRIGGER Confuga.StorageNode_Trigger1"
		"	AFTER UPDATE ON StorageNode"
		"	FOR EACH ROW"
		"	BEGIN"
		"		UPDATE StorageNode SET time_update = (strftime('%s', 'now')) WHERE id = NEW.id;"
		"	END;"
		"CREATE TRIGGER Confuga.StorageNode_Trigger2"
		"	AFTER UPDATE OF hostport ON StorageNode"
		"	FOR EACH ROW"
		"	WHEN (OLD.hostport != NEW.hostport)"
		"	BEGIN"
		"		UPDATE StorageNode SET authenticated = 0 WHERE id = NEW.id;"
		"	END;"
		"CREATE TRIGGER Confuga.StorageNode_Trigger3"
		"	BEFORE UPDATE OF root ON StorageNode"
		"	FOR EACH ROW"
		"	BEGIN"
		"		SELECT RAISE(ABORT, 'cannot update immutable column \"root\" of StorageNode');"
		"	END;"
		"CREATE TABLE Confuga.StorageNodeState ("
		"	state TEXT PRIMARY KEY,"
		"	active INTEGER NOT NULL"
		") WITHOUT ROWID;"
		"INSERT INTO Confuga.StorageNodeState (state, active) VALUES"
		"	('BUILDING', 0),"
		"	('FAULTED', 0),"
		"	('OFFLINE', 0),"
		"	('ONLINE', 1),"
		"	('REMOVING', 0)"
		"	;"
		IMMUTABLE("Confuga.StorageNodeState")
		"CREATE VIEW Confuga.StorageNodeAlive AS"
		"	SELECT StorageNode.*"
		"		FROM StorageNode"
		"		WHERE uuid IS NOT NULL AND lastheardfrom IS NOT NULL AND strftime('%s', 'now', '-15 minutes') <= lastheardfrom;"
		"CREATE VIEW Confuga.StorageNodeAuthenticated AS"
		"	SELECT StorageNodeAlive.*"
		"		FROM StorageNodeAlive"
		"		WHERE authenticated AND strftime('%s', 'now', '-15 minutes') < time_authenticated;"
		"CREATE VIEW Confuga.StorageNodeActive AS"
		"	SELECT StorageNodeAuthenticated.*"
		"		FROM StorageNodeAuthenticated JOIN StorageNodeState ON StorageNodeAuthenticated.state = StorageNodeState.state"
		"		WHERE StorageNodeState.active;"
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
		char *errmsg = NULL;
		rc = sqlite3_exec(db, SQL, NULL, NULL, &errmsg); /* Ignore any errors. */
		if (rc) {
			sqlite3_exec(db, "ROLLBACK TRANSACTION;", NULL, NULL, NULL);
			if (strstr(errmsg, "already exists")) {
				CATCH(dbupgrade(C));
			} else {
				debug(D_DEBUG, "[%s:%d] sqlite3 error: %d `%s': %s", __FILE__, __LINE__, rc, sqlite3_errstr(rc), errmsg);
			}
		}
		sqlite3_free(errmsg);
		sqlcatch(rc);
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
		"SELECT value FROM Confuga.State WHERE key = 'id';"
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

	CATCH(dbload(C));

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
	int ticketfd = -1;
	FILE *key = NULL;
	buffer_t Bout[1], Berr[1];
	int status;

	debug(D_CONFUGA, "creating new authentication ticket");

	buffer_init(Bout);
	buffer_init(Berr);
	CATCHUNIX(shellcode("openssl genrsa " stringify(CONFUGA_TICKET_BITS), NULL, NULL, 0, Bout, Berr, &status));
	if (status) {
		debug(D_CONFUGA, "openssl failed with exit status %d, stderr:\n%s", status, buffer_tostring(Berr));
		CATCH(EIO);
	}
	sha1_buffer(buffer_tostring(Bout), buffer_pos(Bout), (unsigned char *)&C->ticket);

	CATCHUNIX(ticketfd = openat(C->rootfd, "ticket", O_CREAT|O_NOCTTY|O_TRUNC|O_WRONLY, S_IRUSR|S_IWUSR));
	CATCHUNIX(full_write(ticketfd, buffer_tostring(Bout), buffer_pos(Bout)));

	rc = 0;
	goto out;
out:
	if (ticketfd >= 0)
		close(ticketfd);
	if (key)
		pclose(key);
	buffer_free(Bout);
	buffer_free(Berr);
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

	if (pattern_match(uri, "^confuga://([^?]+)(.-)$", &root, &options) >= 0) {
		const char *rest = options;
		size_t n;
		CATCH(confugaN_init(C, root));
		CATCH(confugaI_dbload(C, NULL));

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
		CATCH(confugaN_init(C, uri));
		CATCH(confugaI_dbload(C, NULL));
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
	C->operations = 0;
	C->rootfd = -1;
	C->nsrootfd = -1;

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
	if (rc) {
		if (C->rootfd >= 0)
			close(C->rootfd);
		if (C->nsrootfd >= 0)
			close(C->nsrootfd);
		free(C);
	}
	return rc;
}

CONFUGA_API int confuga_getid (confuga *C, char **id)
{
	static const char SQL[] =
		"SELECT value FROM Confuga.State WHERE key = 'id';"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	*id = NULL;

	sqlcatch(sqlite3_prepare_v2(db, current, strlen(current)+1, &stmt, &current));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_ROW);
	*id = strdup((const char *)sqlite3_column_text(stmt, 0));
	if (!*id) CATCH(ENOMEM);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	if (rc)
		*id = (free(*id), NULL);
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
	unsigned long delay = 1;

	time_t catalog_sync = 0;
	time_t ticket_generated = 0;
	time_t gc = 0;

	while (1) {
		time_t now = time(NULL);
		uint64_t prevops = C->operations;

		if (ticket_generated+TICKET_REFRESH <= now) {
			CATCH(setup_ticket(C));
			ticket_generated = now;
		}

		if (catalog_sync+15 <= now) {
			confugaS_catalog_sync(C);
			catalog_sync = now;
		}

		if (gc+120 <= now) {
			confugaG_fullgc(C);
			gc = now;
		}

		confugaJ_schedule(C);
		confugaR_manager(C);
		confugaS_manager(C);

		if (prevops == C->operations) {
			struct timeval tv = {.tv_sec = delay / 1000000, .tv_usec = delay % 1000000};
			if (tv.tv_sec >= 2) {
				tv.tv_sec = 2;
				tv.tv_usec = 0;
			} else {
				delay <<= 1;
			}
			while (select(0, 0, 0, 0, &tv) == -1 && errno == EINTR) {}
		} else {
			delay = 1;
		}
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
	CATCHUNIX(fstatfs64(C->rootfd, &fsinfo));
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
