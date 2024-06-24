#include "confuga.h"
#include "confuga_fs.h"

#include "catch.h"
#include "chirp_sqlite.h"
#include "compat-at.h"
#include "debug.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <assert.h>

#ifndef O_CLOEXEC
#	define O_CLOEXEC 0
#endif
#ifndef O_DIRECTORY
#	define O_DIRECTORY 0
#endif
#ifndef O_NOFOLLOW
#	define O_NOFOLLOW 0
#endif

static int gcfilestore (confuga *C, int *fd)
{
	int rc;
	int storefd = -1;
	char current[PATH_MAX] = "";
	char next[PATH_MAX] = "";

	CATCHUNIX(storefd = openat(C->rootfd, "store/.", O_CLOEXEC|O_NOCTTY|O_RDONLY, 0));
	CATCHUNIX(flock(storefd, LOCK_EX)); /* released by close */

	/* recover previous GC */
	rc = openat(storefd, "file.gc/.", O_CLOEXEC|O_NOCTTY|O_RDONLY, 0);
	if (rc >= 0) {
		debug(D_DEBUG, "recovering from previous GC");
		*fd = rc;
		rc = 0;
		goto out;
	}

	CATCHUNIX(readlinkat(storefd, "file", current, PATH_MAX));
	assert(rc < PATH_MAX);
	if (strcmp(current, "file.0") == 0) {
		strcpy(next, "file.1");
	} else if (strcmp(current, "file.1") == 0) {
		strcpy(next, "file.0");
	} else assert(0);

	CATCHUNIXIGNORE(mkdirat(storefd, next, S_IRWXU), EEXIST);
	CATCHUNIXIGNORE(unlinkat(storefd, "file.next", 0), ENOENT);
	CATCHUNIX(symlinkat(next, storefd, "file.next"));
	CATCHUNIX(renameat(storefd, "file.next", storefd, "file"));
	CATCHUNIX(renameat(storefd, current, storefd, "file.gc"));

	CATCHUNIX(openat(storefd, "file.gc/.", O_CLOEXEC|O_NOCTTY|O_RDONLY, 0));
	*fd = rc;

	rc = 0;
	goto out;
out:
	CLOSE_FD(storefd);
	if (rc) {
		CLOSE_FD(*fd);
	}
	return rc;

}

static int gcreplicas (confuga *C)
{
	static const char SQL[] =
		"BEGIN TRANSACTION;"
		"SELECT File.id, Replica.sid"
		"	FROM"
		"		Confuga.File"
		"		JOIN Confuga.Replica ON File.id = Replica.fid"
		"	WHERE File.time_health IS NOT NULL AND File.time_health < (SELECT value FROM Confuga.State WHERE key = 'last-gc')"
		";"
		"INSERT OR REPLACE INTO Confuga.State (key, value)"
		"	VALUES ('last-gc', (strftime('%s', 'now')));"
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
	while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
		confuga_fid_t fid;
		confuga_sid_t sid;
		CATCH(confugaF_set(C, &fid, sqlite3_column_blob(stmt, 0)));
		sid = sqlite3_column_int64(stmt, 1);
		CATCH(confugaR_delete(C, sid, fid));
	}
	sqlcatchcode(rc, SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
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

CONFUGA_IAPI int confugaG_fullgc (confuga *C)
{
	int rc;
	int fd = -1;
	int filefd = -1;
	DIR *dir = NULL;

	debug(D_CONFUGA, "performing full GC");

	CATCH(gcfilestore(C, &fd));
	CATCHUNIX(filefd = openat(C->rootfd, "store/file/.", O_CLOEXEC|O_NOCTTY|O_RDONLY, 0));

	dir = fdopendir(fd);
	CATCHUNIX(dir ? 0 : -1);
	fd = -1;

	while (1) {
		errno = 0;
		struct dirent *dent = readdir(dir);
		if (dent) {
			confuga_fid_t fid;
			confuga_off_t size;
			enum CONFUGA_FILE_TYPE type;
			int nlink;

			if (strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0)
				continue;

			/* On error, just print a message and come back later... */
			assert(strchr(dent->d_name, '/') == NULL);

			rc = confugaN_lookup(C, dirfd(dir), dent->d_name, &fid, &size, &type, &nlink);
			if (rc) {
				debug(D_DEBUG, "lookup failed: %s", strerror(errno));
				continue;
			}
			assert(type == CONFUGA_FILE);

			/* renew the File even when it's dead so that its time_health is never NULL */
			rc = confugaF_renew(C, fid);
			if (rc) {
				debug(D_DEBUG, "renew failed: %s", strerror(errno));
				continue;
			}

			if (nlink > 1) {
				debug(D_DEBUG, "found living inode %s", dent->d_name);

				rc = renameat(dirfd(dir), dent->d_name, filefd, dent->d_name);
				if (rc) {
					debug(D_DEBUG, "renameat failed: %s", strerror(errno));
					continue;
				}
			} else {
				debug(D_DEBUG, "found dead inode %s", dent->d_name);

				rc = unlinkat(dirfd(dir), dent->d_name, 0);
				if (rc) {
					debug(D_DEBUG, "unlinkat failed: %s", strerror(errno));
					continue;
				}
			}
		} else {
			CATCH(errno);
			break;
		}
	}
	CATCH(rc);

	CATCH(unlinkat(C->rootfd, "store/file.gc", AT_REMOVEDIR));

	CATCH(gcreplicas(C));

	rc = 0;
	goto out;
out:
	CLOSE_FD(fd);
	CLOSE_FD(filefd);
	return rc;
}

/* vim: set noexpandtab tabstop=8: */
