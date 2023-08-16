/*
 * Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#ifndef CHIRP_SQLITE_H
#define CHIRP_SQLITE_H

#include "buffer.h"
#include "catch.h"
#include "debug.h"

#include "sqlite3.h"

#define CHIRP_SQLITE_TIMEOUT (5000)

#define sqlcatchexec(db,sql) \
do {\
	sqlite3 *_db = (db);\
	char *errmsg;\
	rc = sqlite3_exec(_db, (sql), NULL, NULL, &errmsg);\
	if (rc) {\
		if (rc == SQLITE_BUSY) {\
			rc = EAGAIN;\
		} else {\
			debug(D_DEBUG, "[%s:%d] sqlite3 error: %d `%s': %s", __FILE__, __LINE__, rc, sqlite3_errstr(rc), errmsg);\
			if (rc == SQLITE_CONSTRAINT) {\
				rc = EINVAL;\
			} else {\
				rc = EIO;\
			}\
		}\
		sqlite3_free(errmsg);\
		goto out;\
	}\
} while (0)

#define sqlcatch(S) \
do {\
	rc = S;\
	if (rc) {\
		if (rc == SQLITE_BUSY) {\
			rc = EAGAIN;\
		} else {\
			debug(D_DEBUG, "[%s:%d] sqlite3 error: %d `%s': %s", __FILE__, __LINE__, rc, sqlite3_errstr(rc), sqlite3_errmsg(db));\
			if (rc == SQLITE_CONSTRAINT) {\
				rc = EINVAL;\
			} else {\
				rc = EIO;\
			}\
		}\
		sqlite3_finalize(stmt);\
		stmt = NULL;\
		goto out;\
	}\
} while (0)

#define sqlcatchcode(S, code) \
do {\
	rc = S;\
	if (rc != code) {\
		if (rc == SQLITE_BUSY) {\
			rc = EAGAIN;\
		} else {\
			debug(D_DEBUG, "[%s:%d] sqlite3 error: %d `%s': %s", __FILE__, __LINE__, rc, sqlite3_errstr(rc), sqlite3_errmsg(db));\
			if (rc == SQLITE_CONSTRAINT) {\
				rc = EINVAL;\
			} else {\
				rc = EIO;\
			}\
		}\
		sqlite3_finalize(stmt);\
		stmt = NULL;\
		goto out;\
	}\
} while (0)

/* This macro is part of the prologue for any procedure that begins/ends a
 * transaction. We cannot simply put the ROLLBACK conflict resolution
 * constraint in relevant SQL write operations. The reason for this is because
 * not all errors are in SQLite. For example, the code may do a waitpid which
 * fails, which requires the entire operation to fail. This macro does a
 * rollback of the entire transaction on error, so it handles that case.
 */
#define sqlend(db) \
do {\
	sqlite3 *_db = (db);\
	if (_db && rc) {\
		char *errmsg;\
		int erc = sqlite3_exec(_db, "ROLLBACK TRANSACTION;", NULL, NULL, &errmsg);\
		if (erc) {\
			if (erc == SQLITE_ERROR /* cannot rollback because no transaction is active */) {\
				; /* do nothing */\
			} else {\
				debug(D_DEBUG, "[%s:%d] sqlite3 error: %d `%s': %s", __FILE__, __LINE__, erc, sqlite3_errstr(erc), errmsg);\
			}\
			sqlite3_free(errmsg);\
		}\
	}\
} while (0)

#define sqlendsavepoint(savepoint) \
do {\
	if (rc) {\
		char *errmsg;\
		int erc = sqlite3_exec(db, "ROLLBACK TRANSACTION TO SAVEPOINT " #savepoint "; RELEASE SAVEPOINT " #savepoint ";", NULL, NULL, &errmsg);\
		if (erc) {\
			if (erc == SQLITE_ERROR /* cannot rollback because no transaction is active */) {\
				; /* do nothing */\
			} else {\
				debug(D_DEBUG, "[%s:%d] sqlite3 error: %d `%s': %s", __FILE__, __LINE__, erc, sqlite3_errstr(erc), errmsg);\
			}\
			sqlite3_free(errmsg);\
		}\
	}\
} while (0)

#define IMMUTABLE(T) \
		"CREATE TRIGGER " T "ImmutableI BEFORE INSERT ON " T " FOR EACH ROW" \
		"    BEGIN" \
		"        SELECT RAISE(ABORT, 'cannot insert rows of immutable table');" \
		"    END;" \
		"CREATE TRIGGER " T "ImmutableU BEFORE UPDATE ON " T " FOR EACH ROW" \
		"    BEGIN" \
		"        SELECT RAISE(ABORT, 'cannot update rows of immutable table');" \
		"    END;" \
		"CREATE TRIGGER " T "ImmutableD BEFORE DELETE ON " T " FOR EACH ROW" \
		"    BEGIN" \
		"        SELECT RAISE(ABORT, 'cannot delete rows of immutable table');" \
		"    END;"

#endif

int chirp_sqlite3_column_jsonify(sqlite3_stmt *stmt, int n, buffer_t *B);
int chirp_sqlite3_row_jsonify(sqlite3_stmt *stmt, buffer_t *B);

/* vim: set noexpandtab tabstop=8: */
