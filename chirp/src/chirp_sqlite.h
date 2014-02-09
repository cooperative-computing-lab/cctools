/*
 * Copyright (C) 2013- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#ifndef CHIRP_SQLITE_H
#define CHIRP_SQLITE_H

#include <sqlite3.h>

#define sqlcatchexec(db,sql) \
do {\
	char *errmsg;\
	rc = sqlite3_exec((db), (sql), NULL, NULL, &errmsg);\
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

#define sqlend(db) \
do {\
	if (rc) {\
		char *errmsg;\
		int erc = sqlite3_exec((db), "ROLLBACK TRANSACTION;", NULL, NULL, &errmsg);\
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

#endif

/* vim: set noexpandtab tabstop=4: */
