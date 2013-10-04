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
		debug(D_DEBUG, "[%s:%d] sqlite3 error: %d `%s': %s", __FILE__, __LINE__, rc, sqlite3_errstr(rc), errmsg);\
		sqlite3_free(errmsg);\
		if (rc == SQLITE_CONSTRAINT)\
			rc = EINVAL;\
		else if (rc == SQLITE_BUSY)\
			rc = EAGAIN;\
		else\
			rc = EIO;\
		goto out;\
	}\
} while (0)

#define sqlcatch(S) \
do {\
	rc = S;\
	if (rc) {\
		debug(D_DEBUG, "[%s:%d] sqlite3 error: %d `%s'", __FILE__, __LINE__, rc, sqlite3_errstr(rc));\
		if (rc == SQLITE_CONSTRAINT)\
			rc = EINVAL;\
		else if (rc == SQLITE_BUSY)\
			rc = EAGAIN;\
		else\
			rc = EIO;\
		sqlite3_finalize(stmt);\
		stmt = NULL;\
		goto out;\
	}\
} while (0)

#define sqlcatchcode(S, code) \
do {\
	rc = S;\
	if (rc != code) {\
		debug(D_DEBUG, "[%s:%d] sqlite3 error: %d `%s'", __FILE__, __LINE__, rc, sqlite3_errstr(rc));\
		if (rc == SQLITE_CONSTRAINT)\
			rc = EINVAL;\
		else if (rc == SQLITE_BUSY)\
			rc = EAGAIN;\
		else\
			rc = EIO;\
		sqlite3_finalize(stmt);\
		stmt = NULL;\
		goto out;\
	}\
} while (0)

/* FIXME: we already rollback as part of any insert/update? We don't need this anymore? */
#define sqlend(db) \
do {\
	if (rc) {\
		char *errmsg;\
		int erc = sqlite3_exec((db), "ROLLBACK TRANSACTION;", NULL, NULL, &errmsg);\
		if (erc) {\
			debug(D_DEBUG, "[%s:%d] sqlite3 error: %d `%s': %s", __FILE__, __LINE__, erc, sqlite3_errstr(erc), errmsg);\
			sqlite3_free(errmsg);\
		}\
	}\
} while (0)

#endif

/* vim: set noexpandtab tabstop=4: */
