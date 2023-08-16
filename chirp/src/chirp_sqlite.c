/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "buffer.h"
#include "catch.h"
#include "debug.h"
#include "json.h"
#include "json_aux.h"

#include "sqlite3.h"

#include <errno.h>
#include <float.h>
#include <string.h>

int chirp_sqlite3_column_jsonify(sqlite3_stmt *stmt, int n, buffer_t *B)
{
	int rc;

	switch (sqlite3_column_type(stmt, n)) {
		case SQLITE_NULL:
			CATCHUNIX(buffer_putliteral(B, "null"));
			break;
		case SQLITE_INTEGER:
			CATCHUNIX(buffer_putfstring(B, "%" PRId64, (int64_t) sqlite3_column_int64(stmt, n)));
			break;
		case SQLITE_FLOAT:
			CATCHUNIX(buffer_putfstring(B, "%.*e", DBL_DIG, sqlite3_column_double(stmt, n)));
			break;
		case SQLITE_TEXT:
			CATCHUNIX(buffer_putliteral(B, "\""));
			CATCHUNIX(jsonA_escapestring(B, (const char *)sqlite3_column_text(stmt, n)));
			CATCHUNIX(buffer_putliteral(B, "\""));
			break;
		default:
			abort();
	}

	rc = 0;
	goto out;
out:
	return rc;
}

int chirp_sqlite3_row_jsonify(sqlite3_stmt *stmt, buffer_t *B)
{
	int rc;
	int i, first;

	CATCHUNIX(buffer_putliteral(B, "{"));
	for (i = 0, first = 1; i < sqlite3_column_count(stmt); i++, first = 0) {
		if (!first)
			CATCHUNIX(buffer_putliteral(B, ","));
		CATCHUNIX(buffer_putfstring(B, "\"%s\":", sqlite3_column_name(stmt, i)));
		chirp_sqlite3_column_jsonify(stmt, i, B);
	}
	CATCHUNIX(buffer_putliteral(B, "}"));

	rc = 0;
	goto out;
out:
	return rc;
}

/* vim: set noexpandtab tabstop=8: */
