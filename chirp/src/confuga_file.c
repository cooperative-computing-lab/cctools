/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "confuga_fs.h"

#include "catch.h"
#include "chirp_sqlite.h"
#include "debug.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

int confugaF_extract (confuga *C, confuga_fid_t *fid, const char *str, const char **endptr)
{
	int rc;

	/* extract the File ID from interpolated serv_path */
	size_t k;
	for (k = 0; k < sizeof(fid->id); k += 1, str += 2) {
		char byte[3] = {str[0], str[1], '\0'};
		char *e;
		unsigned long value = strtoul(byte, &e, 16);
		if (e == &byte[2]) {
			fid->id[k] = value;
		} else {
			CATCH(EINVAL);
		}
	}

	if (endptr)
		*endptr = str;

	rc = 0;
	goto out;
out:
	return rc;
}

int confugaF_set (confuga *C, confuga_fid_t *fid, const void *id)
{
	memcpy(fid->id, id, confugaF_size(*fid));
	return 0;
}

CONFUGA_IAPI int confugaF_renew (confuga *C, confuga_fid_t fid)
{
	static const char SQL[] =
		"UPDATE Confuga.File"
		"	SET time_health = (strftime('%s', 'now'))"
		"	WHERE id = ?"
		";"
		;

	int rc;
	sqlite3 *db = C->db;
	sqlite3_stmt *stmt = NULL;
	const char *current = SQL;

	debug(D_DEBUG, "renewing File " CONFUGA_FID_DEBFMT, CONFUGA_FID_PRIARGS(fid));

	sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
	sqlcatch(sqlite3_bind_blob(stmt, 1, confugaF_id(fid), confugaF_size(fid), SQLITE_STATIC));
	sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
	sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

	rc = 0;
	goto out;
out:
	sqlite3_finalize(stmt);
	return rc;
}

/* vim: set noexpandtab tabstop=8: */
