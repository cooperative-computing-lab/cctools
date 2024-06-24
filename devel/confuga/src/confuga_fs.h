/* Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#ifndef CONFUGA_FS_H
#define CONFUGA_FS_H

#include "confuga.h"

#include "sqlite3.h"

#if defined(__GNUC__) && __GNUC__ >= 3 && __GNUC_MINOR__ >= 4
#  define CONFUGA_IAPI __attribute__((__visibility__("hidden")))
#else
#  define CONFUGA_IAPI
#endif

struct confuga {
	sqlite3 *db;
	int      rootfd;
	int      nsrootfd;
	char     root[CONFUGA_PATH_MAX];

	uint64_t concurrency;
	uint64_t pull_threshold;
	int replication;
	uint64_t replication_n;
	int scheduler;
	uint64_t scheduler_n;

	const char *catalog_hosts;

	unsigned char ticket[20]; /* SHA1 of ticket */

	time_t job_stats;
	time_t transfer_stats;

	uint64_t operations;
};

struct confuga_host {
	char hostport[256+8]; /* Maximum length of FQDN is 255 octets per RFC, +8 for port */
	char root[CONFUGA_PATH_MAX];
};

#define CONFUGA_SN_ROOT_DEFAULT "/.confuga"

#define CONFUGA_TICKET_BITS 1024
#define _stringify(D) #D
#define stringify(D) _stringify(D)

CONFUGA_IAPI int confugaI_dbload (confuga *C, sqlite3 *attachdb);
CONFUGA_IAPI int confugaI_dbclose (confuga *C);

enum CONFUGA_FILE_TYPE {
	CONFUGA_FILE,
	CONFUGA_META,
};

CONFUGA_IAPI int confugaN_init (confuga *C, const char *root);
CONFUGA_IAPI int confugaN_lookup (confuga *C, int dirfd, const char *basename, confuga_fid_t *fid, confuga_off_t *size, enum CONFUGA_FILE_TYPE *type, int *nlink);
CONFUGA_IAPI int confugaN_update (confuga *C, int dirfd, const char *basename, confuga_fid_t fid, confuga_off_t size, int flags);

CONFUGA_IAPI int confugaF_extract (confuga *C, confuga_fid_t *fid, const char *str, const char **endptr);
CONFUGA_IAPI int confugaF_renew (confuga *C, confuga_fid_t fid);
CONFUGA_IAPI int confugaF_set (confuga *C, confuga_fid_t *fid, const void *id);
#define confugaF_id(fid) ((fid).id)
#define confugaF_size(fid) (sizeof (fid).id)

CONFUGA_IAPI int confugaG_fullgc (confuga *C);

CONFUGA_IAPI int confugaR_delete (confuga *C, confuga_sid_t sid, confuga_fid_t fid);
CONFUGA_IAPI int confugaR_replicate (confuga *C, confuga_fid_t fid, confuga_sid_t sid, const char *tag, time_t stoptime);
CONFUGA_IAPI int confugaR_register (confuga *C, confuga_fid_t fid, confuga_off_t size, confuga_sid_t sid);
CONFUGA_IAPI int confugaR_manager (confuga *C);

CONFUGA_IAPI int confugaS_catalog (confuga *C, const char *catalog);
CONFUGA_IAPI int confugaS_catalog_sync (confuga *C);
CONFUGA_IAPI int confugaS_manager (confuga *C);
CONFUGA_IAPI int confugaS_node_insert (confuga *C, const char *hostport, const char *root);

CONFUGA_IAPI int confugaJ_schedule (confuga *C);

#define CONFUGA_DB_VERSION  2

#define str(s) #s
#define xstr(s) str(s)

#endif

/* vim: set noexpandtab tabstop=8: */
