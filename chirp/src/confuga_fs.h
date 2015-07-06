/* Copyright (C) 2014- The University of Notre Dame
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
	char     root[CONFUGA_PATH_MAX];

	uint64_t concurrency;
	uint64_t pull_threshold;
	int replication;
	uint64_t replication_n;
	int scheduler;
	uint64_t scheduler_n;

	char     catalog_host[256]; /* FQDN is max 255 bytes */
	uint16_t catalog_port;

	unsigned char ticket[20]; /* SHA1 of ticket */

	time_t catalog_sync;
	time_t sn_heartbeat;
	time_t job_stats;
	time_t transfer_stats;
};

#define CONFUGA_TICKET_BITS 1024
#define _stringify(D) #D
#define stringify(D) _stringify(D)

CONFUGA_IAPI int confugaI_dbload (confuga *C, sqlite3 *attachdb);
CONFUGA_IAPI int confugaI_dbclose (confuga *C);

CONFUGA_IAPI int confugaR_replicate (confuga *C, confuga_fid_t fid, confuga_sid_t sid, const char *tag, time_t stoptime);
CONFUGA_IAPI int confugaR_register (confuga *C, confuga_fid_t fid, confuga_off_t size, confuga_sid_t sid);
CONFUGA_IAPI int confugaR_manager (confuga *C);

CONFUGA_IAPI int confugaS_catalog (confuga *C, const char *catalog);
CONFUGA_IAPI int confugaS_catalog_sync (confuga *C);
CONFUGA_IAPI int confugaS_setup (confuga *C);
CONFUGA_IAPI int confugaS_node_insert (confuga *C, const char *hostport, const char *root);

CONFUGA_IAPI int confugaJ_schedule (confuga *C);

CONFUGA_IAPI int confugaN_init (confuga *C);
CONFUGA_IAPI int confugaN_special_update (confuga *C, const char *path, confuga_fid_t fid, confuga_off_t size);

#define streql(s1,s2) (strcmp(s1,s2) == 0)

#endif

/* vim: set noexpandtab tabstop=4: */
