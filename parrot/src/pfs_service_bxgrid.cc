/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef HAS_BXGRID

#include "pfs_table.h"
#include "pfs_service.h"

extern "C" {
#include "chirp_global.h"
#include "debug.h"
#include "domain_name.h"
#include "hash_table.h"
#include "path.h"
#include "random.h"
#include "stringtools.h"
#include "xxmalloc.h"
}

#include <string.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <utime.h>
#include <dlfcn.h>
#include <pwd.h>
#include <grp.h>
#include <sys/statfs.h>

extern "C" {
#if defined(HAS_MYSQL_MYSQL_H)
#include <mysql/mysql.h>
#elif defined(HAS_MYSQL_H)
#include <mysql.h>
#endif
}

#define	BXGRID_TIMEOUT	2
#define BXGRID_ID_MAX	16
#define BXGRID_REG_MODE	(S_IFREG | 0400)
#define BXGRID_DIR_MODE	(S_IFDIR | 0755)

#define	BXGRID_MAKE_DIR_STAT( name, buf ) \
	pfs_service_emulate_stat((name), (buf)); \
	(buf)->st_mode = BXGRID_DIR_MODE; \
	(buf)->st_size = 0; \

#define BXGRID_END	debug(D_BXGRID,"= %d %s",(int)result,((result>=0) ? "" : strerror(errno))); return result;

extern int pfs_main_timeout;

static int bxgrid_timeout = BXGRID_TIMEOUT;

enum BXGRID_FLAGS {
	BXGRID_FILE_LIST  = 0x000001,
	BXGRID_FILE_QUERY = 0x000002,
	BXGRID_LISTABLE	  = 0x000004
};

static const char *bxgrid_dbname = "biometrics";
static const char *bxgrid_user   = "anonymous";
static const char *bxgrid_pass   = "";
static int bxgrid_cache_stat_query = 1;
static int bxgrid_cache_ftor_query = 1;
static int bxgrid_cache_rtol_query = 1;
static struct hash_table *bxgrid_stat_query_cache = NULL;
static struct hash_table *bxgrid_ftor_query_cache = NULL;
static struct hash_table *bxgrid_rtol_query_cache = NULL;
static char bxgrid_hostname[PFS_LINE_MAX];

struct bxgrid_file_info {
	int mode;
	int size;
	int mtime;
};

struct bxgrid_replica_list {
	int    nreplicas;
	char **replicas;
};

struct bxgrid_replica_location {
	char *host;
	char *path;
};

struct bxgrid_virtual_folder {
	const char	*name;
	const char	*query;
	const char	*stat_query;
	int		flags;
};

static struct bxgrid_virtual_folder BXGRID_VIRTUAL_FOLDERS[] = {
	{ "/",
	  "fileid replicaid",
	  NULL,
	  BXGRID_FILE_LIST | BXGRID_LISTABLE
	},
	{ "/fileid",
	  "SELECT fileid FROM files",
	  "SELECT size, UNIX_TIMESTAMP(files.lastcheck) FROM files LEFT JOIN replicas USING(fileid) WHERE fileid = '%s' AND replicas.state = 'OK' LIMIT 1",
	  BXGRID_FILE_QUERY
	},
	{ "/replicaid",
	  "SELECT replicaid FROM replicas",
	  "SELECT size, UNIX_TIMESTAMP(replicas.lastcheck) FROM files LEFT JOIN replicas USING(fileid) WHERE replicaid = '%s'",
	  BXGRID_FILE_QUERY
	},
	{ NULL, NULL, NULL, 0 }
};

/**
 * Find virtual folder based on path
 *
 * Returns pointer to virtual folder structure if found.
 **/

struct bxgrid_virtual_folder *bxgrid_bvf_find( const char *path )
{
	struct bxgrid_virtual_folder *bvf;

	for (bvf = BXGRID_VIRTUAL_FOLDERS; bvf->name; bvf++) {
		if (strncmp(path, bvf->name, PFS_PATH_MAX) == 0) {
			debug(D_BXGRID, "%s is a virtual folder", path);
			return bvf;
		}
	}

	debug(D_BXGRID, "%s is a not virtual folder", path);
	return NULL;
}

struct bxgrid_virtual_folder *bxgrid_bvf_find_base( const char *path )
{
	char dirname[PFS_PATH_MAX];
	path_dirname(path, (char *)dirname);
	return bxgrid_bvf_find(dirname);
}

MYSQL_RES *bxgrid_db_query( MYSQL *mysql_cxn, const char *queryfmt, ... )
{
	va_list args;
	va_start(args, queryfmt);
	char query[PFS_LINE_MAX];

	vsprintf(query, queryfmt, args);

	debug(D_BXGRID, "db_query: %s", query);
	if (mysql_query(mysql_cxn, query) < 0) {
		debug(D_BXGRID, "couldn't execute query '%s': %s", query, mysql_error(mysql_cxn));
	}

	va_end(args);
	return mysql_store_result(mysql_cxn);
}

/** Copied and pasted relevant attributes from pfs_file_chirp */
class pfs_file_bxgrid : public pfs_file
{
private:
	struct chirp_file *file;
public:
	pfs_file_bxgrid( pfs_name *name, struct chirp_file *f ) : pfs_file(name) {
		file = f;
	}

	virtual int close() {
		return chirp_global_close(file,time(0)+pfs_main_timeout);
	}

	virtual pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset ) {
		return chirp_global_pread(file,data,length,offset,time(0)+pfs_main_timeout);
	}

	virtual int fstat( struct pfs_stat *buf );

	virtual pfs_ssize_t get_size() {
		struct pfs_stat buf;
		if(this->fstat(&buf)==0) {
			return buf.st_size;
		} else {
			return -1;
		}
	}
};

#define BXGRID_QUERY_AND_CHECK( res, cxn, reterr, ... ) \
	(res) = bxgrid_db_query(cxn, __VA_ARGS__); \
	if ((res) == NULL) { debug(D_BXGRID, "query returned no results: %s", mysql_error(mysql_cxn)); return (reterr); }

#define BXGRID_FETCH_AND_CHECK( row, res, reterr ) \
	(row) = mysql_fetch_row(res); \
	if ((row) == NULL) { debug(D_BXGRID, "failed to fetch row: %s", mysql_error(mysql_cxn)); mysql_free_result(res); return (reterr); }

int bxgrid_bvf_stat( MYSQL *mysql_cxn, struct bxgrid_virtual_folder *bvf, struct pfs_name *name, struct pfs_stat *buf )
{
	MYSQL_RES  *file_res;
	MYSQL_ROW   file_row;
	const char *file_path;
	struct bxgrid_file_info *file_info = NULL;

	file_path = name->rest;
	if (bxgrid_cache_stat_query) {
		file_info = (struct bxgrid_file_info *)hash_table_lookup(bxgrid_stat_query_cache, file_path);
		debug(D_BXGRID, "%s is %s the stat_query_cache", file_path, (file_info ? "in" : "not in"));
	}

	if (!file_info) { // Cache miss or no caching
		BXGRID_QUERY_AND_CHECK(file_res, mysql_cxn, -1, bvf->stat_query, path_basename(file_path));
		BXGRID_FETCH_AND_CHECK(file_row, file_res, -1);

		if (bxgrid_cache_stat_query) {
			file_info = (struct bxgrid_file_info *)xxmalloc(sizeof(struct bxgrid_file_info));
			buf->st_mode  = file_info->mode  = BXGRID_REG_MODE;
			buf->st_size  = file_info->size  = strtol(file_row[0], NULL, 10);
			buf->st_mtime = file_info->mtime = (file_row[1] ? strtol(file_row[1], NULL, 10) : time(NULL));

			hash_table_insert(bxgrid_stat_query_cache, file_path, file_info);
		} else {
			buf->st_mode  = BXGRID_REG_MODE;
			buf->st_size  = strtol(file_row[0], NULL, 10);
			buf->st_mtime = (file_row[1] ? strtol(file_row[1], NULL, 10) : time(NULL));
		}

		mysql_free_result(file_res);
	} else { // Cache hit
		buf->st_mode  = file_info->mode;
		buf->st_size  = file_info->size;
		buf->st_mtime = file_info->mtime;
	}

	return 0;
}

#define BXGRID_REPLICAID_QUERY "SELECT replicas.replicaid, replicas.host, replicas.path FROM replicas LEFT JOIN fileservers ON replicas.host = fileservers.name WHERE fileservers.state = 'ok' AND replicas.fileid = '%s' AND replicas.state = 'OK' ORDER BY r_priority"

struct bxgrid_replica_list * bxgrid_lookup_replica_list( MYSQL *mysql_cxn, const char *fileid, int update = 0 )
{
	MYSQL_RES  *rep_res;
	MYSQL_ROW   rep_row;
	const char *replicaid;
	struct bxgrid_replica_list *replica_list = NULL;
	struct bxgrid_replica_location *replica_location;

	if (update) {
		replica_list = (struct bxgrid_replica_list*)hash_table_remove(bxgrid_ftor_query_cache, fileid);
		debug(D_BXGRID, "file %s is %s the ftor_query_cache", fileid, (replica_list ? "in" : "not in"));

		if (replica_list) {
			debug(D_BXGRID, "updating replicas for file %s in ftor_query_cache", fileid);

			for (int i = 0; i < replica_list->nreplicas; i++) {
				replicaid = replica_list->replicas[i];
				if (bxgrid_cache_rtol_query) {
					replica_location = (struct bxgrid_replica_location *)hash_table_remove(bxgrid_rtol_query_cache, replicaid);
					free(replica_location->host);
					free(replica_location->path);
					free(replica_location);
				}
				free(replica_list->replicas[i]);
			}

			free(replica_list);
			replica_list = NULL;
		}
	} else {
		replica_list = (struct bxgrid_replica_list*)hash_table_lookup(bxgrid_ftor_query_cache, fileid);
		debug(D_BXGRID, "file %s is %s the ftor_query_cache", fileid, (replica_list ? "in" : "not in"));
	}

	if (!replica_list) {
		BXGRID_QUERY_AND_CHECK(rep_res, mysql_cxn, NULL, BXGRID_REPLICAID_QUERY, fileid);
		replica_list = (struct bxgrid_replica_list *)xxmalloc(sizeof(struct bxgrid_replica_list));

		replica_list->nreplicas = mysql_num_rows(rep_res);
		debug(D_BXGRID, "fileid %s has %d replicas", fileid, replica_list->nreplicas);

		replica_list->replicas = (char **)xxmalloc(sizeof(char *) * replica_list->nreplicas);
		for (int i = 0; i < replica_list->nreplicas; i++) {
			BXGRID_FETCH_AND_CHECK(rep_row, rep_res, NULL);
			replica_list->replicas[i] = xxstrdup(rep_row[0]);
			debug(D_BXGRID, "= %s", replica_list->replicas[i]);

			if (bxgrid_cache_rtol_query) {
				replica_location = (struct bxgrid_replica_location *)xxmalloc(sizeof(struct bxgrid_replica_location));
				replica_location->host = xxstrdup(rep_row[1]);
				replica_location->path = xxstrdup(rep_row[2]);
				hash_table_insert(bxgrid_rtol_query_cache, replica_list->replicas[i], replica_location);
			}
		}

		hash_table_insert(bxgrid_ftor_query_cache, fileid, replica_list);
		mysql_free_result(rep_res);
	}

	return replica_list;
}

const char *bxgrid_lookup_replicaid( MYSQL *mysql_cxn, const char *fileid, int nid, int update = 0 )
{
	static char replicaid[BXGRID_ID_MAX];

	MYSQL_RES  *rep_res;
	MYSQL_ROW   rep_row;
	struct bxgrid_replica_list *replica_list;
	struct bxgrid_replica_location *replica_location;

	if (bxgrid_cache_ftor_query) {
		replica_list = bxgrid_lookup_replica_list(mysql_cxn, fileid, update);

		if (!replica_list || replica_list->nreplicas == 0 || nid >= replica_list->nreplicas) return NULL;

		if (nid < 0) {
			int i;
			for (i = 0; i < replica_list->nreplicas; i++) {
				replica_location = (struct bxgrid_replica_location *)hash_table_lookup(bxgrid_rtol_query_cache, replica_list->replicas[i]);
				if (strcmp(bxgrid_hostname, replica_location->host) == 0)
					break;
			}

			if (i < replica_list->nreplicas) {
				strncpy(replicaid, replica_list->replicas[i], BXGRID_ID_MAX);
				debug(D_BXGRID, "selecting closest replica %s", replicaid);
			} else {
				// Careful, use UNSIGNED integer for array index!
				strncpy(replicaid, replica_list->replicas[random_uint() % replica_list->nreplicas], BXGRID_ID_MAX);
				debug(D_BXGRID, "selecting random replica %s", replicaid);
			}
		} else {
			strncpy(replicaid, replica_list->replicas[nid], BXGRID_ID_MAX);
			debug(D_BXGRID, "selecting replica %d %s", nid, replicaid);
		}
	} else {
		BXGRID_QUERY_AND_CHECK(rep_res, mysql_cxn, NULL, BXGRID_REPLICAID_QUERY, fileid);

		int nreplicas = mysql_num_rows(rep_res);
		debug(D_BXGRID, "fileid %s has %d replicas", fileid, nreplicas);

		if (nreplicas == 0 || nid >= nreplicas) return NULL;
		if (nid < 0) {
			int i, ri = random_uint() % nreplicas;
			BXGRID_FETCH_AND_CHECK(rep_row, rep_res, NULL);
			for (i = 0; i < nreplicas; i++) {
				if (ri == i)
					strncpy(replicaid, rep_row[0], BXGRID_ID_MAX);
				if (strcmp(bxgrid_hostname, rep_row[1]) == 0)
					break;
				if (i < (nreplicas - 1)) {
					BXGRID_FETCH_AND_CHECK(rep_row, rep_res, NULL);
				}
			}

			if (i < nreplicas) {
				strncpy(replicaid, rep_row[0], BXGRID_ID_MAX);
				debug(D_BXGRID, "selecting closest replica %s", replicaid);
			} else {
				debug(D_BXGRID, "selecting random replica %s", replicaid);
			}
		} else {
			BXGRID_FETCH_AND_CHECK(rep_row, rep_res, NULL);
			for (int i = 0; i < nid; i++) {
				BXGRID_FETCH_AND_CHECK(rep_row, rep_res, NULL);
			}
			strncpy(replicaid, rep_row[0], BXGRID_ID_MAX);
			debug(D_BXGRID, "selecting replica %d %s", nid, replicaid);
		}
		mysql_free_result(rep_res);
	}

	return replicaid;
}

#define BXGRID_REPLICA_PATH_QUERY "SELECT host, path FROM replicas WHERE replicaid = '%s'"

int bxgrid_lookup_replica_location( MYSQL *mysql_cxn, const char *replicaid, char *host, char *path)
{
	MYSQL_RES *rep_res;
	MYSQL_ROW  rep_row;
	struct bxgrid_replica_location *replica_location;

	if (replicaid == NULL) return -1;

	if (bxgrid_cache_rtol_query) {
		replica_location = (struct bxgrid_replica_location*)hash_table_lookup(bxgrid_rtol_query_cache, replicaid);
		debug(D_BXGRID, "replica %s is %s the rtol_query_cache", replicaid, (replica_location ? "in" : "not in"));

		if (!replica_location) {
			BXGRID_QUERY_AND_CHECK(rep_res, mysql_cxn, -1, BXGRID_REPLICA_PATH_QUERY, replicaid);
			BXGRID_FETCH_AND_CHECK(rep_row, rep_res, -1);

			replica_location = (struct bxgrid_replica_location *)xxmalloc(sizeof(struct bxgrid_replica_location));
			replica_location->host = xxstrdup(rep_row[0]);
			replica_location->path = xxstrdup(rep_row[1]);
			hash_table_insert(bxgrid_rtol_query_cache, replicaid, replica_location);
			mysql_free_result(rep_res);
		}

		strncpy(host, replica_location->host, PFS_PATH_MAX);
		strncpy(path, replica_location->path, PFS_PATH_MAX);
	} else {
		BXGRID_QUERY_AND_CHECK(rep_res, mysql_cxn, -1, BXGRID_REPLICA_PATH_QUERY, replicaid);
		BXGRID_FETCH_AND_CHECK(rep_row, rep_res, -1);

		strncpy(host, rep_row[0], PFS_PATH_MAX);
		strncpy(path, rep_row[1], PFS_PATH_MAX);

		mysql_free_result(rep_res);
	}

	debug(D_BXGRID, "replicaid %s is on %s at %s", replicaid, host, path);

	return 0;
}

/**
 *
 * Open a file using the bxgrid virtual folder abstraction
 *
 * If the item is a file, then we will attempt to open the closest replica
 * first, then a random one, then the replicas in order. When we run out of
 * replicas, then reload the replica locations and keep trying until we reach a
 * the global parrot operation timeout.
 *
 * If the item is a replica, then we will attempt to open the specified
 * replica.
 *
 */

pfs_file *bxgrid_bvf_open( MYSQL *mysql_cxn, struct bxgrid_virtual_folder *bvf, pfs_name *name, int flags, mode_t mode )
{
	char host[PFS_PATH_MAX];
	char path[PFS_PATH_MAX];
	const char *fileid, *replicaid;
	int nattempt = -1; // Start with closest/random
	int start_time;

	if (strcmp(bvf->name, "/fileid") == 0) {
		fileid    = path_basename(name->rest);
		replicaid = bxgrid_lookup_replicaid(mysql_cxn, fileid, nattempt);
	} else {
		fileid    = NULL;
		replicaid = path_basename(name->rest);
	}

	start_time = time(0);
	do {
		debug(D_BXGRID, "opening fileid %s using replicaid %s", fileid, replicaid);
		if (bxgrid_lookup_replica_location(mysql_cxn, replicaid, host, path) >= 0) {
			struct chirp_file *cfile;
			cfile = chirp_global_open(host, path, flags, mode, time(0)+bxgrid_timeout);
			if (cfile) {
				return new pfs_file_bxgrid(name, cfile);
			}
		}

		if (fileid) {
			replicaid = bxgrid_lookup_replicaid(mysql_cxn, fileid, ++nattempt);
			if (!replicaid) {
				nattempt  = -1;
				replicaid = bxgrid_lookup_replicaid(mysql_cxn, fileid, nattempt, 1);
			}
		} else {
			replicaid = NULL;
		}
	} while (replicaid && time(0) - start_time < pfs_main_timeout);

	return 0;
}

class pfs_service_bxgrid : public pfs_service {
public:
	/** Environmental variables:
	 * BXGRID_DBNAME: database name     (default: biometrics)
	 * BXGRID_USER:	  database user     (default: anonymous)
	 * BXGRID_PASS:	  database password (default: )
	 *
	 * BXGRID_CACHE_QUERIES:    cache all query results		(default: true)
	 * BXGRID_CACHE_STAT_QUERY: cache stat query results            (default: true)
	 * BXGRID_CACHE_FTOR_QUERY: cache file to replica query results (default: true)
	 * BXGRID_CACHE_RTOL_QUERY: cache replica to path query results (default: true)
	 *
	 * BXGRID_TIMEOUT: timeout in seconds (default: 2)
	 **/
	pfs_service_bxgrid()  {
		char *s;

		s = getenv("BXGRID_DBNAME");
		if (s) bxgrid_dbname = s;

		s = getenv("BXGRID_USER");
		if (s) bxgrid_user = s;

		s = getenv("BXGRID_PASS");
		if (s) bxgrid_pass = s;

		s = getenv("BXGRID_CACHE_QUERIES");
		if (s) bxgrid_cache_stat_query = bxgrid_cache_ftor_query = bxgrid_cache_rtol_query = atoi(s);

		s = getenv("BXGRID_CACHE_STAT_QUERY");
		if (s) bxgrid_cache_stat_query = atoi(s);

		s = getenv("BXGRID_CACHE_FTOR_QUERY");
		if (s) bxgrid_cache_ftor_query = atoi(s);

		s = getenv("BXGRID_CACHE_RTOL_QUERY");
		if (s) bxgrid_cache_rtol_query = atoi(s);

		if (bxgrid_cache_stat_query) bxgrid_stat_query_cache = hash_table_create(0, 0);
		if (bxgrid_cache_ftor_query) bxgrid_ftor_query_cache = hash_table_create(0, 0);
		if (bxgrid_cache_rtol_query) bxgrid_rtol_query_cache = hash_table_create(0, 0);

		s = getenv("BXGRID_TIMEOUT");
		if (s) bxgrid_timeout = atoi(s);

		if (gethostname(bxgrid_hostname, PFS_LINE_MAX) >= 0) {
			char ip_address[PFS_LINE_MAX];
			if (domain_name_lookup(bxgrid_hostname, ip_address))
				domain_name_lookup_reverse(ip_address, bxgrid_hostname);
		} else {
			strncpy(bxgrid_hostname, "localhost", PFS_LINE_MAX);
		}
	}

	virtual void *connect( pfs_name *name ) {
		MYSQL *mysql_cxn;

		debug(D_BXGRID, "hostname is %s", bxgrid_hostname);
		debug(D_BXGRID, "initializing MySQL");
		mysql_cxn = mysql_init(NULL);

		debug(D_BXGRID, "connect %s:%d", name->host, name->port);
		if (!mysql_real_connect(mysql_cxn, name->host, bxgrid_user, bxgrid_pass, bxgrid_dbname, name->port, 0, 0)) {
			debug(D_NOTICE|D_BXGRID, "failed to connect to %s: %s", name->host, mysql_error(mysql_cxn));
			return NULL;
		}

		return mysql_cxn;
	}

	virtual void disconnect( pfs_name *name, void *cxn ) {
		debug(D_BXGRID, "disconnect %s", name->host);
		mysql_close((MYSQL *)cxn);
	}

	virtual pfs_dir *getdir( pfs_name *name ) {
		pfs_dir *result = 0;
		MYSQL *mysql_cxn = (MYSQL *)pfs_service_connect_cache(name);
		struct bxgrid_virtual_folder *bvf;

		debug(D_BXGRID, "getdir %s", name->rest);
		if (mysql_cxn) {
			bvf = bxgrid_bvf_find(name->rest);
			if (bvf) {
				result = new pfs_dir(name);
				if (bvf->flags & BXGRID_LISTABLE) {
					if (bvf->flags & BXGRID_FILE_LIST) {	// BXGRID_FILE_LIST
						debug(D_BXGRID, "%s is file list: %s", name->rest, bvf->query);
						int nfiles;
						char **files;
						char buffer[PFS_LINE_MAX];

						strncpy((char *)buffer, bvf->query, PFS_LINE_MAX);
						if (string_split_quotes(buffer, &nfiles, &files)) {
							for (int i = 0; i < nfiles; i++)
								result->append(files[i]);
						}
					} else {				// BXGRID_FILE_QUERY
						debug(D_BXGRID, "%s is file query: %s", name->rest, bvf->query);
						MYSQL_RES *dir_res = bxgrid_db_query(mysql_cxn, bvf->query);
						MYSQL_ROW  dir_row;

						if (dir_res) {
							while ((dir_row = mysql_fetch_row(dir_res)) != NULL)
								result->append(dir_row[0]);
							mysql_free_result(dir_res);
						}
					}
				} else {
					errno = ENOTSUP;
				}
			} else {
				errno = ENOENT;
			}
			pfs_service_disconnect_cache(name, (void *)mysql_cxn, 0);
		}

		return result;
	}

	virtual int stat( pfs_name *name, struct pfs_stat *buf ) {
		int result = -1;
		MYSQL *mysql_cxn = (MYSQL *)pfs_service_connect_cache(name);

		debug(D_BXGRID, "stat %s", name->rest);
		if (mysql_cxn) {
			result = this->_stat(mysql_cxn, name, buf);
			pfs_service_disconnect_cache(name, (void *)mysql_cxn, 0);
		}
		BXGRID_END
	}

	virtual int lstat( pfs_name *name, struct pfs_stat *buf ) {
		int result = -1;
		MYSQL *mysql_cxn = (MYSQL *)pfs_service_connect_cache(name);

		debug(D_BXGRID, "lstat %s", name->rest);
		if (mysql_cxn) {
			result = this->_stat(mysql_cxn, name, buf);
			pfs_service_disconnect_cache(name, (void *)mysql_cxn, 0);
		}
		BXGRID_END
	}

	virtual int _stat( MYSQL *mysql_cxn, pfs_name *name, struct pfs_stat *buf ) {
		int result = -1;
		struct bxgrid_virtual_folder *bvf = bxgrid_bvf_find(name->rest);

		if (bvf) {
			BXGRID_MAKE_DIR_STAT(name, buf);
			result = 0;
		} else {
			bvf = bxgrid_bvf_find_base(name->rest);
			if (bvf) {
				if (bvf->flags & BXGRID_FILE_LIST) {	// BXGRID_FILE_LIST
					if (strstr(bvf->query, path_basename(name->rest))) {
						BXGRID_MAKE_DIR_STAT(name, buf);
						result = 0;
					} else {
						errno = ENOENT;
					}
				} else {				// BXGRID_FILE_QUERY
					pfs_service_emulate_stat(name, buf);

					if (bxgrid_bvf_stat(mysql_cxn, bvf, name, buf) >= 0) {
						result = 0;
					} else {
						errno = ENOENT;
					}
				}
			} else {
				errno = ENOENT;
			}
		}
		BXGRID_END
	}

	virtual pfs_file *open( pfs_name *name, int flags, mode_t mode ) {
		pfs_file *file = 0;
		MYSQL *mysql_cxn = (MYSQL *)pfs_service_connect_cache(name);

		debug(D_BXGRID, "open %s", name->rest);
		if (mysql_cxn) {
			struct bxgrid_virtual_folder *bvf = bxgrid_bvf_find(name->rest);
			if (bvf) {
				errno = EISDIR;
				return 0;
			} else {
				struct pfs_stat buf;
				int result;

				result = this->_stat(mysql_cxn, name, &buf);
				if (result < 0) return 0;
				if (!result && S_ISDIR(buf.st_mode)) { errno = EISDIR; return 0; }
				if ((flags&O_ACCMODE) != O_RDONLY) { errno = ENOTSUP; return 0; }

				bvf  = bxgrid_bvf_find_base(name->rest);
				file = bxgrid_bvf_open(mysql_cxn, bvf, name, flags, O_RDONLY);
			}
			pfs_service_disconnect_cache(name, (void *)mysql_cxn, 0);
		}

		return file;
	}

	virtual int chdir( pfs_name *name, char *newname ) {
		int result = -1;
		struct pfs_stat buf;

		debug(D_BXGRID, "chdir %s", name->rest);
		if (this->stat(name, &buf) >= 0) {
			if (S_ISDIR(buf.st_mode)) {
				sprintf(newname, "/%s/%s:%d%s", name->service_name, name->host, name->port, name->rest);
				result = 0;
			} else {
				errno = ENOTDIR;
				result = -1;
			}
		}
		BXGRID_END
	}

	virtual int access( pfs_name *name, mode_t mode ) {
		int result = -1;
		struct pfs_stat buf;
		MYSQL *mysql_cxn = (MYSQL *)pfs_service_connect_cache(name);

		debug(D_BXGRID, "access %s", name->rest);
		if (this->_stat(mysql_cxn, name, &buf) >= 0) {
			if (mode&X_OK || mode&W_OK) {
				errno = EACCES;
			} else {
				result = 0;
			}
		} else {
			errno = ENOENT;
		}

		pfs_service_disconnect_cache(name, (void *)mysql_cxn, 0);
		BXGRID_END
	}

	virtual pfs_location* locate( pfs_name *name ) {
		struct pfs_stat buf;
		pfs_location *loc = NULL;
		MYSQL *mysql_cxn = (MYSQL *)pfs_service_connect_cache(name);

		debug(D_BXGRID, "locate %s", name->rest);

		if (this->_stat(mysql_cxn, name, &buf) >= 0) {
			const char *fileid, *replicaid;
			char host[PFS_PATH_MAX], path[PFS_PATH_MAX];
			struct bxgrid_virtual_folder *bvf;

			if (S_ISDIR(buf.st_mode)) {
				errno = ENOTSUP;
			} else {
				bvf = bxgrid_bvf_find_base(name->rest);
				loc = new pfs_location();

				if (strcmp(bvf->name, "/fileid") == 0) {
					int nid = 0;

					fileid = path_basename(name->rest);
					while ((replicaid = bxgrid_lookup_replicaid(mysql_cxn, fileid, nid++))) {
						if (bxgrid_lookup_replica_location(mysql_cxn, replicaid, host, path) >= 0) {
							char *tmp = string_format("%s:%s", host, path);
							loc->append(tmp);
							free(tmp);
						}
					}
				} else {
					replicaid = path_basename(name->rest);
					if (bxgrid_lookup_replica_location(mysql_cxn, replicaid, host, path) >= 0) {
						char *tmp = string_format("%s:%s", host, path);
						loc->append(tmp);
						free(tmp);
					}
				}
			}
		}

		pfs_service_disconnect_cache(name, (void *)mysql_cxn, 0);
		return loc;
	}

	virtual int get_default_port() {
		return 0;
	}

	virtual int is_seekable() {
		return 1;
	}
};

static pfs_service_bxgrid pfs_service_bxgrid_instance;
pfs_service *pfs_service_bxgrid = &pfs_service_bxgrid_instance;

int pfs_file_bxgrid::fstat( struct pfs_stat *buf ) {
	int result;
	result = pfs_service_bxgrid->stat(&name, buf);
	return result;
}

#endif

/* vim: set noexpandtab tabstop=8: */
