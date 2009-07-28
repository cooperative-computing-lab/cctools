/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifdef HAS_BXGRID

#include "pfs_table.h"
#include "pfs_service.h"

extern "C" {
#include "chirp_global.h"
#include "debug.h"
#include "stringtools.h"
#include "xmalloc.h"
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
#include <mysql/mysql.h>
}

#define BXGRID_USER	"anonymous"
#define	BXGRID_PASS	""
#define	BXGRID_DBNAME	"biometrics"
#define BXGRID_REG_MODE	(S_IFREG | 0400)
#define BXGRID_DIR_MODE	(S_IFDIR | 0755)

#define	BXGRID_MAKE_DIR_STAT( name, buf ) \
	pfs_service_emulate_stat((name), (buf)); \
	(buf)->st_mode = BXGRID_DIR_MODE; \
	(buf)->st_size = 0; \

#define BXGRID_END	debug(D_BXGRID,"= %d %s",(int)result,((result>=0) ? "" : strerror(errno))); return result;

extern int pfs_master_timeout;

enum BXGRID_FLAGS {
	BXGRID_FILE_LIST	= 0x000001,
	BXGRID_FILE_QUERY	= 0x000002,
	BXGRID_LISTABLE		= 0x000004
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
	  "SELECT size, UNIX_TIMESTAMP(lastcheck) FROM files WHERE fileid = '%d'",
	  BXGRID_FILE_QUERY
	},
	{ "/replicaid",
	  "SELECT replicaid FROM replicas",
	  "SELECT size, UNIX_TIMESTAMP(replicas.lastcheck) FROM files LEFT JOIN replicas USING(fileid) WHERE replicaid = '%d'",
	  BXGRID_FILE_QUERY
	},
	{ NULL, NULL, NULL, 0 }
};

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
	string_dirname(path, (char *)dirname);
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
		return chirp_global_close(file,time(0)+pfs_master_timeout);
	}       

	virtual pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset ) {   
		return chirp_global_pread(file,data,length,offset,time(0)+pfs_master_timeout);  
	}

	virtual int fstat( struct pfs_stat *buf ) {
		int result;
		struct chirp_stat cbuf;
		result = chirp_global_fstat(file,&cbuf,time(0)+pfs_master_timeout);             
		if(result==0) COPY_CSTAT(cbuf,*buf);
		return result;
	}       

	virtual int fstatfs( struct pfs_statfs *buf ) {
		int result;
		struct chirp_statfs cbuf;
		result = chirp_global_fstatfs(file,&cbuf,time(0)+pfs_master_timeout);           
		if(result==0) COPY_STATFS(cbuf,*buf);
		return result;
	}

	virtual pfs_ssize_t get_size() {
		struct pfs_stat buf;
		if(this->fstat(&buf)==0) {
			return buf.st_size;
		} else {
			return -1;
		}       
	} 
};

#define BXGRID_QUERY_AND_CHECK( res, cxn, ... ) \
	(res) = bxgrid_db_query(cxn, __VA_ARGS__); \
	if ((res) == NULL) return -1;

#define BXGRID_FETCH_AND_CHECK( row, res ) \
	(row) = mysql_fetch_row(res); \
	if ((row) == NULL) { mysql_free_result(res); return -1; }

int bxgrid_bvf_stat( MYSQL *mysql_cxn, struct bxgrid_virtual_folder *bvf, int id, struct pfs_stat *buf )
{
	MYSQL_RES *file_res;
	MYSQL_ROW  file_row;
	
	BXGRID_QUERY_AND_CHECK(file_res, mysql_cxn, bvf->stat_query, id);
	BXGRID_FETCH_AND_CHECK(file_row, file_res);

	buf->st_mode  = BXGRID_REG_MODE;
	buf->st_size  = strtol(file_row[0], NULL, 10);
	buf->st_mtime = strtol(file_row[1], NULL, 10);

	mysql_free_result(file_res);
	return 0;
}

int bxgrid_lookup_replicaid( MYSQL *mysql_cxn, int fileid, int nid )
{
#define BXGRID_REPLICAID_QUERY "SELECT replicaid FROM replicas WHERE fileid = '%d' ORDER BY %s"
	MYSQL_RES *rep_res;
	MYSQL_ROW  rep_row;
	int nreplicas;
	int replicaid;

	if (nid < 0) {
		BXGRID_QUERY_AND_CHECK(rep_res, mysql_cxn, BXGRID_REPLICAID_QUERY, fileid, "RAND()");
	} else {
		BXGRID_QUERY_AND_CHECK(rep_res, mysql_cxn, BXGRID_REPLICAID_QUERY, fileid, "replicaid");
	}

	nreplicas = mysql_num_rows(rep_res);
	debug(D_BXGRID, "fileid %d has %d replicas", fileid, nreplicas);
	if (nreplicas == 0) return -1;

	BXGRID_FETCH_AND_CHECK(rep_row, rep_res);
	for (int i = 0; i < nid; i++) {
		BXGRID_FETCH_AND_CHECK(rep_row, rep_res);
	}
	replicaid = strtol(rep_row[0], NULL, 10);

	mysql_free_result(rep_res);
	return replicaid;
}

int bxgrid_lookup_replica_path( MYSQL *mysql_cxn, int replicaid, char *host, char *path)
{
#define BXGRID_REPLICA_PATH_QUERY "SELECT host, path FROM replicas WHERE replicaid = '%d'"
	MYSQL_RES *rep_res;
	MYSQL_ROW  rep_row;

	if (replicaid < 0) return -1;
	
	BXGRID_QUERY_AND_CHECK(rep_res, mysql_cxn, BXGRID_REPLICA_PATH_QUERY, replicaid);
	BXGRID_FETCH_AND_CHECK(rep_row, rep_res);

	strncpy(host, rep_row[0], PFS_PATH_MAX);
	strncpy(path, rep_row[1], PFS_PATH_MAX);
	
	mysql_free_result(rep_res);
	return 0;
}

pfs_file *bxgrid_bvf_open( MYSQL *mysql_cxn, struct bxgrid_virtual_folder *bvf, pfs_name *name, int flags, mode_t mode )
{
	char host[PFS_PATH_MAX];
	char path[PFS_PATH_MAX];
	int fileid, replicaid;
	int nattempt = -1;

	if (strcmp(bvf->name, "/fileid") == 0) {
		fileid    = strtol(string_basename(name->rest), NULL, 10);
		replicaid = bxgrid_lookup_replicaid(mysql_cxn, fileid, nattempt); // Select random replica
	} else {
		fileid    = -1;
		replicaid = strtol(string_basename(name->rest), NULL, 10);
	}

	do {
		debug(D_BXGRID, "opening fileid %d using replicaid %d", fileid, replicaid);
		if (bxgrid_lookup_replica_path(mysql_cxn, replicaid, host, path) >= 0) {
			struct chirp_file *cfile;
			cfile = chirp_global_open(host, path, flags, mode, time(0)+pfs_master_timeout);
			if (cfile) {
				return new pfs_file_bxgrid(name, cfile);
			}
		}

		if (fileid >= 0) {
			replicaid = bxgrid_lookup_replicaid(mysql_cxn, fileid, nattempt++);
		} else {
			replicaid = -1;
		}
	} while (replicaid >= 0);

	return 0;
}

class pfs_service_bxgrid : public pfs_service {
public:
	pfs_service_bxgrid() {
	}

	virtual void *connect( pfs_name *name ) {
		MYSQL *mysql_cxn;
		
		debug(D_BXGRID, "initializing MySQL");
		mysql_cxn = mysql_init(NULL);

		debug(D_BXGRID, "connect %s:%d", name->host, name->port);
		if (!mysql_real_connect(mysql_cxn, name->host, BXGRID_USER, BXGRID_PASS, BXGRID_DBNAME, name->port, 0, 0)) {
			debug(D_BXGRID, "failed to connect to %s: %s", name->host, mysql_error(mysql_cxn));
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
			struct bxgrid_virtual_folder *bvf = bxgrid_bvf_find(name->rest);
			if (bvf) {
				BXGRID_MAKE_DIR_STAT(name, buf);
				result = 0;
			} else {
				bvf = bxgrid_bvf_find_base(name->rest);
				if (bvf) {
					if (bvf->flags & BXGRID_FILE_LIST) {	// BXGRID_FILE_LIST
						if (strstr(bvf->query, string_basename(name->rest))) {
							BXGRID_MAKE_DIR_STAT(name, buf);
							result = 0;
						} else {
							errno = ENOENT;
						}
					} else {				// BXGRID_FILE_QUERY
						int id = strtol(string_basename(name->rest), NULL, 10);
						pfs_service_emulate_stat(name, buf); 

						if (bxgrid_bvf_stat(mysql_cxn, bvf, id, buf) >= 0) {
							result = 0;
						} else {
							errno = ENOENT;
						}
					}
				} else {
					errno = ENOENT;
				}
			}
			pfs_service_disconnect_cache(name, (void *)mysql_cxn, 0);
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
				
				result = this->stat(name, &buf);
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

	virtual int lstat( pfs_name *name, struct pfs_stat *buf ) {
		int result;

		debug(D_BXGRID, "lstat %s", name->rest);
		result = this->stat(name, buf);
		BXGRID_END
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

	virtual int get_default_port() {
		return 0;
	}

	virtual int is_seekable() {
		return 1;
	}
};

static pfs_service_bxgrid pfs_service_bxgrid_instance;
pfs_service *pfs_service_bxgrid = &pfs_service_bxgrid_instance;

#endif

// vim: sw=8 sts=8 ts=8 ft=cpp 
