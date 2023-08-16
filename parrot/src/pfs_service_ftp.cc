/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_table.h"
#include "pfs_service.h"

extern "C" {
#include "debug.h"
#include "ftp_lite.h"
#include "stringtools.h"
#include "username.h"
#include "domain_name_cache.h"
#include "copy_stream.h"
#include "full_io.h"
}

#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <errno.h>
#include <stdlib.h>

enum ftp_type_t { ANONYMOUS, USERPASS, GLOBUS_GSS };

class pfs_file_ftp : public pfs_file
{
private:
	FILE *stream;
	struct ftp_lite_server *server;

public:
	pfs_file_ftp( pfs_name *n, FILE *s, struct ftp_lite_server *sr ) : pfs_file(n) {
		stream = s;
		server = sr;
	}

	virtual int close() {
		fclose(stream);
		ftp_lite_done(server);
		pfs_service_disconnect_cache(&name,server,0);
		return 0;
	}

	virtual pfs_ssize_t read( void *d, pfs_size_t length, pfs_off_t offset ) {
		return ::full_fread(stream,d,length);
	}

	virtual pfs_ssize_t write( const void *d, pfs_size_t length, pfs_off_t offset ) {
		return ::full_fwrite(stream,d,length);
	}
};

class pfs_service_ftp : public pfs_service {
private:
	ftp_type_t type;

public:

	pfs_service_ftp( ftp_type_t t ) {
		type = t;
	}

	virtual void *connect( pfs_name *name ) {

		struct ftp_lite_server *server = NULL;
		int result = 0;
		int save_errno;

		debug(D_FTP,"connecting to %s:%d",name->host,name->port);

		if(this->type==GLOBUS_GSS) {
			server = ftp_lite_open(name->host,name->port);
			if(!server) return 0;
			result = ftp_lite_auth_globus(server);
		} else if(this->type==ANONYMOUS) {
			char username[USERNAME_MAX];
			char hostname[DOMAIN_NAME_MAX];
			char email[USERNAME_MAX+DOMAIN_NAME_MAX+1];
			if(!username_get(username)) strcpy(username,"anonymous");
			if(!domain_name_cache_guess(hostname)) strcpy(hostname,"nowhere");
			sprintf(email,"%s@%s",username,hostname);
			server = ftp_lite_open(name->host,name->port);
			if(!server) return 0;
			result = ftp_lite_auth_userpass(server,"anonymous",email);
		} else if(this->type==USERPASS) {
			char user[FTP_LITE_LINE_MAX];
			char pass[FTP_LITE_LINE_MAX];

			if(!strncmp(name->host,"anonymous@",10)) {
				server = ftp_lite_open(name->host+10,name->port);
				if(!server) return 0;
				result = ftp_lite_auth_userpass(server,"anonymous","anonymous");
			} else {
				server = ftp_lite_open(name->host,name->port);
				if(!server) return 0;
				if(!ftp_lite_login(name->host,user,FTP_LITE_LINE_MAX,pass,FTP_LITE_LINE_MAX)) {
					memset(pass,0,FTP_LITE_LINE_MAX);
					ftp_lite_close(server);
					errno = EACCES;
					return 0;
				}
				result = ftp_lite_auth_userpass(server,user,pass);
				memset(pass,0,FTP_LITE_LINE_MAX);
			}
		}

		if(!result) {
			save_errno = errno;
			debug(D_FTP,"couldn't authenticate to %s:%d: %s",name->host,name->port,strerror(errno));
			ftp_lite_close(server);
			errno = save_errno;
			return 0;
		} else {
			return (void*) server;
		}
	}

	virtual void disconnect( pfs_name *name, void *cxn ) {
		debug(D_FTP,"disconnecting from %s:%d",name->host,name->port);
		ftp_lite_close((struct ftp_lite_server*)cxn);
	}

	virtual int get_default_port() {
		if(type==GLOBUS_GSS) {
			return FTP_LITE_GSS_DEFAULT_PORT;
		} else {
			return FTP_LITE_DEFAULT_PORT;
		}
	}

	virtual pfs_file * open( pfs_name *name, int flags, mode_t mode ) {
		FILE *stream=0;
		pfs_file *result=0;
		struct ftp_lite_server *server = (struct ftp_lite_server*) pfs_service_connect_cache(name);
		if(server) {
			if((flags&O_ACCMODE)==O_RDONLY) {
				stream = ftp_lite_get(server,name->rest,0);
				if(stream) result = new pfs_file_ftp(name,stream,server);
			} else if((flags&O_ACCMODE)==O_WRONLY) {
				stream = ftp_lite_put(server,name->rest,0,FTP_LITE_WHOLE_FILE);
				if(stream) result = new pfs_file_ftp(name,stream,server);
			} else {
				errno = EINVAL;
				result = 0;
			}
			if(!result) {
				if(errno==EINVAL) {
					// ok
				} else if(errno==ECONNRESET) {
					// ok
				} else if(ftp_lite_change_dir(server,name->rest)) {
					errno = EISDIR;
				} else if(ftp_lite_size(server,name->rest)) {
					errno = EACCES;
				} else {
					errno = ENOENT;
				}
				int invalid = (result==0 && errno==ECONNRESET);
				pfs_service_disconnect_cache(name,(void*)server,invalid);
			}
		}
		return result;
	}

	virtual pfs_dir * getdir( pfs_name *name ) {
		FILE *data;
		char entry[PFS_PATH_MAX];
		pfs_dir *result = 0;

		struct ftp_lite_server *server = (struct ftp_lite_server *)pfs_service_connect_cache(name);
		if(server) {
			data = ftp_lite_list(server,name->rest);
			if(data) {
				result = new pfs_dir(name);
				while(fgets(entry,sizeof(entry),data)) {
					string_chomp(entry);
					result->append(entry);
				}
				fclose(data);
				ftp_lite_done(server);
			}
			int invalid = (errno==ECONNRESET);
			pfs_service_disconnect_cache(name,(void*)server,invalid);
		}

		return result;
	}

	/*
	Some FTP servers fail when SIZE is applied to a directory.
	Some do not.  Thus, always test for a directory first.
	*/

	virtual int stat( pfs_name *name, struct pfs_stat *buf ) {
		INT64_T result=-1;
		struct ftp_lite_server *server = (struct ftp_lite_server *)pfs_service_connect_cache(name);
		if(server) {
			pfs_service_emulate_stat(name,buf);

			if(ftp_lite_change_dir(server,name->rest)) {
				buf->st_mode &= ~(S_IFREG);
				buf->st_mode |= S_IFDIR;
				buf->st_size = 0;
				result = 0;
			} else {
				result = ftp_lite_size(server,name->rest);
				if(result>=0) {
					buf->st_size = result;
					result = 0;
				} else {
					result = -1;
				}
			}
			int invalid = (errno==ECONNRESET);
			pfs_service_disconnect_cache(name,(void*)server,invalid);
		}
		return result;
	}

	virtual int lstat( pfs_name *name, struct pfs_stat *buf ) {
		return this->stat(name,buf);
	}

	virtual int access( pfs_name *name, mode_t mode ) {
		struct pfs_stat buf;
		return this->stat(name,&buf);
	}

	virtual int chdir( pfs_name *name, char *newname ) {
		int result=-1;
		char tname[PFS_PATH_MAX];
		struct ftp_lite_server *server = (struct ftp_lite_server *)pfs_service_connect_cache(name);
		if(server) {
			if(ftp_lite_change_dir(server,name->rest)) {
				if(ftp_lite_current_dir(server,tname)) {
					sprintf(newname,"/%s/%s:%d%s",name->service_name,name->host,name->port,tname);
					result = 0;
				} else {
					strcpy(newname,name->path);
					result = 0;
				}
			} else {
				result = -1;
			}
			int invalid = (errno==ECONNRESET);
			pfs_service_disconnect_cache(name,(void*)server,invalid);
		}
		return result;
	}

	virtual int unlink( pfs_name *name ) {
		int result=-1;
		struct ftp_lite_server *server = (struct ftp_lite_server *)pfs_service_connect_cache(name);
		if(server) {
			if(ftp_lite_delete(server,name->rest)) {
				result = 0;
			} else {
				result = -1;
			}
			int invalid = (errno==ECONNRESET);
			pfs_service_disconnect_cache(name,(void*)server,invalid);
		}
		return result;
	}

	virtual int rename( pfs_name *name, pfs_name *newname ) {
		int result=-1;
		struct ftp_lite_server *server = (struct ftp_lite_server *)pfs_service_connect_cache(name);
		if(server) {
			if(ftp_lite_rename(server,name->rest,newname->rest)) {
				result = 0;
			} else {
				result = -1;
			}
			int invalid = (errno==ECONNRESET);
			pfs_service_disconnect_cache(name,(void*)server,invalid);
		}
		return result;
	}

	virtual int mkdir( pfs_name *name, mode_t mode ) {
		int result=-1;
		struct ftp_lite_server *server = (struct ftp_lite_server *)pfs_service_connect_cache(name);
		if(server) {
			if(ftp_lite_make_dir(server,name->rest)) {
				result = 0;
			} else {
				result = -1;
			}
			int invalid = (errno==ECONNRESET);
			pfs_service_disconnect_cache(name,(void*)server,invalid);
		}
		return result;
	}

	virtual int rmdir( pfs_name *name ) {
		int result=-1;
		struct ftp_lite_server *server = (struct ftp_lite_server *)pfs_service_connect_cache(name);
		if(server) {
			if(ftp_lite_delete_dir(server,name->rest)) {
				result = 0;
			} else {
				result = -1;
			}
			int invalid = (errno==ECONNRESET);
			pfs_service_disconnect_cache(name,(void*)server,invalid);
		}
		return result;
	}

	virtual int is_seekable (void) {
		return 0;
	}
};

static pfs_service_ftp pfs_service_ftp_instance(USERPASS);
static pfs_service_ftp pfs_service_anonftp_instance(ANONYMOUS);
static pfs_service_ftp pfs_service_gsiftp_instance(GLOBUS_GSS);

pfs_service *pfs_service_ftp = &pfs_service_ftp_instance;
pfs_service *pfs_service_anonftp = &pfs_service_anonftp_instance;
pfs_service *pfs_service_gsiftp = &pfs_service_gsiftp_instance;

/* vim: set noexpandtab tabstop=8: */
