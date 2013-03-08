/*
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
Theory of Operation:

*/

#include "pfs_service.h"

extern "C" {
#include "debug.h"
#include "stringtools.h"
#include "domain_name.h"
#include "link.h"
#include "file_cache.h"
#include "password_cache.h"
#include "full_io.h"
#include "http_query.h"
#include "hash_table.h"
#include "xxmalloc.h"
#include "macros.h"
#include "sha1.h"
#include "sleeptools.h"
#include "s3client.h"
#include "md5.h"
}

#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <ctype.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/statfs.h>

#define HTTP_PORT 80
#define MD5_STRING_LENGTH 33

extern int pfs_master_timeout;
extern int pfs_checksum_files;
extern char pfs_temp_dir[];
extern struct file_cache * pfs_file_cache;
extern struct password_cache * pfs_password_cache;

extern void pfs_abort();
extern int pfs_cache_invalidate( pfs_name *name );

void s3_dirent_to_stat( struct s3_dirent_object *d, struct pfs_stat *s )
{
	s->st_dev = 1;
	s->st_ino = hash_string(d->key);
	s->st_mode = S_IFREG | S_IRWXU | S_IRWXG | S_IRWXO;
	s->st_nlink = 1;
	s->st_uid = getuid();
	s->st_gid = getgid();
	s->st_rdev = 1;
	s->st_size = d->size;
	s->st_blksize = 4096;
	s->st_blocks = 1+d->size/512;
	s->st_atime = d->last_modified;
	s->st_mtime = d->last_modified;
	s->st_ctime = d->last_modified;
}

struct pfs_file_cache_data {
	char name[L_tmpnam];
	unsigned char digest[MD5_STRING_LENGTH];
	size_t size;
};

class pfs_file_s3 : public pfs_file
{
private:
	char bucket[PFS_PATH_MAX];
	char *username, *password;
	char *local_name;
	FILE *file;
	char modified;

public:
	pfs_file_s3( pfs_name *n, char *ln, FILE *f ) : pfs_file(n) {
		local_name = ln;
		sscanf(name.hostport, "%[^:]:", bucket);
		username = strdup(pfs_password_cache->username);
		password = strdup(pfs_password_cache->password);
		modified = 0;
		file = f;
	}

	~pfs_file_s3() {
		free(username);
		free(password);
	}

	virtual int close() {
		int result;
		if(!file) return -1;
		result = ::fclose(file);
		if(!result && modified) {
			file = NULL;
			return s3_put_file(local_name, name.rest, bucket, AMZ_PERM_PRIVATE, username, password);
		} else {
			return result;
		}
	}

	virtual pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset ) {
		pfs_ssize_t actual;
		fseek( file, offset, SEEK_SET );
		actual = ::fread( data, 1, length, file );
		return actual;
	}

	virtual pfs_ssize_t write( const void *data, pfs_size_t length, pfs_off_t offset ) {
		pfs_ssize_t actual;
		fseek( file, offset, SEEK_SET );
		actual = ::fwrite( data, 1, length, file );
		if(actual) modified = 1;
		return actual;
	}

	virtual int fstat( struct pfs_stat *buf ) {
		int result;
		struct s3_dirent_object d;
		result = s3_stat_file(name.rest, bucket, &d, username, password);
		if(!result) s3_dirent_to_stat(&d,buf);
		return result;
	}
};

class pfs_service_s3 : public pfs_service {
private:
	struct hash_table *s3_file_cache;
public:

	pfs_service_s3() {
		char *endpoint;
		s3_file_cache = hash_table_create(0,NULL);
		if((endpoint = getenv("PARROT_S3_ENDPOINT"))) {
			s3_set_endpoint(endpoint);
		}
	}
	~pfs_service_s3() {
		//Delete file_cache;
	}

	virtual int get_default_port() {
		return HTTP_PORT;
	}

	virtual pfs_file * open( pfs_name *name, int flags, mode_t mode ) {
		char bucket[PFS_PATH_MAX];
		char path[MAX_KEY_LENGTH];

		char *local_name;
		char local_exists;
		FILE *local_file = NULL;

		char *username, *password;
		if(!pfs_password_cache) {
			errno = EACCES;
			return NULL;
		}
		username = pfs_password_cache->username;
		password = pfs_password_cache->password;

		sscanf(name->hostport, "%[^:]:", bucket);
		sprintf(path, "%s:%s", bucket, name->rest);

		if( !(local_name = (char*)hash_table_lookup(s3_file_cache, path)) ) {
			int fd;
			local_name = new char[L_tmpnam];
			strcpy(local_name, "local_name-XXXXXX");
			fd = mkstemp(local_name); 
			close(fd);               // Ensures the local_name is reserved.

			hash_table_insert(s3_file_cache, path, local_name);
			local_exists = 1;
		}

		if(((char)(flags&O_ACCMODE) == (char)O_RDONLY) || ((char)(flags&O_ACCMODE) == (char)O_RDWR)) {
			struct s3_dirent_object dirent;

			if(local_exists) {
				unsigned char digest[MD5_DIGEST_LENGTH];
				s3_stat_file(name->rest, bucket, &dirent, username, password);
				md5_file(local_name, digest);
				if(strcmp(dirent.digest, md5_string(digest))) local_exists = 0;
			}
			if(!local_exists) {
				local_exists = !s3_get_file(local_name, &dirent, name->rest, bucket, username, password);
			}
		}

		switch(flags&O_ACCMODE) {
			case O_RDONLY:
				if(!local_exists) local_file = NULL;
				else local_file = ::fopen(local_name, "rb+");
				break;
			case O_WRONLY:
				local_file = ::fopen(local_name, "wb+");
				break;
			case O_RDWR:
				if(!local_exists) local_file = ::fopen(local_name, "wb+");
				else local_file = ::fopen(local_name, "rb+");
				break;
			default:
				break;
		}

		if(local_file) {
			return new pfs_file_s3(name, local_name, local_file);
		} else {
			errno = EACCES;
			return NULL;
		}
	}

	pfs_dir * getdir( pfs_name *name ) {
		struct list *dirents;
		struct s3_dirent_object *d;
		char bucket[PFS_PATH_MAX];
		
		if(!pfs_password_cache) {
			errno = EACCES;
			return NULL;
		}

		sscanf(name->hostport, "%[^:]:", bucket);

		pfs_dir *dir = new pfs_dir(name);
		dir->append(".");
		dir->append("..");

		dirents = list_create();
		if(s3_ls_bucket(bucket, dirents, pfs_password_cache->username, pfs_password_cache->password)) {
			list_delete(dirents);
			return dir;
		}

		while( (d = (struct s3_dirent_object*)list_pop_head(dirents)) ) {
			dir->append(d->key);
			free(d);
		}
		list_delete(dirents);

		return dir;
	}

	virtual int lstat( pfs_name *name, struct pfs_stat *info ) {
		struct s3_dirent_object d;
		char bucket[PFS_PATH_MAX];

		if(!pfs_password_cache) {
			errno = EACCES;
			return -1;
		}

		sscanf(name->hostport, "%[^:]:", bucket);

		if( s3_stat_file(name->rest, bucket, &d, pfs_password_cache->username, pfs_password_cache->password) < 0 ) {
			errno = ENOENT;
			return -1;
		}

		s3_dirent_to_stat(&d,info);
		if(!strcmp(name->rest, "/")) info->st_mode = S_IFDIR;

		return 0;
	}

	virtual int stat( pfs_name *name, struct pfs_stat *info ) {
		struct s3_dirent_object d;
		char bucket[PFS_PATH_MAX];

		if(!pfs_password_cache) {
			errno = EACCES;
			return -1;
		}

		sscanf(name->hostport, "%[^:]:", bucket);

		if( s3_stat_file(name->rest, bucket, &d, pfs_password_cache->username, pfs_password_cache->password) < 0 ) {
			errno = ENOENT;
			return -1;
		}

		s3_dirent_to_stat(&d,info);
		if(!strcmp(name->rest, "/")) info->st_mode = S_IFDIR;

		return 0;
	}

	virtual int unlink( pfs_name *name ) {
		char *local_name;
		char path[MAX_KEY_LENGTH];
		int result;
		char bucket[PFS_PATH_MAX];

		if(!pfs_password_cache) {
			errno = EACCES;
			return -1;
		}

		sscanf(name->hostport, "%[^:]:", bucket);

		sprintf(path, "%s:%s", bucket, name->rest);
		local_name = (char *)hash_table_lookup(s3_file_cache, path);

		if(local_name) {
			if(!::unlink(local_name)) {
				errno = EACCES;
				return -1;
			}
			delete local_name;
			hash_table_remove(s3_file_cache, path);

			result = s3_rm_file(name->rest, bucket, pfs_password_cache->username, pfs_password_cache->password);
			if(result) {
				errno = EACCES;
				return result;
			}
			return 0;
		}

		result = s3_rm_file(name->rest, bucket, pfs_password_cache->username, pfs_password_cache->password);
		if(result) {
			errno = EACCES;
			return result;
		}

		return 0;
	}

	virtual int access( pfs_name *name, mode_t mode ) {
		struct pfs_stat info;
		return this->stat(name,&info);
	}

	virtual int chmod( pfs_name *name, mode_t mode ) {
		errno = ENOSYS;
		return -1;
	}

	virtual int chown( pfs_name *name, uid_t uid, gid_t gid ) {
		errno = ENOSYS;
		return -1;
	}

	virtual int lchown( pfs_name *name, uid_t uid, gid_t gid ) {
		errno = ENOSYS;
		return -1;
	}

	virtual int truncate( pfs_name *name, pfs_off_t length ) {
		errno = ENOSYS;
		return -1;
	}

	virtual int utime( pfs_name *name, struct utimbuf *buf ) {
		errno = ENOSYS;
		return -1;
	}

	virtual int rename( pfs_name *oldname, pfs_name *newname ) {
		errno = ENOSYS;
		return -1;
	}

	virtual int chdir( pfs_name *name, char *newpath ) {
		struct pfs_stat info;
		if(this->stat(name,&info)==0) {
			if(S_ISDIR(info.st_mode)) {
				return 0;
			} else {
				errno = ENOTDIR;
				return -1;
			}
		} else {
			return -1;
		}
	}

	virtual int link( pfs_name *oldname, pfs_name *newname ) {
		errno = ENOSYS;
		return -1;
	}

	virtual int symlink( const char *linkname, pfs_name *newname ) {
		errno = ENOSYS;
		return -1;
	}

	virtual int readlink( pfs_name *name, char *buf, pfs_size_t bufsiz ) {
		errno = ENOSYS;
		return -1;
	}

	virtual int mkdir( pfs_name *name, mode_t mode ) {
		errno = ENOSYS;
		return -1;
	}

	virtual int rmdir( pfs_name *name ) {
		errno = ENOSYS;
		return -1;
	}
};

static pfs_service_s3 pfs_service_s3_instance;
pfs_service *pfs_service_s3 = &pfs_service_s3_instance;


