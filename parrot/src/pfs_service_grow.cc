/*

This module was written by Aureliano Rama and  Simone Pagan Griso,
and modified by Douglas Thain.  Igor Sfiligoi and Donatella Lucchesi
contributed significantly to the design and debugging of this system.

This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
Theory of Operation:

The Global Read-Only Web (GROW) filesystem is designed to make a
directory tree stored on a web server accessible over the wide area,
with aggressive caching and end-to-end integrity checks.

To create a GROW filesystem, run make_growfs on the root of the
filesystem, and export it via a web server.  This script creates
a file .growfsdir that contains a complete directory listing and
checksum of all data.  Upon first accessing the filesystem remotely,
GROW-FS loads the directory listing into a tree form in memory.
All metadata requests and directory lookups are handled using this
data structure.

To access a file, GROW issues an HTTP request and reads the data
sequentially into the pfs_file_cache.  A checksum is computed
incrementally.  If the checksum does not match that in the directory
listing, the directory cache is discarded, and the close() fails
with EAGAIN, causing the pfs_file_cache to re-issue the open.
This procedure is repeated with an exponentially repeating backoff
until the filesystem becomes consistent.

The integrity of the directory listing is ensured by fetching
its checksum using https.  If the master checksum
and the directory listing are inconsistent, they are reloaded
in the same way as files.

This scheme is designed to maximize the cacheability of all components
of the filesystem.  Both files and data can be cached on local disk
without reomte consistency checks, as well as cached on shared proxy
servers, allowing the filesystem to scale to a very large number of clients.

(Note that GROW is designed to be an improvment over the old HTTP-FS
filesystem, which placed a listing in every single directory.)

*/

#include "pfs_service.h"

extern "C" {
#include "debug.h"
#include "stringtools.h"
#include "domain_name.h"
#include "link.h"
#include "file_cache.h"
#include "full_io.h"
#include "http_query.h"
#include "hash_table.h"
#include "xxmalloc.h"
#include "macros.h"
#include "sha1.h"
#include "sleeptools.h"
}

#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/statfs.h>

#define GROW_LINE_MAX 4096
#define GROW_PORT 80
#define GROW_EPOCH 1199163600

extern int pfs_master_timeout;
extern int pfs_checksum_files;
extern char pfs_temp_dir[];
extern struct file_cache * pfs_file_cache;

extern void pfs_abort();
extern int pfs_cache_invalidate( pfs_name *name );

static struct grow_filesystem * grow_filesystem_list = 0;
static sha1_context_t grow_filesystem_checksum;

/*
A grow_filesystem structure represents an entire
filesystem rooted at a given host and path.
All known filesystem are kept in a linked list
rooted at grow_filesystem_list 
*/

struct grow_filesystem {
	char hostport[PFS_PATH_MAX];
	char path[PFS_PATH_MAX];
	struct grow_dirent *root;
	struct grow_filesystem *next;
};

/*
A grow_dirent is a node in a tree representing the
entire directory structure of a grow_filesystem.
Each node describes its name, metadata, checksum,
and children (if a directory)
*/

struct grow_dirent {
	char *name;
	char *linkname;
	unsigned mode;
	UINT64_T size;
	UINT64_T inode;
	time_t   mtime;
	char checksum[SHA1_DIGEST_ASCII_LENGTH];
	struct grow_dirent *children;
	struct grow_dirent *parent;
	struct grow_dirent *next;
};

void grow_dirent_delete( struct grow_dirent *d );

/*
Compare two path strings only up to the first slash.
For example, "foo" matches "foo/bar/baz".
Return one if they match, zero otherwise.
*/

static int compare_path_element( const char *a, const char *b )
{
	while(*a==*b) {
		if(!*a) return 1;
		if(*a=='/') return 1;
		a++;
		b++;
	}

	if(*a==0 && *b=='/') return 1;
	if(*b==0 && *a=='/') return 1;

	return 0;
}

/*
Compare two entire path strings to see if a is a prefix of b.
Return the remainder of b not matched by a.
For example, compare_path_prefix("foo/baz","foo/baz/bar") returns "/bar".
Return null if a is not a prefix of b.
*/

static const char * compare_path_prefix( const char *a, const char *b )
{
	while(1) {
		if(*a=='/' && *b=='/') {
			while(*a=='/') a++;
			while(*b=='/') b++;
		}

		if(!*a) return b;
		if(!*b) return 0;

		if(*a==*b) {
			a++;
			b++;
			continue;
		} else {
			return 0;
		}
	}
}

/*
Recursively create a grow directory structure by reading
descriptor lines from a stored file.
*/


struct grow_dirent * grow_dirent_create_from_file( FILE *file, struct grow_dirent *parent )
{
	struct grow_dirent *d;
	struct grow_dirent *list=0;
	char line[GROW_LINE_MAX];
	char name[GROW_LINE_MAX];
	char linkname[GROW_LINE_MAX];
	char type;
	static INT64_T inode=2;
	
	while(fgets(line,sizeof(line),file)) {
		sha1_update(&grow_filesystem_checksum,(unsigned char*)line,strlen(line));
		sha1_update(&grow_filesystem_checksum,(unsigned char*)"\n",1);

		if(line[0]=='E') break;

		d = (struct grow_dirent *) xxmalloc(sizeof(*d));

		linkname[0] = 0;

		/* old large file format */
		int fields = sscanf(line,"%c %[^\t]\t%o %*d %" PRIu64 " %*d %*d %ld %*d %s %[^\n]",
                        &type,
                        name,
                        &d->mode,
                        &d->size,
                        &d->mtime,
                        d->checksum,
                        linkname);

		if(fields<6) {
		  /* new more compact file format */
		fields = sscanf(line,"%c %[^\t]\t%u %" PRIu64 " %ld %s %[^\n]",
			&type,
			name,
			&d->mode,
			&d->size,
			&d->mtime,
			d->checksum,
			linkname);

			d->mtime += GROW_EPOCH;
		}

		d->inode = inode++;

		if(fields>=6) {
			d->name = xxstrdup(name);
			if(linkname[0]) {
				d->linkname = xxstrdup(linkname);
			} else {
				d->linkname = 0;
			}
			if(type=='D') {
				d->children = grow_dirent_create_from_file(file,d);
			} else {
				d->children = 0;
			}
			d->parent = parent;
			d->next = list;
			list = d;
		} else {
			debug(D_GROW,"directory listing is corrupted!");
			free(d);
			grow_dirent_delete(list);
			return 0;
		}
	}

	return list;
}

/*
Recursively destroy a directory structure.
*/

void grow_dirent_delete( struct grow_dirent *d )
{
	struct grow_dirent *n;

	while(d) {
		if(d->name) free(d->name);
		if(d->linkname) free(d->linkname);
		grow_dirent_delete(d->children);
		n = d->next;
		free(d);
		d = n;
	}
}

void grow_dirent_to_stat( struct grow_dirent *d, struct pfs_stat *s )
{
	s->st_dev = 1;
	s->st_ino = d->inode;
	s->st_mode = d->mode;
	s->st_nlink = 1;
	s->st_uid = 0;
	s->st_gid = 0;
	s->st_rdev = 1;
	s->st_size = d->size;
	s->st_blksize = 65536;
	s->st_blocks = 1+d->size/512;
	s->st_atime = d->mtime;
	s->st_mtime = d->mtime;
	s->st_ctime = d->mtime;
}

/*
Recursively search for the grow_dirent named by path
in the filesystem given by root.  If link_count is zero,
then do not traverse symbolic links.  Otherwise, when
link_count reaches 100, ELOOP is returned.
*/


struct grow_dirent * grow_dirent_lookup_recursive( const char *path, struct grow_dirent *root, int link_count )
{
	struct grow_dirent *d;

	if(!path) path = "\0";
	while(*path=='/') path++;

	if( S_ISLNK(root->mode) && ( link_count>0 || path[0] ) ) {
		if(link_count>100) {
			errno = ELOOP;
			return 0;
		}

		char *linkname = root->linkname;

		if(linkname[0]=='/') {
			while(root->parent) {
				root = root->parent;
			}
		} else {
			root = root->parent;
		}

		root = grow_dirent_lookup_recursive(linkname,root,link_count+1);
		if(!root) {
			errno = ENOENT;
			return 0;
		}
	}

	if(!*path) return root;

	if(!S_ISDIR(root->mode)) {
		errno = ENOTDIR;
		return 0;
	}

	const char *subpath = strchr(path,'/');
	if(!subpath) subpath = "\0";

	if(compare_path_element(".",path)) {
		return grow_dirent_lookup_recursive(subpath,root,link_count);
	}

	if(compare_path_element("..",path)) {
		if(root->parent) {
			return grow_dirent_lookup_recursive(subpath,root->parent,link_count);
		} else {
			errno = ENOENT;
			return 0;
		}
	}

	for(d=root->children;d;d=d->next) {
		if(compare_path_element(d->name,path)) {
			return grow_dirent_lookup_recursive(subpath,d,link_count);
		}
	}

	errno = ENOENT;
	return 0;
}

/*
Search for a grow filesystem rooted at the given host and path.
If the required files (.growfsdir and .growfschecksum) exist, then
create a grow filesystem struct and return it.  If the two
are not consistent, delay and loop until they are.
Otherwise, return zero.
*/

struct grow_filesystem * grow_filesystem_create( const char *hostport, const char *path )
{
	unsigned char digest[SHA1_DIGEST_LENGTH];
	unsigned char checksum[SHA1_DIGEST_ASCII_LENGTH];
	char line[GROW_LINE_MAX];
	char url[GROW_LINE_MAX];
	char filename[GROW_LINE_MAX];
	char txn[GROW_LINE_MAX];
	struct grow_filesystem *f;
	struct grow_dirent *d;
	FILE * file;
	struct link *link;
	int sleep_time = 1;
	time_t stoptime = time(0)+pfs_master_timeout;

	retry:

	sprintf(url,"http://%s%s/.growfschecksum",hostport,path);

	debug(D_GROW,"searching for filesystem at %s",url);

	debug(D_GROW,"fetching checksum: %s",url);

	link = http_query_no_cache(url,"GET",stoptime);
	if(link) {
		if(link_readline(link,line,sizeof(line),stoptime)) {
			if(sscanf(line,"%s",checksum)) {
				/* ok to continue */
			} else {
				debug(D_GROW,"checksum is malformed!");
				goto sleep_retry;
			}
		} else {
			debug(D_GROW,"lost connection while fetching checksum!");
			goto sleep_retry;
		}
	} else {
		return 0;
	}

	debug(D_GROW,"remote checksum is %s",checksum);

	sprintf(url,"http://%s%s/.growfsdir",hostport,path);

	if(file_cache_contains(pfs_file_cache,url,filename)!=0) {

		debug(D_GROW,"fetching directory: %s",url);

		int fd = file_cache_begin(pfs_file_cache,url,txn);
		if(fd>=0) {
			INT64_T size;
			struct link *link = http_query_size(url,"GET",&size,stoptime,1);
			if(link) {
				if(link_stream_to_fd(link,fd,size,stoptime)>=0) {
					file_cache_commit(pfs_file_cache,url,txn);
				} else {
					file_cache_abort(pfs_file_cache,url,txn);
				}
				link_close(link);
			} else {
				file_cache_abort(pfs_file_cache,url,txn);
			}
			close(fd);
		}
	} else {
		debug(D_GROW,"directory is already cached");
	}

	if(file_cache_contains(pfs_file_cache,url,filename)!=0) {
		goto sleep_retry;
	}

	debug(D_GROW,"checksumming %s",filename);

	if(!sha1_file(filename,digest)) {
		debug(D_GROW,"couldn't checksum %s: %s",filename,strerror(errno));
		goto sleep_retry;
	}
	
	debug(D_GROW,"local checksum: %s",sha1_string(digest));

	if(strcmp((char*)checksum,sha1_string(digest))) {
		debug(D_GROW,"checksum does not match, reloading...");
		file_cache_delete(pfs_file_cache,url);
		goto sleep_retry;
	}

	file = fopen(filename,"r");
	if(!file) {
		debug(D_GROW,"couldn't open %s: %s",filename,strerror(errno));
		goto sleep_retry;
	}

	d = grow_dirent_create_from_file(file,0);
	if(!d) {
		debug(D_GROW,"%s is corrupted",filename);
		fclose(file);
		file_cache_delete(pfs_file_cache,url);
		goto sleep_retry;
	}

	fclose(file);
	
	f = (struct grow_filesystem *) malloc(sizeof(*f));
	strcpy(f->hostport,hostport);
	strcpy(f->path,path);
	f->root = d;

	return f;

	sleep_retry:

	if(sleep_time<pfs_master_timeout) {
		if(sleep_time>1) {
			debug(D_GROW,"directory and checksum are inconsistent, retry in %d seconds",sleep_time);
			sleep_for(sleep_time);
		}
		sleep_time*=2;
		goto retry;
	} else {
		fatal("directory and checksum still inconsistent after %d seconds",pfs_master_timeout);
		return 0;
	}
}

/*
Recursively destroy a grow filesystem.
*/

void grow_filesystem_delete( struct grow_filesystem *f )
{
	if(!f) return;
	grow_dirent_delete(f->root);
	grow_filesystem_delete(f->next);
	free(f);	
}

/*
Destroy all internal state for all filesystems.
This is called whenever a file checksum is found
to be inconsistent, and the state must be reloaded.
*/

void grow_filesystem_flush_all()
{
	grow_filesystem_delete(grow_filesystem_list);
	grow_filesystem_list = 0;
}

/*
Given a full PFS path name, search for an already-loaded
filesystem record.  If it exists, then search it for the
appropriate dirent.  If no filesystem record is found,
then search for and load the needed filesystem.
*/

struct grow_dirent * grow_dirent_lookup( pfs_name *name, int follow_links )
{
	struct grow_filesystem *f;
	char path[PFS_PATH_MAX];
	const char *subpath;
	char *s;

	for(f=grow_filesystem_list;f;f=f->next) {
		if(!strcmp(f->hostport,name->hostport)) {
			subpath = compare_path_prefix(f->path,name->rest);
			if(!subpath) {
				subpath = compare_path_prefix(name->rest,f->path);
				if(subpath) {
					errno = ENOENT;
					return 0;
				} else {
					continue;
				}
			}
			return grow_dirent_lookup_recursive(subpath,f->root,follow_links);
		}
	}

	strcpy(path,name->rest);
	while(1) {
		f = grow_filesystem_create(name->hostport,path);
		if(f) {
			f->next = grow_filesystem_list;
			grow_filesystem_list = f;
			subpath = compare_path_prefix(f->path,name->rest);
			return grow_dirent_lookup_recursive(subpath,f->root,follow_links);
		}
		s = strrchr(path,'/');
		if(s) {
			*s = 0;
		} else {
			break;
		}
	}

	errno = ENOENT;
	return 0;
}

class pfs_file_grow : public pfs_file
{
private:
	struct link *link;
	pfs_stat info;
	sha1_context_t context;

public:
	pfs_file_grow( pfs_name *n, struct link *l, struct grow_dirent *d ) : pfs_file(n) {
		link = l;
		grow_dirent_to_stat(d,&info);
		if(pfs_checksum_files) {
			sha1_init(&context);
		}
	}

	virtual int close() {
		link_close(link);

		struct grow_dirent *d;
		d = grow_dirent_lookup(&name,1);

		if(!d) {
			debug(D_GROW,"%s is no longer valid, will reload...",name.rest);
			grow_filesystem_flush_all();
			errno = EAGAIN;
			return -1;
		} else if(!strcmp(d->checksum,"0")) {
			return 0;
		} else if(pfs_checksum_files) {
			unsigned char digest[SHA1_DIGEST_LENGTH];
			sha1_final(digest,&context);
			if(!strcmp(sha1_string(digest),d->checksum)) {
				return 0;
			} else {
				debug(D_GROW,"checksum failed on %s, will reload...",name.path);
				grow_filesystem_flush_all();
				errno = EAGAIN;
				return -1;
			}
		} else {
			return 0;
		}
	}

	virtual pfs_ssize_t read( void *d, pfs_size_t length, pfs_off_t offset ) {
		pfs_ssize_t actual;
		actual = link_read(link,(char*)d,length,LINK_FOREVER);
		if(pfs_checksum_files && actual>0) sha1_update(&context,(unsigned char *)d,actual);
		return actual;
	}

	virtual int fstat( struct pfs_stat *i ) {
		*i = info;
		return 0;
	}

	/*
	This is a compatibility hack.
	This filesystem is read only, so locks make no sense.
	This simply satisfies some programs that insist upon it.
	*/	
	virtual int flock( int op ) {
		return 0;
	}

	virtual pfs_ssize_t get_size() {
		return info.st_size;
	}

};

class pfs_service_grow : public pfs_service {
public:
	virtual int get_default_port() {
		return GROW_PORT;
	}

	virtual pfs_file * open( pfs_name *name, int flags, mode_t mode ) {
		struct grow_dirent *d;
		char url[PFS_PATH_MAX];

		d = grow_dirent_lookup(name,1);
		if(!d) return 0;

		if(S_ISDIR(d->mode)) {
			errno = EISDIR;
			return 0;
		}

		sprintf(url,"http://%s%s",name->hostport,name->rest);

		struct link *link = http_query_no_cache(url,"GET",time(0)+pfs_master_timeout);
		if(link) {
			debug(D_GROW,"open %s",url);
			return new pfs_file_grow(name,link,d);
		} else {
			debug(D_GROW,"failed to open %s",url);
			return 0;
		}
	}

	pfs_dir * getdir( pfs_name *name ) {
		/*
		If the root of the GROW filesystem is requested,
		generate it interally using the list of known filesystems.
		*/

		if(!name->host[0]) {
			pfs_dir *dir = new pfs_dir(name);
			dir->append(".");
			dir->append("..");
			struct grow_filesystem *f = grow_filesystem_list;
			while(f) {
				dir->append(f->hostport);
				f = f->next;
			}
			return dir;
		}


		struct grow_dirent *d;

		d = grow_dirent_lookup(name,1);
		if(!d) return 0;

		if(!S_ISDIR(d->mode)) {
			errno = ENOTDIR;
			return 0;
		}

		pfs_dir *dir = new pfs_dir(name);

		dir->append(".");
		if(d->parent) dir->append("..");

		for(d=d->children;d;d=d->next) {
			dir->append(d->name);
		}

		return dir;
	}

	virtual int lstat( pfs_name *name, struct pfs_stat *info ) {
		/* If we get stat("/grow") then construct a dummy entry. */

		if(!name->host[0]) {
                        pfs_service_emulate_stat(name,info);
                        info->st_mode = S_IFDIR | 0555;
			return 0;
		}

		struct grow_dirent *d;

		d = grow_dirent_lookup(name,0);
		if(!d) return -1;

		grow_dirent_to_stat(d,info);

		return 0;
	}

	virtual int stat( pfs_name *name, struct pfs_stat *info ) {
		/* If we get stat("/grow") then construct a dummy entry. */

		if(!name->host[0]) {
                        pfs_service_emulate_stat(name,info);
                        info->st_mode = S_IFDIR | 0555;
			return 0;
		}

		struct grow_dirent *d;

		d = grow_dirent_lookup(name,1);
		if(!d) return -1;

		grow_dirent_to_stat(d,info);

		return 0;
	}

	virtual int unlink( pfs_name *name ) {
		errno = EROFS;
		return -1;
	}

	virtual int access( pfs_name *name, mode_t mode ) {
		struct pfs_stat info;
		if(this->stat(name,&info)==0) {
			if(mode&W_OK) {
				errno = EROFS;
				return -1;
			} else {
				return 0;
			}
		} else {
			return -1;
		}
	}

	virtual int chmod( pfs_name *name, mode_t mode ) {
		errno = EROFS;
		return -1;
	}

	virtual int chown( pfs_name *name, uid_t uid, gid_t gid ) {
		errno = EROFS;
		return -1;
	}

	virtual int lchown( pfs_name *name, uid_t uid, gid_t gid ) {
		errno = EROFS;
		return -1;
	}

	virtual int truncate( pfs_name *name, pfs_off_t length ) {
		errno = EROFS;
		return -1;
	}

	virtual int utime( pfs_name *name, struct utimbuf *buf ) {
		errno = EROFS;
		return -1;
	}

	virtual int rename( pfs_name *oldname, pfs_name *newname ) {
		errno = EROFS;
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
		errno = EROFS;
		return -1;
	}

	virtual int symlink( const char *linkname, pfs_name *newname ) {
		errno = EROFS;
		return -1;
	}

	virtual int readlink( pfs_name *name, char *buf, pfs_size_t bufsiz ) {
		struct grow_dirent *d;

		d = grow_dirent_lookup(name,0);
		if(!d) return -1;

		if(S_ISLNK(d->mode)) {
			int length;
			strncpy(buf,d->linkname,bufsiz);
			length = MIN((unsigned)bufsiz,strlen(d->linkname));
			buf[length] = 0;
			return length;
		} else {
			errno = EINVAL;
			return -1;
		}
	}

	virtual int mkdir( pfs_name *name, mode_t mode ) {
		errno = EROFS;
		return -1;
	}

	virtual int rmdir( pfs_name *name ) {
		errno = EROFS;
		return -1;
	}
};

static pfs_service_grow pfs_service_grow_instance;
pfs_service *pfs_service_grow = &pfs_service_grow_instance;


