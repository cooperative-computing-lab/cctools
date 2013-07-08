/*
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef HAS_CVMFS

#include "pfs_service.h"
#include "libcvmfs.h"

extern "C" {
#include "debug.h"
#include "stringtools.h"
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
#include <time.h>
#include <assert.h>
#include <string>
#include <list>

extern int pfs_master_timeout;
extern char pfs_temp_dir[];
extern const char * pfs_cvmfs_repo_arg;
extern bool pfs_cvmfs_repo_switching;

static bool cvmfs_configured = false;
static struct cvmfs_filesystem *cvmfs_filesystem_list = 0;
static struct cvmfs_filesystem *cvmfs_active_filesystem = 0;

#define CERN_KEY_PLACEHOLDER "<BUILTIN-cern.ch.pub>"

static const char *default_cvmfs_repo = "*.cern.ch:pubkey=" CERN_KEY_PLACEHOLDER ",url=http://cvmfs-stratum-one.cern.ch/opt/*;http://cernvmfs.gridpp.rl.ac.uk/opt/*;http://cvmfs.racf.bnl.gov/opt/*";

static const char *cern_key_text = 
"-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAukBusmYyFW8KJxVMmeCj\n\
N7vcU1mERMpDhPTa5PgFROSViiwbUsbtpP9CvfxB/KU1gggdbtWOTZVTQqA3b+p8\n\
g5Vve3/rdnN5ZEquxeEfIG6iEZta9Zei5mZMeuK+DPdyjtvN1wP0982ppbZzKRBu\n\
BbzR4YdrwwWXXNZH65zZuUISDJB4my4XRoVclrN5aGVz4PjmIZFlOJ+ytKsMlegW\n\
SNDwZO9z/YtBFil/Ca8FJhRPFMKdvxK+ezgq+OQWAerVNX7fArMC+4Ya5pF3ASr6\n\
3mlvIsBpejCUBygV4N2pxIcPJu/ZDaikmVvdPTNOTZlIFMf4zIP/YHegQSJmOyVp\n\
HQIDAQAB\n\
-----END PUBLIC KEY-----\n\
";

static bool wrote_cern_key;
static std::string cern_key_fname;

/*
A cvmfs_filesystem structure represents an entire
filesystem rooted at a given host and path.
All known filesystem are kept in a linked list
rooted at cvmfs_filesystem_list 
*/

class cvmfs_filesystem {
public:
	std::string host;
	std::string path;
	struct cvmfs_filesystem *next;
	std::string cvmfs_options;
	  // index into cvmfs_options+subst_offset to insert chars matching * in host
	std::list<int> wildcard_subst;
	int subst_offset;
	bool match_wildcard;

	cvmfs_filesystem *createMatch(char const *repo_name) const;
};

/*
A cvmfs_dirent contains information about a node
in the file tree.
*/

class cvmfs_dirent {
      public:
	cvmfs_dirent();
	~cvmfs_dirent();

	bool lookup(pfs_name * name, bool follow_leaf_symlinks, bool expand_internal_symlinks);

	char *name;
	unsigned mode;
	UINT64_T size;
	UINT64_T inode;
	time_t mtime;
};

cvmfs_dirent::cvmfs_dirent():
name(0), mode(0), size(0), inode(0), mtime(0)
{
}

cvmfs_dirent::~cvmfs_dirent()
{
	if(name) {
		free(name);
	}
}

/*
Compare two entire path strings to see if a is a prefix of b.
Return the remainder of b not matched by a.
For example, compare_path_prefix("foo/baz","foo/baz/bar") returns "/bar".
Return null if a is not a prefix of b.
*/

static const char *compare_path_prefix(const char *a, const char *b)
{
	while(1) {
		if(*a == '/' && *b == '/') {
			while(*a == '/')
				a++;
			while(*b == '/')
				b++;
		}

		if(!*a)
			return b;
		if(!*b)
			return 0;

		if(*a == *b) {
			a++;
			b++;
			continue;
		} else {
			return 0;
		}
	}
}

static void cvmfs_dirent_to_stat(struct cvmfs_dirent *d, struct pfs_stat *s)
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
	s->st_blocks = 1 + d->size / 512;
	s->st_atime = d->mtime;
	s->st_mtime = d->mtime;
	s->st_ctime = d->mtime;
}

static void cvmfs_parrot_logger(const char *msg)
{
	debug(D_CVMFS, "%s", msg);
}

static bool write_cern_key()
{
	if( wrote_cern_key ) {
		return true;
	}

	cern_key_fname = pfs_temp_dir;
	cern_key_fname += "/cvmfs"; // same directory as parent of cvmfs cache

	// if mkdir fails, just fall through and catch the error in fopen()
	// (it is expected to fail if the dir already exists)
	mkdir(cern_key_fname.c_str(),0755);

	cern_key_fname += "/cern.ch.pub";
	int key_fd = open(cern_key_fname.c_str(),O_WRONLY|O_CREAT|O_TRUNC|O_NOFOLLOW,0644);
	if( key_fd == -1 ) {
		debug(D_CVMFS|D_NOTICE,"ERROR: failed to open %s: errno=%d %s",
			  cern_key_fname.c_str(), errno, strerror(errno));
		return false;
	}

	int n_to_write = strlen(cern_key_text);
	int n_written = write(key_fd,cern_key_text,n_to_write);
	if( n_written != n_to_write ) {
		debug(D_CVMFS|D_NOTICE,"ERROR: failed to write to %s: errno=%d %s",
			  cern_key_fname.c_str(), errno, strerror(errno));
		close(key_fd);
		return false;
	}
	close(key_fd);

	wrote_cern_key = true;
	return true;
}

static bool cvmfs_activate_filesystem(struct cvmfs_filesystem *f)
{
	static int did_warning = 0;

	if(cvmfs_active_filesystem != f) {

		if(cvmfs_active_filesystem != NULL) {

			if(!did_warning) {

				did_warning = 1;

				if(!pfs_cvmfs_repo_switching) {
					debug(D_CVMFS|D_NOTICE,
						  "ERROR: using multiple CVMFS repositories in a single parrot session "
						  "is not allowed.  Define PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES "
						  "to enable experimental support, which could result in parrot crashing "
						  "or performing poorly.");
					return false;
				} else {
					debug(D_CVMFS|D_NOTICE,
						  "ERROR: using multiple CVMFS repositories in a single parrot session "
						  "is not fully supported.  PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES "
						  "has been defined, so switching now from %s to %s.  "
						  "Parrot may crash or perform poorly!",
						  cvmfs_active_filesystem->host.c_str(),
						  f->host.c_str());
				}
			}

			debug(D_CVMFS, "cvmfs_fini()");
			cvmfs_fini();
			cvmfs_active_filesystem = NULL;
		}

		debug(D_CVMFS,"activating repository %s",f->host.c_str());

		// check for references to the built-in cern.ch.pub key
		char const *cern_key_pos = strstr(f->cvmfs_options.c_str(),CERN_KEY_PLACEHOLDER);
		if( cern_key_pos ) {
			if( !write_cern_key() ) {
				debug(D_CVMFS|D_NOTICE,
					  "ERROR: cannot load cvmfs repository %s, because failed to write cern.ch.pub",
					  f->host.c_str());
				return false;
			}

			f->cvmfs_options.replace(cern_key_pos-f->cvmfs_options.c_str(),strlen(CERN_KEY_PLACEHOLDER),cern_key_fname);
		}

		cvmfs_set_log_fn(cvmfs_parrot_logger);

		debug(D_CVMFS, "cvmfs_init(%s)", f->cvmfs_options.c_str());
		int rc = cvmfs_init(f->cvmfs_options.c_str());
		
		if(rc != 0) {
			return false;
		}
		cvmfs_active_filesystem = f;
	}
	return true;
}

static cvmfs_filesystem *cvmfs_filesystem_create(const char *repo_name, bool wildcard, const char *path, const char *user_options, std::list<int> const &subst)
{
	cvmfs_filesystem *f = new cvmfs_filesystem;
	f->subst_offset = 0;
	f->next = NULL;
	f->host = repo_name;
	f->path = path;

	char *proxy = getenv("HTTP_PROXY");
	if( !proxy || !proxy[0] || !strcmp(proxy,"DIRECT") ) {
		if( !strstr(user_options,"proxies=") ) {
			debug(D_CVMFS|D_NOTICE,"CVMFS requires an http proxy.  None has been configured!");
			debug(D_CVMFS,"Ignoring configuration of CVMFS repository %s:%s",repo_name,user_options);
			delete f;
			return NULL;
		}
	}

	if( !user_options ) {
		user_options = "";
	}

	int repo_name_offset = 0;
	int repo_name_in_cachedir_offset = 0;
	char *buf = (char *)malloc(strlen(user_options)+2*strlen(repo_name)+strlen(pfs_temp_dir)+strlen(proxy?proxy:"")+100);
	sprintf(buf,
			"repo_name=%n%s,cachedir=%s/cvmfs/%n%s,timeout=%d,timeout_direct=%d%s%s,%n%s",
			&repo_name_offset,
			repo_name,
			pfs_temp_dir,
			&repo_name_in_cachedir_offset,
			repo_name,
			pfs_master_timeout,
			pfs_master_timeout,
			proxy ? ",proxies=" : "",
			proxy ? proxy : "",
			&f->subst_offset,
			user_options);
	f->cvmfs_options = buf;

	f->match_wildcard = wildcard;
	f->wildcard_subst = subst;
	if( wildcard ) {
		// make a note to fix up the repo name later
		f->wildcard_subst.push_back(repo_name_in_cachedir_offset - f->subst_offset);
		f->wildcard_subst.push_back(repo_name_offset - f->subst_offset);
	}
	return f;
}

/* A filesystem with a wildcard in its name has been matched.
 * Create a filesystem entry representing the match.
 */
cvmfs_filesystem *cvmfs_filesystem::createMatch(char const *repo_name) const
{
	cvmfs_filesystem *f = new cvmfs_filesystem(*this);
	f->match_wildcard = false;
	f->host = repo_name;

	// get the part of repo_name that matched the wildcard (i.e. the front)
	std::string subst;
	size_t subst_len = f->host.length() - host.length();
	subst.append(repo_name,subst_len);

	// substitute it into the options as needed
	// (relies on wildcard_subst containing
	// indices in decreasing order, so substitutions
	// do not disturbe indices of remaining ones)
	std::list<int>::const_iterator it;
	for(it = wildcard_subst.begin();
		it != wildcard_subst.end();
		it++)
	{
		size_t pos = subst_offset + *it;
		f->cvmfs_options.insert(pos,subst);
	}

	return f;
}


/* Read configuration for CVMFS repositories accessible to parrot.
 * Expected format of the configuration string:
 *   repo_name/subpath:cvmfs_options repo_name2/subpath:cvmfs_options ...
 *
 * The repo_name may begin with a * character, which matches one or
 * more characters in the requested path.  The character(s) matched by
 * the * replace any occurance of * in the options.  The subpath is
 * optional.  Literal spaces or asterisks in the configuration must be
 * escaped with a backslash.
 *
 * Example for /cvmfs/cms.cern.ch:
 * cms.cern.ch:pubkey=/path/to/cern.ch.pub,url=http://cvmfs-stratum-one.cern.ch/opt/cms
 *
 * Example with wildcard (using <*> to avoid compiler warning about nested comment):
 * *.cern.ch:pubkey=/path/to/cern.ch.pub,url=http://cvmfs-stratum-one.cern.ch/opt/<*>
 */
static void cvmfs_read_config()
{
	std::string cvmfs_options_buf;

	char *allow_switching = getenv("PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES");
	if( allow_switching && strcmp(allow_switching,"0")!=0) {
		pfs_cvmfs_repo_switching = true;
	}

	const char *cvmfs_options = pfs_cvmfs_repo_arg;
	if( !cvmfs_options ) {
		cvmfs_options = getenv("PARROT_CVMFS_REPO");
	}
	if( !cvmfs_options ) {
		cvmfs_options = default_cvmfs_repo;
	}
	if( !cvmfs_options || !cvmfs_options[0] ) {
		debug(D_CVMFS|D_NOTICE, "No CVMFS filesystems have been configured.  To access CVMFS, you must configure PARROT_CVMFS_REPO.");
		return;
	}

	while( isspace(*cvmfs_options) ) {
		cvmfs_options++;
	}

	while( *cvmfs_options ) {
		std::string repo_name;
		std::string subpath;
		std::string options;
		std::list<int> wildcard_subst;

		// first comes optional wildcard in repo name
		bool contains_wildcard = false;
		if( *cvmfs_options == '*' ) {
			contains_wildcard = true;
			cvmfs_options++;
		}
		// next comes the repo name
		for(; *cvmfs_options; cvmfs_options++ ) {
			if( *cvmfs_options == '/' || *cvmfs_options == ':' || isspace(*cvmfs_options) ) {
				break;
			}
			if( *cvmfs_options == '\\' ) {
				cvmfs_options++;
				if( *cvmfs_options == '\0' ) break;
			}
			repo_name += *cvmfs_options;
		}
		// next comes the optional repo subpath
		if( *cvmfs_options == '/' ) for(; *cvmfs_options; cvmfs_options++ ) {
			if( *cvmfs_options == ':' || isspace(*cvmfs_options) ) {
				break;
			}
			if( *cvmfs_options == '\\' ) {
				cvmfs_options++;
				if( *cvmfs_options == '\0' ) break;
			}
			subpath += *cvmfs_options;
		}
		if( *cvmfs_options == ':' ) {
			cvmfs_options++;
		}
		// next comes the options
		for(; *cvmfs_options; cvmfs_options++ ) {
			if( isspace(*cvmfs_options) ) {
				break;
			}
			if( *cvmfs_options == '*' ) {

				// must save substitutions in order of decreasing
				// index for createMatch()

				wildcard_subst.push_front(options.length());
				continue;
			}
			if( *cvmfs_options == '\\' ) {
				cvmfs_options++;
				if( *cvmfs_options == '\0' ) break;
			}
			options += *cvmfs_options;
		}

		if( repo_name == "<default-repositories>" ) {
			// placeholder for inserting the default configuration
			// in case user wants to add to it
			std::string new_options = default_cvmfs_repo;
			new_options += " ";
			if( !options.empty() ) {
				// user specified some additional options to be applied to the default repos
				int i = new_options.length();
				while( i>1 ) {
					i--;
					if( isspace(new_options[i]) && !isspace(new_options[i-1]) ) {
						new_options.insert(i,options);
						new_options.insert(i,",");
					}
				}
			}
			new_options += cvmfs_options; // append remaining unparsed contents of config string
			cvmfs_options_buf = new_options;
			cvmfs_options = cvmfs_options_buf.c_str();
		}
		else {
			cvmfs_filesystem *f = cvmfs_filesystem_create(repo_name.c_str(),contains_wildcard,subpath.c_str(),options.c_str(),wildcard_subst);
			if(f) {
				debug(D_CVMFS, "filesystem configured %c%s with repo path %s and options %s",
					  contains_wildcard ? '*' : ' ',
					  f->host.c_str(), f->path.c_str(), f->cvmfs_options.c_str());
				f->next = cvmfs_filesystem_list;
				cvmfs_filesystem_list = f;
			}
		}

		while( isspace(*cvmfs_options) ) {
			cvmfs_options++;
		}
	}
}

static cvmfs_filesystem *lookup_filesystem(pfs_name * name, char const **subpath_result)
{
	struct cvmfs_filesystem *f;
	const char *subpath;

	if(!name->host[0]) {
		errno = ENOENT;
		return 0;
	}

	if( !cvmfs_configured ) {
		cvmfs_configured = true;
		cvmfs_read_config();
	}

	if( !cvmfs_filesystem_list ) {
		errno = ENOENT;
		return 0;
	}

	size_t namelen = strlen(name->host);
	for(f = cvmfs_filesystem_list; f; f = f->next) {
		if( f->match_wildcard ) {
			size_t hostlen = f->host.length();
			if( hostlen >= namelen || strcmp(f->host.c_str(),name->host+(namelen-hostlen))!=0 ) {
				continue;
			}
		}
		else if(strcmp(f->host.c_str(),name->host)!=0) {
			continue;
		}

		// the host part patches, now check subpath
		subpath = compare_path_prefix(f->path.c_str(), name->rest);
		if(!subpath) {
			subpath = compare_path_prefix(name->rest, f->path.c_str());
			if(subpath) {
				errno = ENOENT;
				return 0;
			} else {
				continue;
			}
		}

		*subpath_result = subpath;

		if( f->match_wildcard ) {
			// create a new filesystem entry for this specific match of the pattern
			f = f->createMatch(name->host);
			debug(D_CVMFS, "filesystem configured from pattern: %s with repo path %s and options %s", f->host.c_str(), f->path.c_str(), f->cvmfs_options.c_str());

			// insert new instance at front of list, so in future we test it before
			// the general pattern
			f->next = cvmfs_filesystem_list;
			cvmfs_filesystem_list = f;
		}

		return f;
	}

	/*
	It is common for various programs to search for config files
	starting with dot in their parent directories, all the way up
	to the root.  This unnecessarily triggers the following error
	message.  Suppress the error message if the hostname begins
	with dot.
	*/
 
	if(name->host[0]!='.') {
		debug(D_CVMFS|D_NOTICE, "PARROT_CVMFS_REPO does not contain an entry for the CVMFS repository '%s'",name->host);
	}

	errno = ENOENT;
	return 0;
}

/*
Remove trailing slashes from a path
*/

static void chomp_slashes( char *s )
{
        char *t = s;

        if(!s) return;

        while(*t) {
                t++;
        }

        t--;

        while(*t=='/' && t!=s ) {
                *t=0;
                t--;
        }
}
static bool path_expand_symlink(struct pfs_name *path, struct pfs_name *xpath)
{

        /* During each iteration path->rest is decomposed into
	xpath->rest/path_head/path_tail. path_head is tried for
	symlink expansion, and on failing, added to xpath->rest. */
	   
	char path_head[PFS_PATH_MAX];
	char path_tail[PFS_PATH_MAX];
	char link_target[PFS_PATH_MAX];

	memcpy(xpath, path, sizeof(pfs_name));
	xpath->rest[0] = '\0';
	strncpy(path_tail, path->rest, PFS_PATH_MAX);

	do
	{
		string_split_path(path_tail, path_head, path_tail);

		int rest_len = strlen(xpath->rest);
		xpath->rest[rest_len] = '/';
		xpath->rest[rest_len + 1] = '\0';

		strncat(xpath->rest, path_head, PFS_PATH_MAX - 1);

		int rl = xpath->service->readlink(xpath, link_target, PFS_PATH_MAX - 1); 

		if(rl<0) {
			if(errno==EINVAL) {
				/* The prefix exists, but is not a link, so keep descending. */
				continue;
			} else {
				/* For any other reason, do not descend any further. */
				break;
			}
		} else {
			/* The prefix is a link, so process it. */

			if(link_target[0] != '/')
			{
				/* If link is relative, then we look
				for the rightmost slash, and subsitute
				that path with the link contents,
				collapsing if needed. */

				char *last_d = strrchr(xpath->rest, '/');
				if(last_d)
				{
					
					char path_relative[PFS_PATH_MAX];
					*(last_d + 1) = '\0';
					
					strncat(xpath->rest, link_target, PFS_PATH_MAX); 
					string_collapse_path(xpath->rest, path_relative, 1);
					snprintf(link_target, PFS_PATH_MAX, "/cvmfs/%s%s",
						 xpath->host, path_relative); 
				}
			}

			if(sscanf(link_target, "/cvmfs/%[^/]%[^\n]", xpath->host, path_head) < 1)
			{
				errno = EXDEV;
				return false;
			}

			snprintf(xpath->rest, PFS_PATH_MAX, "%s%s", path_head, path_tail);
			snprintf(xpath->path, PFS_PATH_MAX, "/cvmfs/%s%s", xpath->host, xpath->rest);
			strcpy(xpath->logical_name, xpath->path);

			debug(D_CVMFS, "expanding symlinks %s to %s\n", path->path, xpath->path);

			return true;
		}

	} while(path_head[0]);

	/* if we get here, then there was not an expansion and lookup fails */

	return false;
}

/*
Given a full PFS path name, search for an already-loaded
filesystem record.  If it exists, then search it for the
appropriate dirent.  If no filesystem record is found,
then search for and load the needed filesystem.
*/

bool cvmfs_dirent::lookup(pfs_name * path, bool follow_leaf_symlinks, bool expand_internal_symlinks)
{
	char const *subpath = NULL;

	cvmfs_filesystem *f = lookup_filesystem(path, &subpath);
	if(!f) {
		return false;
	}

	/*
	If we attempt to lookup a directory name using a path
	ending in a slash, CVMFS will *not* find it.
	So, we clean that up manually.
	*/

	chomp_slashes(path->rest);

	if(!cvmfs_activate_filesystem(f)) {
		errno = EIO;
		return false;
	}

	struct stat st;
	int rc;
	if( follow_leaf_symlinks ) {
		debug(D_CVMFS,"stat(%s)",path->rest);
		rc = cvmfs_stat(path->rest, &st);
	} else {
		debug(D_CVMFS,"lstat(%s)",path->rest);
		rc = cvmfs_lstat(path->rest, &st);
	}

	if(rc != 0) {
		/* lookup may have failed because some of the path
		   components are symlinks. In that case, we try each of the path
		   components as symlinks. If we do not find one, lookup fails. */
		struct pfs_name xpath;
		if(expand_internal_symlinks && path_expand_symlink(path, &xpath))
			return lookup(&xpath, follow_leaf_symlinks, 1);
		else
			return false;
	}
	
	name = strdup(subpath);
	mode = st.st_mode;
	size = st.st_size;
	inode = st.st_ino;
	mtime = st.st_mtime;

	return true;
}


class pfs_file_cvmfs:public pfs_file {
      private:
	int fd;
	pfs_stat info;
	pfs_off_t last_offset;

      public:
	pfs_file_cvmfs(pfs_name * n, int fd_arg, cvmfs_dirent & d):pfs_file(n) {
		fd = fd_arg;
		last_offset = 0;
		cvmfs_dirent_to_stat(&d, &info);
	}

	virtual int close() {
		return cvmfs_close(fd);
	}

	virtual pfs_ssize_t read(void *d, pfs_size_t length, pfs_off_t offset) {
		pfs_ssize_t result;

		debug(D_LOCAL, "read %d 0x%x %lld %lld", fd, d, length, offset);

		if(offset != last_offset)
			::lseek64(fd, offset, SEEK_SET);
		result =::read(fd, d, length);
		if(result > 0)
			last_offset = offset + result;

		return result;
	}

	virtual int fstat(struct pfs_stat *i) {
		*i = info;
		return 0;
	}

	/*
	   This is a compatibility hack.
	   This filesystem is read only, so locks make no sense.
	   This simply satisfies some programs that insist upon it.
	 */
	virtual int flock(int op) {
		return 0;
	}

	virtual pfs_ssize_t get_size() {
		return info.st_size;
	}

};

class pfs_service_cvmfs:public pfs_service {
      public:
	virtual int get_default_port() {
		return 0;
	}

	virtual int is_seekable() {
		// CVMFS has its own cache, and the file descriptors returned
		// by cvmfs_open are just handles to whole files in the CVMFS
		// cache.  Telling parrot that the handle is seekable also
		// causes parrot not to copy the files from the CVMFS cache
		// into the parrot cache.

		return 1;
	}

	virtual pfs_file *open(pfs_name * name, int flags, mode_t mode) {
		struct cvmfs_dirent d;

		if(!d.lookup(name, 1, 1)) {
			// errno is set by lookup()
			return 0;
		}

		/* cvmfs_open does not work with directories (it gives a 'fail to fetch' error). */
		if(S_ISDIR(d.mode)) {
			errno = EISDIR;
			return 0;
		}

		debug(D_CVMFS,"open(%s)",name->rest);
		int fd = cvmfs_open(name->rest);

		if(fd<0) return 0;

		return new pfs_file_cvmfs(name, fd, d);
	}

	pfs_dir *getdir(pfs_name * name) {
		struct cvmfs_dirent d;

		/*
		If the root of the CVFMS filesystem is requested,
		we must generate it internally, containing the
		list of the known filesystems.
		*/

		if(!name->host[0]) {
			pfs_dir *dir = new pfs_dir(name);
			dir->append(".");
			dir->append("..");
			struct cvmfs_filesystem *f = cvmfs_filesystem_list;
			while(f) {
				/* If the host begins with dot, then it is a wildcard entry. */
				/* Otherwise, it is a normal entry. */
				const char *host = f->host.c_str();
				if(host && host[0]!='.') {
					dir->append(host);
				}
				f = f->next;
			}
			return dir;
		}

		/*
		Otherwise, go to CVMFS for the directory liting.
		*/

		if(!d.lookup(name, 1, 1)) {
			return 0;
		}

		if(!S_ISDIR(d.mode)) {
			errno = ENOTDIR;
			return 0;
		}

		pfs_dir *dir = new pfs_dir(name);

		char **buf = NULL;
		size_t buflen = 0;

		debug(D_CVMFS, "getdir(%s)", name->rest);
		int rc = cvmfs_listdir(name->rest, &buf, &buflen);

		if(rc<0) return 0;

		int i;
		for(i = 0; buf[i]; i++) {
			dir->append(buf[i]);
			free(buf[i]);
		}
		free(buf);

		return dir;
	}

	virtual int anystat(pfs_name * name, struct pfs_stat *info, int follow_leaf_links, int expand_internal_symlinks ) {
		struct cvmfs_dirent d;

		/*
		If we get stat("/cvmfs") then construct a dummy
		entry that looks like a directory.
		*/

		if(!name->host[0]) {
                        pfs_service_emulate_stat(name,info);
                        info->st_mode = S_IFDIR | 0555;
			return 0;
		}

		/*
		Otherwise, do the lookup in CVMFS itself.
		*/

		if(!d.lookup(name, follow_leaf_links, expand_internal_symlinks)) {
			return -1;
		}

		cvmfs_dirent_to_stat(&d, info);

		return 0;
	}

	virtual int lstat(pfs_name * name, struct pfs_stat *info) {
		return anystat(name,info,0,1);
	}

	virtual int stat(pfs_name * name, struct pfs_stat *info) {
		return anystat(name,info,1,1);
	}

	virtual int unlink(pfs_name * name) {
		errno = EROFS;
		return -1;
	}

	virtual int access(pfs_name * name, mode_t mode) {
		struct pfs_stat info;
		if(this->stat(name, &info) == 0) {
			if(mode & W_OK) {
				errno = EROFS;
				return -1;
			} else {
				return 0;
			}
		} else {
			return -1;
		}
	}

	virtual int chmod(pfs_name * name, mode_t mode) {
		errno = EROFS;
		return -1;
	}

	virtual int chown(pfs_name * name, uid_t uid, gid_t gid) {
		errno = EROFS;
		return -1;
	}

	virtual int lchown(pfs_name * name, uid_t uid, gid_t gid) {
		errno = EROFS;
		return -1;
	}

	virtual int truncate(pfs_name * name, pfs_off_t length) {
		errno = EROFS;
		return -1;
	}

	virtual int utime(pfs_name * name, struct utimbuf *buf) {
		errno = EROFS;
		return -1;
	}

	virtual int rename(pfs_name * oldname, pfs_name * newname) {
		errno = EROFS;
		return -1;
	}

	virtual int chdir(pfs_name * name, char *newpath) {
		struct pfs_stat info;

		if(this->stat(name, &info) == 0) {
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

	virtual int link(pfs_name * oldname, pfs_name * newname) {
		errno = EROFS;
		return -1;
	}

	virtual int symlink(const char *linkname, pfs_name * newname) {
		errno = EROFS;
		return -1;
	}

	virtual int readlink(pfs_name * name, char *buf, pfs_size_t bufsiz) {

                /*
                If we get readlink("/cvmfs"), return not-a-link.
                */

                if(!name->host[0]) {
			errno = EINVAL;
			return -1;
                }

                /*
                Otherwise, do the lookup in CVMFS itself.
                */

		struct cvmfs_dirent d;

		if(!d.lookup(name, 0, 0)) {
			return -1;
		}

		if(S_ISLNK(d.mode)) {
			debug(D_CVMFS, "readlink(%s)", name->rest);
			int rc = cvmfs_readlink(name->rest, buf, bufsiz);

			if(rc < 0) return rc;

			return strlen(buf);
		} else {
			errno = EINVAL;
			return -1;
		}
	}

	virtual int mkdir(pfs_name * name, mode_t mode) {
		errno = EROFS;
		return -1;
	}

	virtual int rmdir(pfs_name * name) {
		errno = EROFS;
		return -1;
	}
};

static pfs_service_cvmfs pfs_service_cvmfs_instance;
pfs_service *pfs_service_cvmfs = &pfs_service_cvmfs_instance;

#endif

