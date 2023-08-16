/*
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef HAS_CVMFS

#include "pfs_service.h"
#include <libcvmfs.h>

#ifndef LIBCVMFS_VERSION
	#define LIBCVMFS_VERSION 1
#endif
#ifndef LIBCVMFS_REVISION
	#define LIBCVMFS_REVISION 0
#endif


extern "C" {
#include "buffer.h"
#include "debug.h"
#include "path.h"
#include "stringtools.h"
#include "create_dir.h"
#include "jx.h"
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
#include <sys/file.h>
#include <time.h>
#include <assert.h>
#include <string>
#include <list>

extern int pfs_main_timeout;
extern char pfs_temp_dir[];
extern const char * pfs_cvmfs_repo_arg;
extern const char * pfs_cvmfs_config_arg;
extern bool pfs_cvmfs_repo_switching;
extern char pfs_cvmfs_alien_cache_dir[];
extern char pfs_cvmfs_locks_dir[];
extern bool pfs_cvmfs_enable_alien;
extern char pfs_cvmfs_option_file[];
extern struct jx *pfs_cvmfs_options;

extern char * pfs_cvmfs_http_proxy;


static bool cvmfs_configured = false;
static struct cvmfs_filesystem *cvmfs_active_filesystem = 0;

#define CERN_KEY_PLACEHOLDER     "<BUILTIN-cern.ch.pub>"
#define CERN_IT1_KEY_PLACEHOLDER "<BUILTIN-cern-it1.ch.pub>"
#define CERN_IT4_KEY_PLACEHOLDER "<BUILTIN-cern-it4.ch.pub>"
#define CERN_IT5_KEY_PLACEHOLDER "<BUILTIN-cern-it5.ch.pub>"
#define OASIS_KEY_PLACEHOLDER    "<BUILTIN-opensciencegrid.org.pub>"

/* All repositories are matched in order, therefore we write them from less to more specific */
static const char *default_cvmfs_repo =
"*:try_local_filesystem \
\
 *.cern.ch:pubkey=" CERN_KEY_PLACEHOLDER ":" CERN_IT1_KEY_PLACEHOLDER ":" CERN_IT4_KEY_PLACEHOLDER ":" CERN_IT5_KEY_PLACEHOLDER ",url=http://cvmfs-stratum-one.cern.ch/cvmfs/*.cern.ch;http://cernvmfs.gridpp.rl.ac.uk/cvmfs/*.cern.ch;http://cvmfs.racf.bnl.gov/cvmfs/*.cern.ch \
\
 *.opensciencegrid.org:pubkey=" OASIS_KEY_PLACEHOLDER ",url=http://oasis-replica.opensciencegrid.org:8000/cvmfs/*;http://cvmfs.fnal.gov:8000/cvmfs/*;http://cvmfs.racf.bnl.gov:8000/cvmfs/*";

#if LIBCVMFS_VERSION > 1
static const char *default_cvmfs_global_config = "change_to_cache_directory,log_prefix=libcvmfs";
#endif

static bool wrote_cern_key;
static std::string cern_key_fname;
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

static bool wrote_oasis_key;
static std::string oasis_key_fname;
static const char *oasis_key_text =
"-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqQGYXTp9cRcMbGeDoijB\n\
gKNTCEpIWB7XcqIHVXJjfxEkycQXMyZkB7O0CvV3UmmY2K7CQqTnd9ddcApn7BqQ\n\
/7QGP0H1jfXLfqVdwnhyjIHxmV2x8GIHRHFA0wE+DadQwoi1G0k0SNxOVS5qbdeV\n\
yiyKsoU4JSqy5l2tK3K/RJE4htSruPCrRCK3xcN5nBeZK5gZd+/ufPIG+hd78kjQ\n\
Dy3YQXwmEPm7kAZwIsEbMa0PNkp85IDkdR1GpvRvDMCRmUaRHrQUPBwPIjs0akL+\n\
qoTxJs9k6quV0g3Wd8z65s/k5mEZ+AnHHI0+0CL3y80wnuLSBYmw05YBtKyoa1Fb\n\
FQIDAQAB\n\
-----END PUBLIC KEY-----\n\
";

static bool wrote_cern_it1_key;
static std::string cern_it1_key_fname;
static const char *cern_it1_key_text =
"-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAo8uKvscgW7FNxzb65Uhm\n\
yr8jPJiyrl2kVzb/hhgdfN14C0tCbfFoE6ciuZFg+9ytLeiL9pzM96gSC+atIFl4\n\
7wTgtAFO1W4PtDQBwA/IG2bnwvNrzk19ob0JYhjZlS9tYKeh7TKCub55+vMwcEbP\n\
urzo3WSNCzJngiGMh1vM5iSlGLpCdSGzdwxLGwc1VjRM7q3KAd7M7TJCynKqXZPX\n\
R2xiD6I/p4xv39AnwphCFSmDh0MWE1WeeNHIiiveikvvN+l8d/ZNASIDhKNCsz6o\n\
aFDsGXvjGy7dg43YzjSSYSFGUnONtl5Fe6y4bQZj1LEPbeInW334MAbMwYF4LKma\n\
yQIDAQAB\n\
-----END PUBLIC KEY-----\n\
";

static bool wrote_cern_it4_key;
static std::string cern_it4_key_fname;
static const char *cern_it4_key_text =
"-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzlAraXimfJP5ie0KtDAE\n\
rNUU5d9bzst+kqfhnb0U0OUtmCIbsueaDlbMmTdRSHMr+T0jI8i9CZxJwtxDqll+\n\
UuB3Li2hYBhk0tYTy29JJYvofVULvrw1kMSLKyTWnV30/MHjYxhLHoZWfdepTjVg\n\
lM0rP58K10wR3Z/AaaikOcy4z6P/MHs9ES1jdZqEBQEmmzKw5nf7pfU2QuVWJrKP\n\
wZ9XeYDzipVbMc1zaLEK0slE+bm2ge/Myvuj/rpYKT+6qzbasQg62abGFuOrjgKI\n\
X4/BVnilkhUfH6ssRKw4yehlKG1M5KJje2+y+iVvLbfoaw3g1Sjrf4p3Gq+ul7AC\n\
PwIDAQAB\n\
-----END PUBLIC KEY-----\n\
";

static bool wrote_cern_it5_key;
static std::string cern_it5_key_fname;
static const char *cern_it5_key_text =
"-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqFzLLZAg2xmHJLbbq0+N\n\
eYtjRDghUK5mYhARndnC3skFVowDTiqJVc9dIDX5zuxQ9HyC0iKM1HbvN64IH/Uf\n\
qoXLyZLiXbFwpg6BtEJxwhijdZCiCC5PC//Bb7zSFIVZvWjndujr6ejaY6kx3+jI\n\
sU1HSJ66pqorj+D1fbZCziLcWbS1GzceZ7aTYYPUdGZF1CgjBK5uKrEAoBsPgjWo\n\
+YOEkjskY7swfhUzkCe0YyMyAaS0gsWgYrY2ebrpauFFqKxveKWjDVBTGcwDhiBX\n\
60inUgD6CJXhUpvGHfU8V7Bv6l7dmyzhq/Bk2kRC92TIvxfaHRmS7nuknUY0hW6t\n\
2QIDAQAB\n\
-----END PUBLIC KEY-----\n\
";

#if LIBCVMFS_REVISION >= 23
static cvmfs_option_map *cvmfs_global_options_v2;
#endif

/*
A cvmfs_filesystem structure represents an entire
filesystem rooted at a given host and path.
All known filesystem are kept in a map in
cvmfs_filesystem_list
*/

class cvmfs_filesystem {
public:
	std::string host;
	std::string path;
	std::string cvmfs_options;
	  // index into cvmfs_options+subst_offset to insert chars matching * in host
	std::list<int> wildcard_subst;
	int subst_offset;
	bool match_wildcard;
	bool try_local_filesystem; // test for locally mounted cvmfs filesystem
	bool use_local_filesystem; // always use locally mounted cvmfs filesystem
	bool cvmfs_not_configured; // only local access is possible

#if LIBCVMFS_VERSION > 1
	cvmfs_context *cvmfs_ctx;
#endif

	cvmfs_filesystem *createMatch(char const *repo_name) const;
};

typedef std::list<cvmfs_filesystem*> CvmfsFilesystemList;
CvmfsFilesystemList cvmfs_filesystem_list;


int compat_cvmfs_open(const char *path) {
	assert (cvmfs_active_filesystem != NULL);
#if LIBCVMFS_VERSION == 1
	return cvmfs_open(path);
#else
	return cvmfs_open(cvmfs_active_filesystem->cvmfs_ctx, path);
#endif
}

pfs_ssize_t compat_cvmfs_read(int fd, void *d, pfs_size_t length, pfs_off_t offset, pfs_off_t last_offset) {
	pfs_ssize_t result;

	debug(D_CVMFS, "read %d %p %lld %lld", fd, d, (long long)length, (long long)offset);

#if LIBCVMFS_REVISION < 18
	if(offset != last_offset)
		::lseek64(fd, offset, SEEK_SET);
	result =::read(fd, d, length);
#else
	result = cvmfs_pread(cvmfs_active_filesystem->cvmfs_ctx, fd, d, length, offset);
#endif

	return result;
}

int compat_cvmfs_close(int fd) {
	assert (cvmfs_active_filesystem != NULL);
#if LIBCVMFS_VERSION == 1
	return cvmfs_close(fd);
#else
	return cvmfs_close(cvmfs_active_filesystem->cvmfs_ctx, fd);
#endif
}

int compat_cvmfs_readlink(const char *path, char *buf, size_t size) {
	assert (cvmfs_active_filesystem != NULL);
#if LIBCVMFS_VERSION == 1
	return cvmfs_readlink(path, buf, size);
#else
	return cvmfs_readlink(cvmfs_active_filesystem->cvmfs_ctx, path, buf, size);
#endif
}

int compat_cvmfs_stat(const char *path, struct stat *st) {
	assert (cvmfs_active_filesystem != NULL);
#if LIBCVMFS_VERSION == 1
	return cvmfs_stat(path, st);
#else
	return cvmfs_stat(cvmfs_active_filesystem->cvmfs_ctx, path, st);
#endif
}

int compat_cvmfs_lstat(const char *path, struct stat *st) {
	assert (cvmfs_active_filesystem != NULL);
#if LIBCVMFS_VERSION == 1
	return cvmfs_lstat(path, st);
#else
	return cvmfs_lstat(cvmfs_active_filesystem->cvmfs_ctx, path, st);
#endif
}

int compat_cvmfs_listdir(const char *path,char ***buf,size_t *buflen) {
	assert (cvmfs_active_filesystem != NULL);
#if LIBCVMFS_VERSION == 1
	return cvmfs_listdir(path, buf, buflen);
#else
	return cvmfs_listdir(cvmfs_active_filesystem->cvmfs_ctx, path, buf, buflen);
#endif
}


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

void cvmfs_parrot_logger(const char *msg)
{
	debug(D_CVMFS, "%s", msg);
}

static bool write_key(char const *key_text,char const *key_basename,std::string &full_key_fname)
{
	// Write keys per instance, avoiding race condition in which two parrot
	// instances try to write the same key. As a bonus, we clear the key files
	// on exit, since pfs_cvmfs_locks_dir is garbage collected on exit.
	full_key_fname = pfs_cvmfs_locks_dir;

	full_key_fname += "/cvmfs";

	if( !create_dir(full_key_fname.c_str(),0755) && errno != EEXIST ) {
		debug(D_CVMFS|D_NOTICE,"WARNING: failed to mkdir %s: errno=%d %s",
			  full_key_fname.c_str(), errno, strerror(errno));
	}

	full_key_fname += "/";
	full_key_fname += key_basename;

	int key_fd = open(full_key_fname.c_str(),O_WRONLY|O_CREAT|O_TRUNC|O_NOFOLLOW,0644);
	if( key_fd == -1 ) {
		debug(D_CVMFS|D_NOTICE,"ERROR: failed to open %s: errno=%d %s",
			  full_key_fname.c_str(), errno, strerror(errno));
		return false;
	}

	int n_to_write = strlen(key_text);
	int n_written = write(key_fd,key_text,n_to_write);
	if( n_written != n_to_write ) {
		debug(D_CVMFS|D_NOTICE,"ERROR: failed to write to %s: errno=%d %s",
			  full_key_fname.c_str(), errno, strerror(errno));
		close(key_fd);
		return false;
	}
	close(key_fd);

	return true;
}

static bool write_cern_key()
{
	if( wrote_cern_key ) {
		return true;
	}
	if( !write_key(cern_key_text,"cern.ch.pub",cern_key_fname) ) {
		return false;
	}
	wrote_cern_key = true;
	return true;
}

static bool write_oasis_key()
{
	if( wrote_oasis_key ) {
		return true;
	}
	if( !write_key(oasis_key_text,"opensciencegrid.org.pub",oasis_key_fname) ) {
		return false;
	}
	wrote_oasis_key = true;
	return true;
}

static bool write_cern_it1_key()
{
	if( wrote_cern_it1_key ) {
		return true;
	}
	if( !write_key(cern_it1_key_text,"cern_it1.ch.pub",cern_it1_key_fname) ) {
		return false;
	}
	wrote_cern_it1_key = true;
	return true;
}

static bool write_cern_it4_key()
{
	if( wrote_cern_it4_key ) {
		return true;
	}
	if( !write_key(cern_it4_key_text,"cern_it4.ch.pub",cern_it4_key_fname) ) {
		return false;
	}
	wrote_cern_it4_key = true;
	return true;
}

static bool write_cern_it5_key()
{
	if( wrote_cern_it5_key ) {
		return true;
	}
	if( !write_key(cern_it5_key_text,"cern_it5.ch.pub",cern_it5_key_fname) ) {
		return false;
	}
	wrote_cern_it5_key = true;
	return true;
}


static bool cvmfs_activate_filesystem(struct cvmfs_filesystem *f)
{
	if(cvmfs_active_filesystem == f) {
		return true;
	}

	if(cvmfs_active_filesystem != NULL && !pfs_cvmfs_repo_switching) {
		debug(D_CVMFS|D_NOTICE,
			  "ERROR: using multiple CVMFS repositories in a single parrot session "
			  "is not allowed.  Define PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES "
			  "to enable experimental support, which could result in parrot crashing "
			  "or performing poorly.");
		return false;
	}

#if LIBCVMFS_VERSION == 1
	if(cvmfs_active_filesystem != NULL) {
		static bool did_warning = false;
		if (!did_warning) {
			did_warning = true;
			debug(D_CVMFS,
				  "ERROR: using multiple CVMFS repositories in a single parrot session "
				  "is not fully supported.  PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES "
				  "has been defined, so switching now from %s to %s.  "
				  "Parrot may crash or perform poorly!",
				  cvmfs_active_filesystem->host.c_str(),
				  f->host.c_str());
		}

		debug(D_CVMFS, "cvmfs_fini()");
		cvmfs_fini();
		cvmfs_active_filesystem = NULL;
	}
#endif

#if LIBCVMFS_VERSION > 1
	if (f->cvmfs_ctx != NULL) {
		debug(D_CVMFS,"re-activating repository %s",f->host.c_str());
		cvmfs_active_filesystem = f;
		return true;
	}
#endif
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

	// check for references to the cern_it1.ch.pub key
	char const *cern_it1_key_pos = strstr(f->cvmfs_options.c_str(),CERN_IT1_KEY_PLACEHOLDER);
	if( cern_it1_key_pos ) {
		if( !write_cern_it1_key() ) {
			debug(D_CVMFS|D_NOTICE,
				  "ERROR: cannot load cvmfs repository %s, because failed to write cern_it1.ch.pub",
				  f->host.c_str());
			return false;
		}

		f->cvmfs_options.replace(cern_it1_key_pos-f->cvmfs_options.c_str(),strlen(CERN_IT1_KEY_PLACEHOLDER),cern_it1_key_fname);
	}

	// check for references to the cern_it4.ch.pub key
	char const *cern_it4_key_pos = strstr(f->cvmfs_options.c_str(),CERN_IT4_KEY_PLACEHOLDER);
	if( cern_it4_key_pos ) {
		if( !write_cern_it4_key() ) {
			debug(D_CVMFS|D_NOTICE,
				  "ERROR: cannot load cvmfs repository %s, because failed to write cern_it4.ch.pub",
				  f->host.c_str());
			return false;
		}

		f->cvmfs_options.replace(cern_it4_key_pos-f->cvmfs_options.c_str(),strlen(CERN_IT4_KEY_PLACEHOLDER),cern_it4_key_fname);
	}

	// check for references to the cern_it5.ch.pub key
	char const *cern_it5_key_pos = strstr(f->cvmfs_options.c_str(),CERN_IT5_KEY_PLACEHOLDER);
	if( cern_it5_key_pos ) {
		if( !write_cern_it5_key() ) {
			debug(D_CVMFS|D_NOTICE,
				  "ERROR: cannot load cvmfs repository %s, because failed to write cern_it5.ch.pub",
				  f->host.c_str());
			return false;
		}

		f->cvmfs_options.replace(cern_it5_key_pos-f->cvmfs_options.c_str(),strlen(CERN_IT5_KEY_PLACEHOLDER),cern_it5_key_fname);
	}

	// check for references to the built-in opensciencegrid.org.pub key
	char const *oasis_key_pos = strstr(f->cvmfs_options.c_str(),OASIS_KEY_PLACEHOLDER);
	if( oasis_key_pos ) {
		if( !write_oasis_key() ) {
			debug(D_CVMFS|D_NOTICE,
				  "ERROR: cannot load cvmfs repository %s, because failed to write opensciencegrid.org.pub",
				  f->host.c_str());
			return false;
		}

		f->cvmfs_options.replace(oasis_key_pos-f->cvmfs_options.c_str(),strlen(OASIS_KEY_PLACEHOLDER),oasis_key_fname);
	}

#if LIBCVMFS_VERSION == 1

	// Internally, cvmfs will attempt to lock this file,
	// and then block silently if it cannot run.  Since
	// we are linked against cvmfs anyhow, we use the same
	// routine to check here explicitly.  There is still
	// a race condition here, but now the user has a good
	// chance of getting a useful error message before
	// cvmfs_init blocks.

	char lockfile[PFS_PATH_MAX];
	sprintf(lockfile,"%s/cvmfs/%s/lock.%s",pfs_temp_dir,f->host.c_str(),f->host.c_str());
	debug(D_CVMFS,"checking lock file %s",lockfile);
	int fd = open(lockfile,O_RDONLY|O_CREAT,0600);
	if(fd>=0) {
		int result = flock(fd,LOCK_EX|LOCK_NB);

		close(fd);

		if(result<0) {
			debug(D_NOTICE|D_CVMFS,"waiting for another process to release cvmfs lock %s",lockfile);
		}
	}

	debug(D_CVMFS, "cvmfs_init(%s)", f->cvmfs_options.c_str());
	cvmfs_set_log_fn(cvmfs_parrot_logger);
	int rc = cvmfs_init(f->cvmfs_options.c_str());

	if(rc != 0) {
		return false;
	}
#elif LIBCVMFS_REVISION < 23
	debug(D_CVMFS, "cvmfs_attach_repo(%s)", f->cvmfs_options.c_str());
	f->cvmfs_ctx = cvmfs_attach_repo(f->cvmfs_options.c_str());
	if (f->cvmfs_ctx == NULL) {
		return false;
	}
#else
	cvmfs_option_map *fs_options = cvmfs_options_clone_legacy(cvmfs_global_options_v2, f->cvmfs_options.c_str());
	if (!fs_options) {
		return false;
	}
	char *fqrn = cvmfs_options_get(fs_options, "CVMFS_FQRN");
	if (!fqrn) {
		return false;
	}

	char *repo_options_str = cvmfs_options_dump(fs_options);
	debug(D_CVMFS, "cvmfs_attach_repo_v2(%s)", repo_options_str);
	cvmfs_options_free(repo_options_str);
	cvmfs_errors retval = cvmfs_attach_repo_v2(fqrn, fs_options, &f->cvmfs_ctx);
	free(fqrn);
	if (retval != LIBCVMFS_ERR_OK) {
		return false;
	}
	cvmfs_adopt_options(f->cvmfs_ctx, fs_options);
#endif

	cvmfs_active_filesystem = f;

	return true;
}

static cvmfs_filesystem *cvmfs_filesystem_create(const char *repo_name, bool wildcard, const char *path, const char *user_options, std::list<int> const &subst)
{
	cvmfs_filesystem *f = new cvmfs_filesystem;
	f->subst_offset = 0;
	f->host = repo_name;
	f->path = path;
#if LIBCVMFS_VERSION > 1
	f->cvmfs_ctx = NULL;
#endif

#if LIBCVMFS_REVISION < 23
	if( !pfs_cvmfs_http_proxy || !pfs_cvmfs_http_proxy[0] || !strcmp(pfs_cvmfs_http_proxy,"DIRECT") ) {
		if( !strstr(user_options,"proxies=") ) {
			debug(D_CVMFS|D_NOTICE,"CVMFS requires an http proxy.  None has been configured!");
			debug(D_CVMFS,"Ignoring configuration of CVMFS repository %s:%s",repo_name,user_options);
			delete f;
			return NULL;
		}
	}
#endif

	if( !user_options ) {
		user_options = "";
	}

	int repo_name_offset                   = -1;
	int repo_name_in_cachedir_offset       = -1;
	int repo_name_in_alien_cachedir_offset = -1;

	int enable_alien_on_this_repository = pfs_cvmfs_enable_alien;
	if(enable_alien_on_this_repository && strstr(user_options,"quota_limit=")) {
		debug(D_NOTICE, "Disabling alien cache since it is mutually exclusive with quota limits.\n");
		enable_alien_on_this_repository = false;
	}

#if LIBCVMFS_VERSION == 1
	char *buf = string_format("repo_name=%n%s,cachedir=%s/cvmfs/%n%s,%s%s%s%n%s%stimeout=%d,timeout_direct=%d%s%s,%n%s",
			&repo_name_offset,
			repo_name,

			enable_alien_on_this_repository ? pfs_cvmfs_locks_dir : pfs_temp_dir,
			&repo_name_in_cachedir_offset,
			repo_name,

			enable_alien_on_this_repository ? "alien_cachedir=" : "",
			enable_alien_on_this_repository ? pfs_cvmfs_alien_cache_dir : "",
			enable_alien_on_this_repository ? "/" : "",
			&repo_name_in_alien_cachedir_offset,
			enable_alien_on_this_repository ? repo_name : "",
			enable_alien_on_this_repository ? "," : "",

			pfs_main_timeout,
			pfs_main_timeout,
			pfs_cvmfs_http_proxy ? ",proxies=" : "",
			pfs_cvmfs_http_proxy ? pfs_cvmfs_http_proxy : "",
			&f->subst_offset,
			user_options);
#else
	char *buf = string_format("repo_name=%n%s,timeout=%d,timeout_direct=%d%s%s,%n%s",
			&repo_name_offset,
			repo_name,

			pfs_main_timeout,
			pfs_main_timeout,
			pfs_cvmfs_http_proxy ? ",proxies=" : "",
			pfs_cvmfs_http_proxy ? pfs_cvmfs_http_proxy : "",
			&f->subst_offset,
			user_options);
#endif

	f->use_local_filesystem = false;
	f->try_local_filesystem = false;
	char *try_local_filesystem_pos = strstr(buf,"try_local_filesystem");
	if( try_local_filesystem_pos ) {
		f->try_local_filesystem = true;
		char *rest = try_local_filesystem_pos+strlen("try_local_filesystem");
		memmove(try_local_filesystem_pos,rest,strlen(rest)+1);
	}

	// see if this entry is just for local filesystem access, or if it supports parrot cvmfs access
	f->cvmfs_not_configured = true;
	if( strstr(buf,"url") ) {
		f->cvmfs_not_configured = false;
	}

	f->cvmfs_options = buf;

	f->match_wildcard = wildcard;
	f->wildcard_subst = subst;
	if( wildcard ) {
		// make a note to fix up the repo name later
		// Order of the following is important! Substitute in reverse order as
		// they appear in buf.
		if(enable_alien_on_this_repository && repo_name_in_alien_cachedir_offset >= 0) {
			f->wildcard_subst.push_back(repo_name_in_alien_cachedir_offset - f->subst_offset);
		}
		if(repo_name_in_cachedir_offset >= 0) {
			f->wildcard_subst.push_back(repo_name_in_cachedir_offset - f->subst_offset);
		}
		if(repo_name_offset >= 0){
			f->wildcard_subst.push_back(repo_name_offset - f->subst_offset);
		}
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
 * cms.cern.ch:pubkey=/path/to/cern.ch.pub,url=http://cvmfs-stratum-one.cern.ch/cvmfs/cms.cern.ch
 *
 * Example with wildcard (using <*> to avoid compiler warning about nested comment):
 * *.cern.ch:pubkey=/path/to/cern.ch.pub,url=http://cvmfs-stratum-one.cern.ch/cvmfs/<*>.cern.ch
 */
static void cvmfs_read_config()
{
	assert (!cvmfs_configured);
	std::string cvmfs_options_buf;

	debug(D_CVMFS, "Using libcvmfs version: %d", LIBCVMFS_VERSION);

#if LIBCVMFS_REVISION < 23
	if (pfs_cvmfs_options) {
		debug(D_CVMFS|D_NOTICE, "The installed libcvmfs version does not support passing options from the command line");
		return;
	}
	if (strlen(pfs_cvmfs_option_file) > 0) {
		debug(D_CVMFS|D_NOTICE, "The installed libcvmfs version does not support passing an option file");
		return;
	}
#endif

#if LIBCVMFS_VERSION == 1
	char *allow_switching = getenv("PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES");
	if( allow_switching && strcmp(allow_switching,"0")!=0) {
		pfs_cvmfs_repo_switching = true;
  }
#else
	pfs_cvmfs_repo_switching = true;
#endif
	{
		buffer_t B;
		buffer_init(&B);
		buffer_abortonfailure(&B, 1);
		buffer_putfstring(&B, "parrot-%d-%d-%d", CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO);
		if(getenv("CERNVM_UUID"))
			buffer_putfstring(&B, "-%s", getenv("CERNVM_UUID")); /* can't use space since that is filtered, use '-' */
		setenv("CERNVM_UUID", buffer_tostring(&B), 1);
		debug(D_CVMFS, "setenv CERNVM_UUID=`%s'", buffer_tostring(&B));
		buffer_free(&B);
	}

#if LIBCVMFS_VERSION > 1
	const char *cvmfs_global_options = pfs_cvmfs_config_arg;
	if ( !cvmfs_global_options ) {
		cvmfs_global_options = getenv("PARROT_CVMFS_CONFIG");
	}
#if LIBCVMFS_REVISION < 23
	if ( !cvmfs_global_options ) {
		cvmfs_global_options = string_format("cache_directory=%s%s%s%s%s",
				pfs_cvmfs_alien_cache_dir,
				pfs_cvmfs_enable_alien  ?  ",lock_directory="  : "",
				pfs_cvmfs_enable_alien  ?  pfs_cvmfs_locks_dir : "",
				pfs_cvmfs_enable_alien  ?  ",alien_cache,"     : "",
				default_cvmfs_global_config);
	}
	if ( !cvmfs_global_options || !cvmfs_global_options[0] ) {
		debug(D_CVMFS|D_NOTICE, "No global CVMFS configuration found. To enable CVMFS access, you must configure PARROT_CVMFS_CONFIG.");
		return;
	}

	debug(D_CVMFS|D_DEBUG, "Using CVMFS global options: %s", cvmfs_global_options);

	cvmfs_set_log_fn(cvmfs_parrot_logger);
	const int init_retval = cvmfs_init(cvmfs_global_options);
	if (init_retval != 0) {
		debug(D_CVMFS|D_NOTICE, "ERROR: failed to initialize cvmfs (%d)", init_retval);
		return;
	}
#else
	if (cvmfs_global_options) {
		cvmfs_global_options_v2 = cvmfs_options_init_legacy(cvmfs_global_options);
		assert(cvmfs_global_options_v2);
	} else {
		cvmfs_global_options_v2 = cvmfs_options_init_legacy(default_cvmfs_global_config);
		assert(cvmfs_global_options_v2);
		cvmfs_options_set(cvmfs_global_options_v2, "CVMFS_CACHE_DIR", pfs_cvmfs_alien_cache_dir);
		if (pfs_cvmfs_enable_alien) {
			cvmfs_options_set(cvmfs_global_options_v2, "CVMFS_WORKSPACE", pfs_cvmfs_locks_dir);
			cvmfs_options_set(cvmfs_global_options_v2, "CVMFS_ALIEN_CACHE", pfs_cvmfs_alien_cache_dir);
		}
	}

	if (strlen(pfs_cvmfs_option_file) > 0) {
		if (!cvmfs_global_options_v2) {
			cvmfs_global_options_v2 = cvmfs_options_init();
		}
		assert(cvmfs_global_options_v2);
		int rc = cvmfs_options_parse(cvmfs_global_options_v2, pfs_cvmfs_option_file);
		if (rc != 0) {
			debug(D_CVMFS|D_NOTICE, "ERROR: failed to parse %s", pfs_cvmfs_option_file);
			return;
		}
	}

	if (!cvmfs_global_options_v2) {
		debug(D_CVMFS|D_NOTICE, "No global CVMFS configuration found. To enable CVMFS access, you should use --cvmfs-option-file.");
		return;
	}

	if (pfs_cvmfs_options) {
		struct jx_pair *p;
		for (p = pfs_cvmfs_options->u.pairs; p; p = p->next) {
			if (jx_istype(p->key, JX_STRING) && jx_istype(p->value, JX_STRING)) {
				cvmfs_options_set(cvmfs_global_options_v2, p->key->u.string_value, p->value->u.string_value);
			}
		}
	}

	char *global_options_str = cvmfs_options_dump(cvmfs_global_options_v2);
	debug(D_CVMFS|D_DEBUG, "Using CVMFS global options: %s", global_options_str);
	cvmfs_options_free(global_options_str);

	cvmfs_errors retval = cvmfs_init_v2(cvmfs_global_options_v2);
	if (retval != LIBCVMFS_ERR_OK) {
		debug(D_CVMFS|D_DEBUG, "Unable to initialize libcvmfs");
		return;
	}
#endif
#endif

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
				cvmfs_filesystem_list.push_front(f);
			}
		}

		while( isspace(*cvmfs_options) ) {
			cvmfs_options++;
		}
	}
}

static cvmfs_filesystem *lookup_filesystem(pfs_name * name, char const **subpath_result)
{
	const char *subpath;

	if(!name->host[0]) {
		errno = ENOENT;
		return 0;
	}

	if( !cvmfs_configured ) {
		cvmfs_read_config();
		cvmfs_configured = true;
	}

	if( cvmfs_filesystem_list.empty() ) {
		errno = ENOENT;
		return 0;
	}

	size_t namelen = strlen(name->host);
	CvmfsFilesystemList::const_iterator i    = cvmfs_filesystem_list.begin();
	CvmfsFilesystemList::const_iterator iend = cvmfs_filesystem_list.end();
	for(; i != iend; ++i) {
		cvmfs_filesystem *f = *i;
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
			cvmfs_filesystem_list.push_front(f);
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

static int do_readlink(pfs_name *name, char *buf, pfs_size_t bufsiz, bool expand_internal_symlinks) {

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

	if(!d.lookup(name, 0, expand_internal_symlinks)) {
		if( errno == EAGAIN ) {
			class pfs_service *local = pfs_service_lookup_default();
			return local->readlink(name,buf,bufsiz);
		}
		return -1;
	}

	if(S_ISLNK(d.mode)) {
		debug(D_CVMFS, "readlink(%s)", d.name);
		int rc = compat_cvmfs_readlink(d.name, buf, bufsiz);

		if(rc < 0) return rc;

		return strlen(buf);
	} else {
		errno = EINVAL;
		return -1;
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
		path_split(path_tail, path_head, path_tail);

		int rest_len = strlen(xpath->rest);
		xpath->rest[rest_len] = '/';
		xpath->rest[rest_len + 1] = '\0';

		strncat(xpath->rest, path_head, PFS_PATH_MAX - 1);

		int rl = do_readlink(xpath, link_target, PFS_PATH_MAX - 1, false);

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
					path_collapse(xpath->rest, path_relative, 1);
					string_nformat(link_target, PFS_PATH_MAX, "/cvmfs/%s%s",
						 xpath->host, path_relative);
				}
			}

			if(sscanf(link_target, "/cvmfs/%[^/]%[^\n]", xpath->host, path_head) < 1)
			{
				/* The path points outside of cvmfs, we do not allow that. */
				debug(D_CVMFS, "refusing to follow path outside of cvmfs: '%s' -> '%s'", path->path, link_target);
				errno = ENOENT;
				return false;
			}

			string_nformat(xpath->rest, PFS_PATH_MAX, "%s%s", path_head, path_tail);
			string_nformat(xpath->path, PFS_PATH_MAX, "/cvmfs/%s%s", xpath->host, xpath->rest);
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
	if( f->try_local_filesystem ) {
		class pfs_service *local = pfs_service_lookup_default();
		struct pfs_name local_fs;

		snprintf(local_fs.rest,PFS_PATH_MAX,"/cvmfs/%s/%s",f->host.c_str(),f->path.c_str());
		local_fs.rest[PFS_PATH_MAX-1] = '\0';
		local_fs.is_local = 1;

		struct pfs_stat st;
		if( local->lstat(&local_fs,&st)==0 ) {
			f->use_local_filesystem = true;
			debug(D_CVMFS,"Found %s on local filesystem, so not using parrot cvmfs.",
				  local_fs.rest);
		}
		else if( f->cvmfs_not_configured ) {
			debug(D_CVMFS|D_NOTICE,"ERROR: Did not find %s on local filesystem (errno=%d %s), "
				  "and parrot has not been configured to know how to access this CVMFS repository",
				  local_fs.rest,errno,strerror(errno));
			return false;
		}
		else {
			debug(D_CVMFS,"Did not find %s on local filesystem (errno=%d %s), so using parrot cvmfs",
				  local_fs.rest,errno,strerror(errno));
		}
		f->try_local_filesystem = false; // For efficiency, only test local access once.
	}
	if( f->use_local_filesystem ) {
		// Tell caller to try again via the local filesystem.
		strcpy(path->rest,path->logical_name);
		path->is_local = 1;
		errno = EAGAIN;
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
		rc = compat_cvmfs_stat(path->rest, &st);
	} else {
		debug(D_CVMFS,"lstat(%s)",path->rest);
		rc = compat_cvmfs_lstat(path->rest, &st);
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

	name = strdup(path->rest);
	mode = st.st_mode;
	size = st.st_size;
	inode = st.st_ino;
	mtime = st.st_mtime;

	return true;
}


class pfs_file_cvmfs:public pfs_file {
	private:
		struct cvmfs_filesystem *filesystem;
		int fd;
		pfs_stat info;
		pfs_off_t last_offset;

	public:
		pfs_file_cvmfs(pfs_name * n, struct cvmfs_filesystem *fsys_arg, int fd_arg, cvmfs_dirent & d):pfs_file(n) {
			filesystem = fsys_arg;
			fd = fd_arg;
			last_offset = 0;
			cvmfs_dirent_to_stat(&d, &info);
		}

		virtual int close() {

			if(filesystem && filesystem != cvmfs_active_filesystem) {
				cvmfs_activate_filesystem(filesystem);
			}

			return compat_cvmfs_close(fd);
		}

		virtual pfs_ssize_t read(void *d, pfs_size_t length, pfs_off_t offset) {

			if(filesystem && filesystem != cvmfs_active_filesystem) {
				cvmfs_activate_filesystem(filesystem);
			}

			pfs_ssize_t result = compat_cvmfs_read(fd, d, length, offset, last_offset);
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
			if( errno == EAGAIN ) {
				class pfs_service *local = pfs_service_lookup_default();
				return local->open(name,flags,mode);
			}
			return 0;
		}

		/* cvmfs_open does not work with directories (it gives a 'fail to fetch' error). */
		if(S_ISDIR(d.mode)) {
			errno = EISDIR;
			return 0;
		}

		debug(D_CVMFS,"open(%s)", d.name);
		int fd = compat_cvmfs_open(d.name);

		if(fd<0) return 0;

		return new pfs_file_cvmfs(name, cvmfs_active_filesystem, fd, d);
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
			CvmfsFilesystemList::const_iterator i    = cvmfs_filesystem_list.begin();
			CvmfsFilesystemList::const_iterator iend = cvmfs_filesystem_list.end();
			for(; i != iend; ++i) {
				cvmfs_filesystem *f = *i;
				/* If the host begins with dot, then it is a wildcard entry. */
				/* Otherwise, it is a normal entry. */
				const char *host = f->host.c_str();
				if(host && host[0]!='.') {
					dir->append(host);
				}
			}
			return dir;
		}

		/*
		Otherwise, go to CVMFS for the directory liting.
		*/

		if(!d.lookup(name, 1, 1)) {
			if( errno == EAGAIN ) {
				class pfs_service *local = pfs_service_lookup_default();
				return local->getdir(name);
			}
			return 0;
		}

		if(!S_ISDIR(d.mode)) {
			errno = ENOTDIR;
			return 0;
		}

		pfs_dir *dir = new pfs_dir(name);

		char **buf = NULL;
		size_t buflen = 0;

		debug(D_CVMFS, "getdir(%s)", d.name);
		int rc = compat_cvmfs_listdir(d.name, &buf, &buflen);

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
		int rc = anystat(name,info,0,1);
		if( rc == -1 && errno == EAGAIN ) {
			class pfs_service *local = pfs_service_lookup_default();
			return local->lstat(name,info);
		}
		return rc;
	}

	virtual int stat(pfs_name * name, struct pfs_stat *info) {
		int rc = anystat(name,info,1,1);
		if( rc == -1 && errno == EAGAIN ) {
			class pfs_service *local = pfs_service_lookup_default();
			return local->stat(name,info);
		}
		return rc;
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
			// errno set by stat
			return -1;
		}
	}

	/*
	It matters to a few rare applications whether unlink
	and other write operations on non-existent files return
	ENOENT versus EROFS.  For these, we check for existence,
	and return EROFS otherwise.
	*/

	virtual int unlink(pfs_name * name) {
		return access(name,W_OK);
	}

	virtual int chmod(pfs_name * name, mode_t mode) {
		return access(name,W_OK);
	}

	virtual int chown(pfs_name * name, uid_t uid, gid_t gid) {
		return access(name,W_OK);
	}

	virtual int lchown(pfs_name * name, uid_t uid, gid_t gid) {
		return access(name,W_OK);
	}

	virtual int truncate(pfs_name * name, pfs_off_t length) {
		return access(name,W_OK);
	}

	virtual int utime(pfs_name * name, struct utimbuf *buf) {
		return access(name,W_OK);
	}

	virtual int rename(pfs_name * oldname, pfs_name * newname) {
		return access(oldname,W_OK);
	}

	virtual int link(pfs_name * oldname, pfs_name * newname) {
		return access(newname,W_OK);
	}

	virtual int symlink(const char *linkname, pfs_name * newname) {
		return access(newname,W_OK);
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

	virtual int readlink(pfs_name * name, char *buf, pfs_size_t bufsiz) {
			return do_readlink(name, buf, bufsiz, true);
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

/* vim: set noexpandtab tabstop=8: */
