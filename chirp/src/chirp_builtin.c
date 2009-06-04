
#include "chirp_thirdput.h"
#include "chirp_alloc.h"
#include "chirp_acl.h"

#include "sort_dir.h"
#include "debug.h"
#include "md5.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>

#define LONGTIME (60*60*24*365)

/*
Note that thirdput does not need to check ACLs, because thirdput already
does so recursively.  All of the rest of the builtin calls must check ACLs.
*/

extern int chirp_path_fix( char *path );

static int chirp_builtin_path_fix( const char *path, char *newpath )
{
	strcpy(newpath,path);
	return chirp_path_fix(newpath);
}

static INT64_T chirp_builtin_thirdput( const char *subject, const char *lpath, const char *hostport, const char *rpath )
{
	char newlpath[CHIRP_PATH_MAX];

	if(!chirp_builtin_path_fix(lpath,newlpath)) return -1;

	return chirp_thirdput(subject,newlpath,hostport,rpath,time(0)+LONGTIME);
}

static INT64_T chirp_builtin_rmall( const char *subject, const char *path )
{
	char newpath[CHIRP_PATH_MAX];

	if(!chirp_builtin_path_fix(path,newpath)) return -1;

	if(chirp_acl_check(newpath,subject,CHIRP_ACL_DELETE) || chirp_acl_check_dir(newpath,subject,CHIRP_ACL_DELETE)) {
		return chirp_alloc_rmall(newpath);
	} else {
		return -1;
	}
}

static INT64_T chirp_builtin_checksum_recursive( const char *subject, const char *path, INT64_T *bytes, INT64_T *files, INT64_T *dirs, char *digest )
{
	INT64_T result;
	char subpath[CHIRP_PATH_MAX];
	char subdigest[MD5_DIGEST_LENGTH];
	struct chirp_stat info;
	int i;

	result = chirp_alloc_lstat(path,&info);
	if(result<0) return -1;

	if(S_ISDIR(info.cst_mode)) {
		char **list;

		if(!sort_dir(path,&list,strcmp)) return -1;

		md5_context_t context;
		if(digest) md5_init(&context);

		for(i=0;list[i];i++) {
			if(!strcmp(list[i],".")) continue;
			if(!strcmp(list[i],"..")) continue;
			if(!strncmp(list[i],".__",3)) continue;

			sprintf(subpath,"%s/%s",path,list[i]);

			if(digest) {
				result = chirp_builtin_checksum_recursive(subject,subpath,bytes,files,dirs,subdigest);
			} else {
				result = chirp_builtin_checksum_recursive(subject,subpath,bytes,files,dirs,0);
			}
			
			if(result<0) break;

			if(digest) {
				md5_update(&context,list[i],strlen(list[i]));
				md5_update(&context,subdigest,sizeof(subdigest));
			}
		}

		sort_dir_free(list);

		if(digest) md5_final(digest,&context);

		(*dirs++);
		return result;
	} else if(S_ISREG(info.cst_mode)) {
		(*bytes)+=info.cst_size;
		(*files)++;
		if(digest) if(!md5_file(path,digest)) return -1;
		return 0;
	} else {
		return 0;
	}
}

static INT64_T chirp_builtin_checksum( const char *subject, const char *path )
{
	INT64_T result;
	unsigned char digest[MD5_DIGEST_LENGTH];
	char newpath[CHIRP_PATH_MAX];
	INT64_T bytes=0, files=0, dirs=0;

	if(!chirp_builtin_path_fix(path,newpath)) return -1;

	if(chirp_acl_check(newpath,subject,CHIRP_ACL_READ|CHIRP_ACL_LIST) || chirp_acl_check_dir(newpath,subject,CHIRP_ACL_READ|CHIRP_ACL_LIST)) {
		/* ok to proceed */
	} else {
		return -1;
	}

	result = chirp_builtin_checksum_recursive(subject,newpath,&bytes,&files,&dirs,digest);

	if(result==0) {
		printf("bytes %lld files %lld dirs %lld checksum %s\n",bytes,files,dirs,md5_string(digest));
	} else {
		printf("@checksum failed: %s\n",strerror(errno));
	}

	return result;
}

static INT64_T chirp_builtin_measure( const char *subject, const char *path )
{
	INT64_T result, bytes=0, files=0, dirs=0;
	char newpath[CHIRP_PATH_MAX];

	if(!chirp_builtin_path_fix(path,newpath)) return -1;

	if(chirp_acl_check(newpath,subject,CHIRP_ACL_LIST) || chirp_acl_check_dir(newpath,subject,CHIRP_ACL_LIST)) {
		/* ok to proceed */
	} else {
		return -1;
	}

	result = chirp_builtin_checksum_recursive(subject,newpath,&bytes,&files,&dirs,0);
	if(result>=0) {
		printf("%lld bytes %lld files %lld dirs\n",bytes,files,dirs);
	} else {
		printf("couldn't measure %s: %s\n",path,strerror(errno));
	}

	return result;
}

static INT64_T chirp_builtin_setacl_recursive( const char *subject, const char *path, const char *aclsubject, int aclflags )
{
	INT64_T result;
	char subpath[CHIRP_PATH_MAX];
	struct chirp_stat info;
	void *dir;
	char *d;

	result = chirp_alloc_lstat(path,&info);
	if(result<0) return -1;

	if(S_ISDIR(info.cst_mode)) {
		if(!chirp_acl_check(path,subject,CHIRP_ACL_ADMIN)) return -1;
		if(chirp_acl_set(path,aclsubject,aclflags,0)<0) return -1;

		dir = chirp_alloc_opendir(path);
		if(!dir) return -1;

		while((d=chirp_alloc_readdir(dir))) {
			if(!strcmp(d,".")) continue;
			if(!strcmp(d,"..")) continue;
			if(!strncmp(d,".__",3)) continue;

			sprintf(subpath,"%s/%s",path,d);

			result = chirp_builtin_setacl_recursive(subject,subpath,aclsubject,aclflags);
			if(result<0) break;
		}

		chirp_alloc_closedir(dir);

		return result;
	} else {
		return 0;
	}
}

static INT64_T chirp_builtin_setacl( const char *subject, const char *path, const char *aclsubject, const char* aclstring )
{
	char newpath[CHIRP_PATH_MAX];
	if(!chirp_builtin_path_fix(path,newpath)) return -1;
	return chirp_builtin_setacl_recursive(subject,newpath,aclsubject,chirp_acl_text_to_flags(aclstring));
}

INT64_T chirp_builtin( const char *subject, int argc, char *argv[] )
{
	int result;

	if(!strcmp(argv[0],"@thirdput")) {
		if(argc!=4) {
			printf("use: @thirdput <sourcepath> <hostport> <targetpath>\n");
			result = -1;
		} else {
			result = chirp_builtin_thirdput(subject,argv[1],argv[2],argv[3]);
		}
	} else if(!strcmp(argv[0],"@checksum")) {
		if(argc!=2) {
			printf("use: @checksum <path>\n");
			result = -1;
		} else {
			result = chirp_builtin_checksum(subject,argv[1]);
		}
	} else if(!strcmp(argv[0],"@setacl")) {
		if(argc!=4) {
			printf("use: @setacl <path> <subject> <rights>\n");
			result = -1;
		} else {
			result = chirp_builtin_setacl(subject,argv[1],argv[2],argv[3]);
		}
	} else if(!strcmp(argv[0],"@rmall")) {
		if(argc!=2) {
			printf("use: @rmall <path>\n");
			result = -1;
		} else {
			result = chirp_builtin_rmall(subject,argv[1]);
		}
	} else if(!strcmp(argv[0],"@measure")) {
		if(argc!=2) {
			printf("use: @measure <path>\n");
			result = -1;
		} else {
			result = chirp_builtin_measure(subject,argv[1]);
		}
	} else {
		errno = EINVAL;
		result = -1;
	}

	/*
	Swap the sense of results.
	For Chirp API calls, success is >=0, failure is < 0
	For Unix programs, success is 0, failure is non zero.
	*/

	if(result<0) {
		printf("%s failed: %s\n",argv[0],strerror(errno));
		result = 1;
	} else {
		result = 0;
	}

	exit(result);
}

