
#include "ds_measure.h"

#include "md5.h"
#include "stringtools.h"
#include "debug.h"

#include <sys/stat.h>
#include <dirent.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

int64_t ds_measure_and_hash( const char *path, char *hash )
{
	struct stat info;

	int result = stat(path,&info);
	if(result<0) return -1;

	int64_t totalsize = 0;

	if(S_ISDIR(info.st_mode)) {
		unsigned char digest[MD5_DIGEST_LENGTH];
		md5_context_t context;
		md5_init(&context);

		DIR *dir = opendir(path);
		if(!dir) return -1;

		struct dirent *d;
		while((d=readdir(dir))) {
			if(!strcmp(d->d_name,".")) continue;
			if(!strcmp(d->d_name,"..")) continue;

			char subhash[33];
			char *subpath = string_format("%s/%s",path,d->d_name);

			int64_t subsize = ds_measure_and_hash(subpath,subhash);
			if(subsize<0) {
				debug(D_DATASWARM,"couldn't measure %s: %s",subpath,strerror(errno));
				totalsize = -1;
				break;
			}

			char *substring = string_format("%s\n%s\n",d->d_name,subhash);
			md5_update(&context,substring,strlen(substring));
			free(substring);

			free(subpath);

			totalsize += subsize;
		}
		closedir(dir);
		md5_final(digest,&context);
		strcpy(hash,md5_string(digest));
	} else {
		unsigned char digest[MD5_DIGEST_LENGTH];
		md5_file(path,digest);
		strcpy(hash,md5_string(digest));
		totalsize = info.st_size;
	}

	return totalsize;	
}

int64_t ds_measure( const char *path )
{
	struct stat info;

	int result = stat(path,&info);
	if(result<0) return -1;

	int64_t totalsize = 0;

	if(S_ISDIR(info.st_mode)) {
		DIR *dir = opendir(path);
		if(!dir) return -1;

		struct dirent *d;
		while((d=readdir(dir))) {
			if(!strcmp(d->d_name,".")) continue;
			if(!strcmp(d->d_name,"..")) continue;

			char *subpath = string_format("%s/%s",path,d->d_name);

			int64_t subsize = ds_measure(subpath);
			if(subsize<0) {
				debug(D_DATASWARM,"couldn't measure %s: %s",subpath,strerror(errno));
				totalsize = -1;
				break;
			}

			free(subpath);
			totalsize += subsize;
		}
		closedir(dir);
	} else {
		totalsize = info.st_size;
	}

	return totalsize;
}
