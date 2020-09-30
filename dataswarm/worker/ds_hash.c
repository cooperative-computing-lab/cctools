
#include "ds_hash.h"

#include "md5.h"
#include "stringtools.h"

#include <sys/stat.h>
#include <dirent.h>
#include <string.h>
#include <stdlib.h>

char * ds_hash( const char *path, int64_t *size )
{
	struct stat info;

	int result = stat(path,&info);
	if(result<0) return 0;

	if(S_ISDIR(info.st_mode)) {
		unsigned char digest[MD5_DIGEST_LENGTH];
		md5_context_t context;
		md5_init(&context);

		DIR *dir = opendir(path);
		if(!dir) return 0;

		struct dirent *d;
		while((d=readdir(dir))) {
			if(!strcmp(d->d_name,".")) continue;
			if(!strcmp(d->d_name,"..")) continue;

			int64_t subsize;
			char *subpath = string_format("%s/%s",path,d->d_name);
			char *subhash = ds_hash(subpath,&subsize);

			char *substring = string_format("%s\n%s\n",d->d_name,subhash);
			md5_update(&context,subhash,strlen(subhash));

			free(substring);
			free(subhash);
			free(subpath);

			*size += subsize;
		}
		closedir(dir);
		md5_final(digest,&context);
		return strdup(md5_string(digest));
	} else {
		unsigned char digest[MD5_DIGEST_LENGTH];
		*size = info.st_size;
		md5_file(path,digest);
		return strdup(md5_string(digest));
	}

}
