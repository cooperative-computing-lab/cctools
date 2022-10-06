/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "full_io.h"
#include "getopt.h"
#include "parrot_client.h"
#include "pfs_types.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <syscall.h>
#include <unistd.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int verbose_mode = 0;
static int recursive_mode = 0;
static int update_mode = 0;
static int symlink_mode = 0;
static int hardlink_mode = 0;
static int force_mode = 0;
static int interactive_mode = 0;

int copyfile_slow( const char *source, const char *target )
{
	char *buffer;
	int buffer_size = 65536;
	int s, t;
	int result, actual;
	int nerrors = 0;
	struct stat64 info;

	s = open(source,O_RDONLY,0);
	if(s<0) {
		fprintf(stderr,"parrot_cp: couldn't open '%s' for reading: %s\n",source,strerror(errno));
		return 1;
	}

	fstat64(s,&info);
	if(S_ISDIR(info.st_mode)) {
		errno = EISDIR;
		return -1;
	}

	t = open(target,O_WRONLY|O_CREAT|O_TRUNC,0777);
	if(t<0) {
		fprintf(stderr,"parrot_cp: couldn't open '%s' for writing: %s\n",target,strerror(errno));
		return 1;
	}

	buffer = malloc(buffer_size);

	while(1) {
		result = full_read(s,buffer,buffer_size);
		if(result<0) {
			fprintf(stderr,"parrot_cp: error reading '%s': %s\n",source,strerror(errno));
			nerrors++;
			break;
		} else if(result==0) {
			break;
		}

		actual = full_write(t,buffer,result);
		if(result!=actual) {
			fprintf(stderr,"parrot_cp: error writing '%s': %s\n",target,strerror(errno));
			nerrors++;
			break;
		}
	}

	close(s);
	close(t);
	free(buffer);

	if(nerrors) {
		return -1;
	} else {
		return 0;
	}
}

int copyfile( const char *source, const char *target )
{
	int result;

	if(update_mode || interactive_mode) {
		struct stat64 sinfo, tinfo;

		if(stat64(source,&sinfo)<0) {
			fprintf(stderr,"parrot_cp: couldn't stat %s: %s\n",source,strerror(errno));
			return -1;
		}

		if(stat64(target,&tinfo)<0) {
			/* no problem */
		} else {
			if(update_mode && sinfo.st_mtime<=tinfo.st_mtime ) {
				return 0;
			}
			if(interactive_mode) {
				char answer;
				printf("parrot_cp: overwrite '%s'? ",target);
				if(fscanf(stdin,"%c",&answer)==1) {
					if(answer!='y') return 0;
				} else {
					return 0;
				}
			}
		}
	}

	if(verbose_mode) {
		printf("'%s' -> '%s'\n",source,target);
	}

	if(force_mode) {
		chmod(target,0700);
		unlink(target);
	}

	if(symlink_mode) {
		return symlink(source,target);
	} else if(hardlink_mode) {
		return link(source,target);
	} else {
		result = parrot_cp(source,target);
		if(result<0 && errno==ENOSYS) {
			return copyfile_slow(source,target);
		} else {
			return result;
		}
	}
}

int copypath( const char *source, const char *target )
{
	char newsource[PFS_PATH_MAX];
	char newtarget[PFS_PATH_MAX];
	int nerrors=0;

	int result = copyfile(source,target);

	if(result>=0) {
		/* success: do nothing else */
	} else if(errno==EISDIR) {
		DIR *dir;
		struct dirent *d;

		if(!recursive_mode) {
			fprintf(stderr,"parrot_cp: omitting directory '%s'\n",source);
			return 1;
		}

		if(verbose_mode) {
			printf("'%s' -> '%s'\n",source,target);
		}

		if(mkdir(target,0777)<0) {
			if(errno==EEXIST) {
				/* keep going */
			} else {
				fprintf(stderr,"parrot_cp: cannot mkdir '%s': %s\n",target,strerror(errno));
				return 1;
			}
		}

		dir = opendir(source);
		if(!dir) {
			fprintf(stderr,"parrot_cp: cannot opendir '%s': %s\n",source,strerror(errno));
			return 1;
		}

		while((d=readdir(dir))) {
			if(!strcmp(d->d_name,".")) continue;
			if(!strcmp(d->d_name,"..")) continue;

			sprintf(newsource,"%s/%s",source,d->d_name);
			sprintf(newtarget,"%s/%s",target,d->d_name);

			nerrors += copypath(newsource,newtarget);
		}

		closedir(dir);
	} else {
		nerrors++;
		fprintf(stderr,"parrot_cp: cannot copy %s to %s: %s\n",source,target,strerror(errno));
	}

	return nerrors;
}

void show_help()
{
		fprintf(stdout, "Use: parrot_cp [OPTIONS]... SOURCES ... DEST\n");
	fprintf(stdout, "Where options are:\n");
	fprintf(stdout, " %-30s Forcibly remove target before copying.\n", "-f,--force");
	fprintf(stdout, " %-30s Interactive mode: ask before overwriting.\n", "-i,--interactive");
	fprintf(stdout, " %-30s Recursively copy directories.\n", "-r,-R,--recursive");
	fprintf(stdout, " %-30s Make symbolic links instead of copying files.\n", "-s,--symlinks");
	fprintf(stdout, " %-30s Make hard links instead of copying files.\n", "-l,--hardlinks");
	fprintf(stdout, " %-30s Update mode: Copy only if source is newer than target.\n", "-u,--update-only");
	fprintf(stdout, " %-30s Verbose mode: Show names of files copied.\n", "-v,--verbose");
	fprintf(stdout, " %-30s Help: Show these options.\n", "-h,--help");

}

int main( int argc, char *argv[] )
{

	char *target;
	signed char c;
	int i;
	int nerrors = 0;
	int target_is_dir = 0;
	struct stat statbuf;
	char newtarget[PFS_PATH_MAX];

	static const struct option long_options[] = {
		{"help",  no_argument, 0, 'h'},
		{"verbose", no_argument, 0, 'v'},
		{"force", no_argument, 0, 'f'},
		{"interactive", no_argument, 0, 'i'},
		{"recursive", no_argument, 0, 'r'},
		{"symlinks", no_argument, 0, 's'},
		{"hardlinks", no_argument, 0, 'l'},
		{"update-only", no_argument, 0, 'u'},
		{0,0,0,0}
	};

	while((c=getopt_long(argc,argv,"firRsluvh", long_options, NULL)) > -1) {
		switch(c) {
		case 'f':
			force_mode = 1;
			break;
		case 'i':
			interactive_mode = 1;
			break;
		case 'r':
		case 'R':
			recursive_mode = 1;
			break;
		case 's':
			symlink_mode = 1;
			break;
		case 'l':
			hardlink_mode = 1;
			break;
		case 'u':
			update_mode = 1;
			break;
		case 'v':
			verbose_mode = 1;
			break;
		case 'h':
			show_help();
			exit(0);
			break;
		}
	}

	// Check for not enough arguments
	if( (argc-optind)<2 ) {
		printf("Insufficient number of arguments\n");
		show_help();
		return 1;
	}

	// Set and check target
	target = argv[argc-1];

	if(stat(target, &statbuf)>=0) {
		if(S_ISDIR(statbuf.st_mode)) {
			target_is_dir=1;
		}
	}

	// Check that we are not moving multiple files into a non-directory
	if( (argc-optind)>2 && !target_is_dir) {
		printf("%s: copying multiple files, but last argument '%s' is not a directory\n", argv[0], target);
		return 1;
	}

	for(i=optind; i<=argc-2; i++) {
		if(target_is_dir) {
			char *basename = strrchr(argv[i],'/');
			if(basename) {
				sprintf(newtarget,"%s%s",target,basename);
			} else {
				sprintf(newtarget,"%s/%s",target,argv[i]);
			}
		} else {
			strcpy(newtarget,target);
		}
		nerrors += copypath(argv[i],newtarget);
	}

	return nerrors!=0;
}

/* vim: set noexpandtab tabstop=4: */
