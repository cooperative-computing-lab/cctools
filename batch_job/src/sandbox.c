
#include "sandbox.h"
#include "debug.h"
#include "stringtools.h"
#include "create_dir.h"
#include "delete_dir.h"

#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

struct sandbox * sandbox_create( const char *parent_dir, const char *input_files, const char *output_files )
{
	char *sandbox_path = string_format("%s/.sandbox.XXXXXX",parent_dir);

	if(!mkdtemp(sandbox_path)) {
		debug(D_BATCH|D_NOTICE,"couldn't create sandbox %s: %s",sandbox_path,strerror(errno));
		return 0;
	}

	debug(D_BATCH,"creating sandbox %s",sandbox_path);

	int failed = 0;

	char *files = strdup(input_files);
	char *file = strtok(files,",");
	while(file && !failed) {

		/*
		When remote renaming is used, the file may be of the
		form x=y, where x is the name outside the sandbox, 
		and y is the name inside the sandbox.
		*/

		char *inside_file = strchr(file,'=');
		if(inside_file) {
			*inside_file = 0;
			inside_file++;
		} else {
			inside_file = file;
		}

		char *inside_path = string_format("%s/%s",sandbox_path,inside_file);
		char *outside_path = string_format("../%s",file);

		debug(D_BATCH,"symlink %s -> %s",inside_path,outside_path);

		int result = symlink(outside_path,inside_path);
		if(result<0) {
			debug(D_BATCH|D_NOTICE,"couldn't symlink %s to %s: %s",inside_path,outside_path,strerror(errno));
			failed = 1;
			break;
		}
		
		free(inside_path);
		free(outside_path);

		file = strtok(0,",");
	}

	free(files);

	if(failed) {
		delete_dir(sandbox_path);
		return 0;
	} else {
		struct sandbox *s = malloc(sizeof(*s));
		s->sandbox_path = strdup(sandbox_path);
		s->output_files = strdup(output_files);
		return s;
	}
}

void sandbox_delete( struct sandbox *s )
{
	if(!s) return;
	debug(D_BATCH,"deleting sandbox %s",s->sandbox_path);
	delete_dir(s->sandbox_path);
	free(s->sandbox_path);
	free(s->output_files);
	free(s);
}

void sandbox_cleanup( struct sandbox *s )
{
	if(!s) return;

	debug(D_BATCH,"cleaning sandbox %s",s->sandbox_path);

	char *files = strdup(s->output_files);

	char *file = strtok(files,",");
	while(file) {

		/*
		When remote renaming is used, the file may be of the
		form x=y, where x is the name outside the sandbox, 
		and y is the name inside the sandbox.
		*/

		char *inside_file = strchr(file,'=');
		if(inside_file) {
			*inside_file = 0;
			inside_file++;
		} else {
			inside_file = file;
		}

		char *inside_path = string_format("%s/%s",s->sandbox_path,inside_file);
		char *outside_path = string_format("%s",file);

		debug(D_BATCH,"rename %s -> %s",inside_path,outside_path);

		int result = rename(inside_path,outside_path);
		if(result<0) {
			debug(D_BATCH|D_NOTICE,"couldn't move %s to %s: %s",inside_path,outside_path,strerror(errno));
			// fall through on failure, let next layer detect it
		}
		
		free(inside_path);
		free(outside_path);

		file = strtok(0,",");
	}

	free(files);

	sandbox_delete(s);
}

