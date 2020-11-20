
#include "ds_db.h"
#include "ds_task.h"
#include "ds_file.h"

#include "debug.h"
#include "stringtools.h"
#include "hash_table.h"
#include "create_dir.h"

#include <dirent.h>
#include <string.h>
#include <errno.h>

void ds_db_commit_task( struct ds_manager *m, const char *taskid )
{
	struct ds_task *t = hash_table_lookup(m->task_table,taskid);
	if(!t) return;

	char *filename = string_format("%s/tasks/%s",m->dbpath,taskid);
	char *tempname = string_format("%s.tmp",filename);

	if(ds_task_to_file(t,tempname) && rename(tempname,filename)==0) {
		// success
	} else {
		fatal("couldn't write task to %s: %s",filename,strerror(errno));
	}
	
	free(tempname);
	free(filename);
}

void ds_db_commit_file( struct ds_manager *m, const char *fileid )
{
	struct ds_file *f = hash_table_lookup(m->file_table,fileid);
	if(!f) return;

	char *filename = string_format("%s/files/%s",m->dbpath,fileid);
	char *tempname = string_format("%s.tmp",filename);

	if(ds_file_to_file(f,tempname) && rename(tempname,filename)==0) {
		// success
	} else {
		fatal("couldn't write file to %s: %s",filename,strerror(errno));
	}
	
	free(tempname);
	free(filename);
}

void ds_db_recover_files( struct ds_manager *m, const char *path )
{
	DIR *dir;
	struct dirent *d;
	int count = 0;

	dir = opendir(path);
	if(!dir) fatal("couldn't opendir %s: %s",path,strerror(errno));

	while((d=readdir(dir))) {
		if(!strcmp(d->d_name,".")) continue;
		if(!strcmp(d->d_name,"..")) continue;
		if(!strcmp(string_back(d->d_name,4),".tmp")) continue;

		char *filename = string_format("%s/files/%s",path,d->d_name);

		struct ds_file *f = ds_file_create_from_file(filename);
		if(!f) fatal("could not parse file: %s",filename);
		hash_table_insert(m->file_table,d->d_name,f);

		free(filename);

		count++;
	}
	
	closedir(dir);

	printf("recovered %d files from %s\n",count,path);
}

void ds_db_recover_tasks( struct ds_manager *m, const char *path)
{
	DIR *dir;
	struct dirent *d;
	int count = 0;

	dir = opendir(path);
	if(!dir) fatal("couldn't opendir %s: %s",path,strerror(errno));

	while((d=readdir(dir))) {
		if(!strcmp(d->d_name,".")) continue;
		if(!strcmp(d->d_name,"..")) continue;
		if(!strcmp(string_back(d->d_name,4),".tmp")) continue;

		char *filename = string_format("%s/tasks/%s",path,d->d_name);

		struct ds_task *t = ds_task_create_from_file(filename);
		if(!t) fatal("could not parse file: %s",filename);
		hash_table_insert(m->task_table,d->d_name,t);

		free(filename);
		count++;
	}
	
	closedir(dir);

	printf("recovered %d tasks from %s\n",count,path);
}

void ds_db_recover_all( struct ds_manager *m )
{
	create_dir_parents(m->dbpath,0777);

	char *taskpath = string_format("%s/tasks",m->dbpath);
	char *filepath = string_format("%s/files",m->dbpath);

	create_dir(taskpath,0777);
	ds_db_recover_tasks(m,taskpath);

	create_dir(filepath,0777);
	ds_db_recover_files(m,filepath);

	free(taskpath);
	free(filepath);
}



