/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_resolve.h"
#include "pfs_types.h"
#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "hash_table.h"

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <sys/stat.h>
#include <fnmatch.h>

extern char pfs_temp_dir[PFS_PATH_MAX];

struct mount_entry {
	char prefix[PFS_PATH_MAX];
	char redirect[PFS_PATH_MAX];
	struct mount_entry *next;
};

static struct mount_entry * mount_list = 0;
static struct hash_table *resolve_cache = 0;

static void add_mount_entry( const char *prefix, const char *redirect )
{
	struct mount_entry * m = xxmalloc(sizeof(*m));
	strcpy(m->prefix,prefix);
	strcpy(m->redirect,redirect);
	m->next = mount_list;
	mount_list = m;
}

void pfs_resolve_manual_config( const char *str )
{
	char *e;
	str = xxstrdup(str);
	e = strchr(str,'=');
	if(!e) fatal("badly formed mount string: %s",str);
	*e = 0;
	e++;
	add_mount_entry(str,e);
}

void pfs_resolve_file_config( const char *filename )
{
	FILE *file;
	char line[PFS_LINE_MAX];
	char prefix[PFS_LINE_MAX];
	char redirect[PFS_LINE_MAX];
	int fields;
	int linenum=0;

	file = fopen(filename,"r");
	if(!file) fatal("couldn't open mountfile %s: %s\n",filename,strerror(errno));

	while(1) {
		if(!fgets(line,sizeof(line),file)) {
			if(errno==EINTR) {
				continue;
			} else {
				break;
			}
		}
		linenum++;
		if(line[0]=='#') continue;
		string_chomp(line);
		if(!line[0]) continue;
		fields = sscanf(line,"%s %s",prefix,redirect);

		if(fields==0) {
			continue;
		} else if(fields<2) {
			fatal("%s has an error on line %d\n",filename,linenum);
		} else {
			add_mount_entry(prefix,redirect);
		}
	}

	fclose(file);
}

int pfs_resolve_external( const char *logical_name, const char *prefix, const char *redirect, char *physical_name )
{
	char cmd[PFS_PATH_MAX];
	FILE *file;
	int result;

	sprintf(cmd,"%s %s",redirect,&logical_name[strlen(prefix)]);

	debug(D_RESOLVE,"external resolver: %s\n",cmd);

	file = popen(cmd,"r");
	if(!file) {
		debug(D_RESOLVE,"couldn't execute resolver %s: %s",cmd,strerror(errno));
		return PFS_RESOLVE_FAILED;
	}

	if(fgets(physical_name,PFS_PATH_MAX,file)) {
		string_chomp(physical_name);
	} else {
		physical_name[0] = 0;
	}

	result = pclose(file);

	if(result==0) {
		return PFS_RESOLVE_CHANGED;
	} else {
		return PFS_RESOLVE_FAILED;
	}
}

/*
Compare a logical name to a mountlist entry and
determine what to do with it.
*/

static pfs_resolve_t mount_entry_check( const char *logical_name, const char *prefix, const char *redirect, char *physical_name )
{
	pfs_resolve_t result;
	const char *prefix_sep, *local_prefix, *remote_prefix;
	int local_prefix_len;
	struct stat64 statbuf;

	int plen = strlen(prefix);
	int llen = strlen(logical_name);

	if(
		/* match patterns to logical name */
		!fnmatch(prefix,logical_name,0)
		||
		/* or match prefix exactly to logical name */
		(
			!strncmp(prefix,logical_name,plen) &&
			(
				prefix[plen-1]=='/' ||
				logical_name[plen]=='/' ||
				plen==llen
			)
		)
	) {
		if(!strcmp(redirect,"DENY")) {
			result = PFS_RESOLVE_DENIED;
		} else if(!strcmp(redirect,"ENOENT")) {
			result = PFS_RESOLVE_ENOENT;
		} else if(!strcmp(redirect,"LOCAL")) {
			strcpy(physical_name,logical_name);
			result = PFS_RESOLVE_CHANGED;
		} else if(!strncmp(redirect,"resolver:",9)) {
			result = pfs_resolve_external(logical_name,prefix,&redirect[9],physical_name);
		} else if(!strncmp(redirect,"lcache:",7) &&
			  (prefix_sep = strchr(redirect, '|'))) {
			/* redirect entry is in the format lcache:/local/path|/remote/path */
			local_prefix = &redirect[7];
			local_prefix_len = (int)(prefix_sep-local_prefix);
			/* anything in the local_prefix tree and the PFS cache is local */
			if ((!strncmp(logical_name, local_prefix, local_prefix_len)) || 
                            (!strncmp(logical_name, pfs_temp_dir, strlen(pfs_temp_dir))) )
			{
				strcpy(physical_name,logical_name);
				result = PFS_RESOLVE_CHANGED;
			} else {
				int retstat;
				strncpy(physical_name, local_prefix, local_prefix_len);
				physical_name[local_prefix_len] = '\000';
				if(llen>plen) {
					strcat(physical_name,"/");
					strcat(physical_name,&logical_name[plen]);
				}
				retstat = stat64(physical_name, &statbuf);
				/* All directories and all missing files are to be handled remotely */
				if (retstat < 0 || (retstat >= 0 && S_ISDIR(statbuf.st_mode))) {
					remote_prefix = prefix_sep+1;
					strcpy(physical_name,remote_prefix);
					if(llen>plen) {
						strcat(physical_name,"/");
						strcat(physical_name,&logical_name[plen]);
						
					}
				}
				result = PFS_RESOLVE_CHANGED;
			}
		} else {
			strcpy(physical_name,redirect);
			if(llen>plen) {
				if(logical_name[plen]!='/') {
					strcat(physical_name,"/");
				}
				strcat(physical_name,&logical_name[plen]);
			}
			result = PFS_RESOLVE_CHANGED;
		}
	} else {
		result = PFS_RESOLVE_UNCHANGED;
	}

	return result;
}

/*
Some services, such as the Condor chirp proxy,
will give us unusual url-looking paths like buffer:remote:/biz/foo.
Clean these up into a form that we can use.
*/

void clean_up_path( char *path )
{
	char temp[PFS_PATH_MAX];
	char prefix[PFS_PATH_MAX];
	char rest[PFS_PATH_MAX];

	while(1) {
		if(!strncmp(path,"buffer:",7)) {
			strcpy(temp,path+7);
			strcpy(path,temp);
		} else {
			if(sscanf(path,"%[^:/]:%s",prefix,rest)==2) {
				int plen = strlen(prefix);
				if(!strcmp(prefix,"remote")) {
					strcpy(prefix,"chirp/CONDOR");
				}
				sprintf(temp,"/%s/%s",prefix,path+plen+2);
				debug(D_RESOLVE,"%s -> %s",path,temp);
				strcpy(path,temp);
			} else {
				break;
				/* leave it as is */
			}
		}
	}
}

pfs_resolve_t pfs_resolve( const char *logical_name, char *physical_name, time_t stoptime )
{
	pfs_resolve_t result = PFS_RESOLVE_UNCHANGED;
	struct mount_entry *e;
	const char *t;

	if(!resolve_cache) resolve_cache = hash_table_create(0,0);

	t = hash_table_lookup(resolve_cache,logical_name);
	if(t) {
		strcpy(physical_name,t);
		result = PFS_RESOLVE_CHANGED;
	} else {
		for(e=mount_list;e;e=e->next) {
			result = mount_entry_check(logical_name,e->prefix,e->redirect,physical_name);
			if(result!=PFS_RESOLVE_UNCHANGED) break;
		}
	}

	switch(result) {
		case PFS_RESOLVE_UNCHANGED:
			strcpy(physical_name,logical_name);
			break;
		case PFS_RESOLVE_CHANGED:
			clean_up_path(physical_name);
			break;
		case PFS_RESOLVE_FAILED:
			debug(D_RESOLVE,"%s failed",logical_name);
			break;
		case PFS_RESOLVE_ENOENT:	
			debug(D_RESOLVE,"%s ENOENT",logical_name);
			break;
		case PFS_RESOLVE_DENIED:
			debug(D_RESOLVE,"%s denied",logical_name);
			break;
	}

	if(result==PFS_RESOLVE_UNCHANGED || result==PFS_RESOLVE_CHANGED) {
		debug(D_RESOLVE,"%s = %s",logical_name,physical_name);
		if(!hash_table_lookup(resolve_cache,logical_name)) {
			hash_table_insert(resolve_cache,logical_name,xxstrdup(physical_name));
		}
	}

	return result;
}


