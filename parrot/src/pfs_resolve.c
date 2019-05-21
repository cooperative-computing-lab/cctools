/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_resolve.h"

#include "pfs_types.h"
#include "pfs_mountfile.h"
#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "hash_table.h"

#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <sys/stat.h>
#include <fnmatch.h>
#include <fcntl.h>
#include <limits.h>

struct pfs_mount_entry *pfs_process_current_ns(void);

/*
Some things that could be cleaned up in this code:
- Use list.h instead of an embedded linked list.
- The resolver cache was used to cache the (expensive) lookups of the external resolver, which is basically unused.  Not clear if the cache actually helps for internal lookups, which simply traverse a (usually short) linked list.  Try removing the resolver cache and see how that impacts real applications.
*/

extern char pfs_temp_dir[PFS_PATH_MAX];

static struct pfs_mount_entry *mount_list = 0;
static struct hash_table *resolve_cache = 0;

static pfs_resolve_t pfs_resolve_ns( struct pfs_mount_entry *ns, const char *logical_name, char *physical_name, mode_t mode, time_t stoptime );

void pfs_resolve_init(void) {
	if (!mount_list) mount_list = (struct pfs_mount_entry *) xxmalloc(sizeof(*mount_list));
	memset(mount_list, 0, sizeof(*mount_list));
	mount_list->refcount = 1;
}

static void pfs_resolve_cache_flush()
{
	char *key, *value;

	if(!resolve_cache) return;

	hash_table_firstkey(resolve_cache);
	while(hash_table_nextkey(resolve_cache,&key,(void**)&value)) {
		hash_table_remove(resolve_cache,key);
		free(value);
	}
}

static struct pfs_mount_entry *find_parent_ns(struct pfs_mount_entry *ns) {
	while (ns && ns->next) {
		assert(!ns->parent);
		ns = ns->next;
	}
	while (ns && ns->parent) {
		assert(!ns->next);
		ns = ns->parent;
	}
	return ns;
}

void pfs_resolve_add_entry( const char *prefix, const char *redirect, mode_t mode )
{
	assert(prefix);
	assert(redirect);
	char real_redirect[PFS_PATH_MAX];
	struct pfs_mount_entry *ns = pfs_process_current_ns();
	if (!ns) ns = mount_list;
	assert(ns);

	debug(D_RESOLVE,"resolving %s in parent ns",redirect);
	switch (pfs_resolve_ns(find_parent_ns(ns), redirect, real_redirect, mode, 0)) {
	case PFS_RESOLVE_CHANGED:
	case PFS_RESOLVE_UNCHANGED:
		break;
	default:
		debug(D_NOTICE,"couldn't resolve redirect %s",prefix);
		return;
	}

	struct pfs_mount_entry *m = (struct pfs_mount_entry *) xxmalloc(sizeof(*m));
	memcpy(m, ns, sizeof(*m));
	memset(ns, 0, sizeof(*ns));
	strcpy(ns->prefix, prefix);
	strcpy(ns->redirect, real_redirect);
	ns->mode = mode;
	ns->next = m;
	ns->refcount = m->refcount;
	m->refcount = 1;
	pfs_resolve_cache_flush();
}

int pfs_resolve_remove_entry( const char *prefix )
{
	assert(prefix);
	struct pfs_mount_entry *ns = pfs_process_current_ns();
	if (!ns) ns = mount_list;
	assert(ns);
	assert(!(ns->next && ns->parent));

	while (ns) {
		if(!strcmp(ns->prefix,prefix)) {
			unsigned refcount = ns->refcount;
			struct pfs_mount_entry *e = NULL;
			if (ns->next) {
				e = ns->next;
			} else if (ns->parent) {
				e = ns->parent;
			} else {
				fatal("unable to remove mount entry");
			}

			assert(!(e->next && e->parent));
			memcpy(ns, e, sizeof(*ns));
			ns->refcount = refcount;
			pfs_resolve_share_ns(e->next);
			pfs_resolve_share_ns(e->parent);
			pfs_resolve_drop_ns(e);

			pfs_resolve_cache_flush();
			return 1;
		}
		ns = ns->next;
	}

	return 0;
}

int pfs_resolve_mount ( const char *path, const char *destination, const char *mode ) {
	int m = pfs_mountfile_parse_mode(mode);
	if (m < 0) return m;
	pfs_resolve_add_entry(path, destination, m);
	return 0;
}

static pfs_resolve_t pfs_resolve_external( const char *logical_name, const char *prefix, const char *redirect, char *physical_name )
{
	char cmd[PFS_PATH_MAX];
	FILE *file;
	int result;

	assert(logical_name);
	assert(prefix);
	assert(redirect);
	assert(physical_name);

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

	assert(logical_name);
	assert(prefix);
	assert(redirect);
	assert(physical_name);

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

	assert(path);

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
				string_nformat(temp,sizeof(temp),"/%s/%s",prefix,path+plen+2);
				debug(D_RESOLVE,"%s -> %s",path,temp);
				strcpy(path,temp);
			} else {
				break;
				/* leave it as is */
			}
		}
	}
}

pfs_resolve_t pfs_resolve( const char *logical_name, char *physical_name, mode_t mode, time_t stoptime )
{
	struct pfs_mount_entry *ns = pfs_process_current_ns();
	if (!ns) ns = mount_list;
	return pfs_resolve_ns(ns, logical_name, physical_name, mode, stoptime);
}

static pfs_resolve_t pfs_resolve_ns( struct pfs_mount_entry *ns, const char *logical_name, char *physical_name, mode_t mode, time_t stoptime )
{
	assert(ns);
	assert(physical_name);
	assert(physical_name);
	pfs_resolve_t result = PFS_RESOLVE_UNCHANGED;
	const char *t;
	char lookup_key[PFS_PATH_MAX + 3 * sizeof(int) + 1];

	sprintf(lookup_key, "%o|%p|%s", mode, ns, logical_name);

	if(!resolve_cache) resolve_cache = hash_table_create(0,0);

	t = (const char *) hash_table_lookup(resolve_cache,lookup_key);
	if(t) {
		strcpy(physical_name,t);
		result = PFS_RESOLVE_CHANGED;
	} else {
		while (ns) {
			assert(!(ns->next && ns->parent));
			assert(ns->refcount > 0);
			if (ns->parent) {
				ns = ns->parent;
				continue;
			}
			if (*ns->prefix == '\x00' || *ns->redirect == '\x00') {
				// we hit the end of the mountlist
				break;
			}
			result = mount_entry_check(logical_name,ns->prefix,ns->redirect,physical_name);
			if(result!=PFS_RESOLVE_UNCHANGED) {
				if ((mode & ns->mode) != mode) {
					result = PFS_RESOLVE_DENIED;
					debug(D_RESOLVE,"%s denied, requesting mode %o on mount entry with %o",logical_name,mode,ns->mode);
				}
				break;
			}
			ns = ns->next;
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
		debug(D_RESOLVE,"%s = %s,%o",logical_name,physical_name,mode);
		if(!hash_table_lookup(resolve_cache,lookup_key)) {
			hash_table_insert(resolve_cache,lookup_key,xxstrdup(physical_name));
		}
	}

	return result;
}

struct pfs_mount_entry *pfs_resolve_fork_ns(struct pfs_mount_entry *ns) {
	struct pfs_mount_entry *result = (struct pfs_mount_entry *) xxmalloc(sizeof(*result));
	memset(result, 0, sizeof(*result));
	result->refcount = 1;
	if (ns) {
		assert(!(ns->next && ns->parent));
		result->parent = pfs_resolve_share_ns(ns);
	} else {
		result->parent = pfs_resolve_share_ns(mount_list);
	}
	return result;
}

struct pfs_mount_entry *pfs_resolve_share_ns(struct pfs_mount_entry *ns) {
	if (!ns) return NULL;
	assert(ns->refcount > 0);
	assert(ns->refcount < UINT_MAX);
	assert(!(ns->next && ns->parent));
	++ns->refcount;
	return ns;
}

void pfs_resolve_drop_ns(struct pfs_mount_entry *ns) {
	if (!ns) return;

	assert(ns->refcount > 0);
	assert(!(ns->next && ns->parent));
	--ns->refcount;
	if (ns->refcount == 0) {
		pfs_resolve_drop_ns(ns->next);
		pfs_resolve_drop_ns(ns->parent);
		free(ns);
	}
}

void pfs_resolve_seal_ns(void) {
	struct pfs_mount_entry *ns = pfs_process_current_ns();
	if (!ns) ns = mount_list;
	assert(ns);

	struct pfs_mount_entry *m = (struct pfs_mount_entry *) xxmalloc(sizeof(*m));
	memcpy(m, ns, sizeof(*m));
	memset(ns, 0, sizeof(*ns));
	ns->parent = m;
	ns->refcount = m->refcount;
	m->refcount = 1;
}

/* vim: set noexpandtab tabstop=4: */
