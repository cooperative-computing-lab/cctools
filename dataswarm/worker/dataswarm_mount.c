
#include "dataswarm_mount.h"
#include "jx.h"
#include "debug.h"

#include <stdlib.h>
#include <string.h>

struct dataswarm_mount * dataswarm_mounts_create( struct jx *jmounts )
{
/*
Need to find a clean way of iterating over an object and producing a linked list.
*/
  return 0;
}

dataswarm_flags_t dataswarm_flags_parse( const char *s )
{
	if(!s) return 0;

	int flags = 0;

	while(*s) {
		switch(*s) {
			case 'r':
			case 'R':
				flags |= DATASWARM_FLAGS_READ;
				break;

			case 'w':
			case 'W':
				flags |= DATASWARM_FLAGS_WRITE;
				break;

			case 'a':
			case 'A':
				flags |= DATASWARM_FLAGS_APPEND;
				break;

			default:
				debug(D_NOTICE|D_DATASWARM,"igoring invalid mount flag: %c\n",*s);
				break;
		}
	}

	return flags;
}

struct jx * dataswarm_flags_to_jx( dataswarm_flags_t flags )
{
	static char str[4];
	str[0] = 0;
	
	if(flags&DATASWARM_FLAGS_READ) strcat(str,"R");
	if(flags&DATASWARM_FLAGS_WRITE) strcat(str,"W");
	if(flags&DATASWARM_FLAGS_APPEND) strcat(str,"A");

	return jx_string(str);	
}

struct dataswarm_mount * dataswarm_mount_create( const char *uuid, struct jx *jmount )
{
	struct dataswarm_mount *m = malloc(sizeof(*m));
	memset(m,0,sizeof(*m));

	m->uuid = strdup(uuid);

	const char *type = jx_lookup_string(jmount,"type");
	if(!strcmp(type,"path")) {
		m->type = DATASWARM_MOUNT_PATH;
		m->path = jx_lookup_string(jmount,"path");
		if(m->path) m->path = strdup(m->path);
		m->flags = dataswarm_flags_parse(jx_lookup_string(jmount,"flags"));
	} else if(!strcmp(type,"fd")) {
		m->type = DATASWARM_MOUNT_FD;
		m->fd = jx_lookup_integer(jmount,"fd");
		m->flags = dataswarm_flags_parse(jx_lookup_string(jmount,"flags"));
	} else if(!strcmp(type,"stdin")) {
		m->type = DATASWARM_MOUNT_FD;
		m->fd = 0;
		m->flags = DATASWARM_FLAGS_READ;
	} else if(!strcmp(type,"stdout")) {
		m->type = DATASWARM_MOUNT_FD;
		m->fd = 1;
		m->flags = DATASWARM_FLAGS_WRITE|DATASWARM_FLAGS_TRUNCATE;
	} else if(!strcmp(type,"stderr")) {
		m->type = DATASWARM_MOUNT_FD;
		m->fd = 2;
		m->flags = DATASWARM_FLAGS_WRITE|DATASWARM_FLAGS_TRUNCATE;
	} else {
		dataswarm_mount_delete(m);
		return 0;
	}

	return m;
}

struct jx * dataswarm_mounts_to_jx( struct dataswarm_mount *m )
{
	struct jx *jmounts = jx_object(0);

	while(m) {
		struct jx *jm = dataswarm_mount_to_jx(m);
		jx_insert(jmounts,jx_string(m->uuid),jm);
		m = m->next;
	}

	return jmounts;
}

struct jx * dataswarm_mount_to_jx( struct dataswarm_mount *m )
{
	struct jx *j = jx_object(0);
	if(m->type==DATASWARM_MOUNT_PATH) {
		jx_insert_string(j,"type","path");	
		jx_insert_string(j,"path",m->path);
		jx_insert(j,jx_string("flags"),dataswarm_flags_to_jx(m->flags));
	} else if(m->type==DATASWARM_MOUNT_FD) {
		jx_insert_string(j,"type","fd");	
		jx_insert_integer(j,"fd",m->fd);
		jx_insert(j,jx_string("flags"),dataswarm_flags_to_jx(m->flags));
	}

	return j;
}

void dataswarm_mount_delete( struct dataswarm_mount *m )
{
	if(!m) return;
	dataswarm_mount_delete(m->next);
       	free(m);
}


