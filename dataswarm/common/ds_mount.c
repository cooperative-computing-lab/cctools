
#include "ds_mount.h"
#include "jx.h"
#include "debug.h"

#include <stdlib.h>
#include <string.h>

struct ds_mount * ds_mounts_create( struct jx *jmounts )
{
	struct jx_pair *p;
	struct ds_mount *head = 0;

	if (!jmounts) return NULL;

	for(p=jmounts->u.pairs;p;p=p->next) {
		const char *key = p->key->u.string_value;
		struct jx *value = p->value;
		struct ds_mount *m;
		m = ds_mount_create(key,value);
		m->next = head;
		head = m;
	}

	return head;
}

dataswarm_flags_t dataswarm_flags_parse( const char *s )
{
	if(!s) return 0;

	int flags = 0;

	while(*s) {
		switch(*s) {
			case 'r':
			case 'R':
				flags |= DS_FLAGS_READ;
				break;

			case 'w':
			case 'W':
				flags |= DS_FLAGS_WRITE;
				break;

			case 'a':
			case 'A':
				flags |= DS_FLAGS_APPEND;
				break;

			default:
				debug(D_NOTICE|D_DATASWARM,"igoring invalid mount flag: %c\n",*s);
				break;
		}
		s++;
	}

	return flags;
}

struct jx * dataswarm_flags_to_jx( dataswarm_flags_t flags )
{
	static char str[4];
	str[0] = 0;

	if(flags&DS_FLAGS_READ) strcat(str,"R");
	if(flags&DS_FLAGS_WRITE) strcat(str,"W");
	if(flags&DS_FLAGS_APPEND) strcat(str,"A");

	return jx_string(str);
}

struct ds_mount * ds_mount_create( const char *uuid, struct jx *jmount )
{
	struct ds_mount *m = malloc(sizeof(*m));
	memset(m,0,sizeof(*m));

	m->uuid = strdup(uuid);

	const char *type = jx_lookup_string(jmount,"type");
	if(!strcmp(type,"path")) {
		m->type = DS_MOUNT_PATH;
		m->path = jx_lookup_string_dup(jmount,"path");
		m->flags = dataswarm_flags_parse(jx_lookup_string(jmount,"flags"));
	} else if(!strcmp(type,"fd")) {
		m->type = DS_MOUNT_FD;
		m->fd = jx_lookup_integer(jmount,"fd");
		m->flags = dataswarm_flags_parse(jx_lookup_string(jmount,"flags"));
	} else if(!strcmp(type,"stdin")) {
		m->type = DS_MOUNT_FD;
		m->fd = 0;
		m->flags = DS_FLAGS_READ;
	} else if(!strcmp(type,"stdout")) {
		m->type = DS_MOUNT_FD;
		m->fd = 1;
		m->flags = DS_FLAGS_WRITE|DS_FLAGS_TRUNCATE;
	} else if(!strcmp(type,"stderr")) {
		m->type = DS_MOUNT_FD;
		m->fd = 2;
		m->flags = DS_FLAGS_WRITE|DS_FLAGS_TRUNCATE;
	} else {
		ds_mount_delete(m);
		return 0;
	}

	return m;
}

struct jx * ds_mounts_to_jx( struct ds_mount *m )
{
	struct jx *jmounts = jx_object(0);

	while(m) {
		struct jx *jm = ds_mount_to_jx(m);
		jx_insert(jmounts,jx_string(m->uuid),jm);
		m = m->next;
	}

	return jmounts;
}

struct jx * ds_mount_to_jx( struct ds_mount *m )
{
	struct jx *j = jx_object(0);
	if(m->type==DS_MOUNT_PATH) {
		jx_insert_string(j,"type","path");
		jx_insert_string(j,"path",m->path);
		jx_insert(j,jx_string("flags"),dataswarm_flags_to_jx(m->flags));
	} else if(m->type==DS_MOUNT_FD) {
		jx_insert_string(j,"type","fd");
		jx_insert_integer(j,"fd",m->fd);
		jx_insert(j,jx_string("flags"),dataswarm_flags_to_jx(m->flags));
	}

	return j;
}

void ds_mount_delete( struct ds_mount *m )
{
	if(!m) return;
	ds_mount_delete(m->next);
	free(m->path);
       	free(m);
}
