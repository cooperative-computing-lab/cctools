/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "auth.h"
#include "debug.h"
#include "stringtools.h"
#include "domain_name_cache.h"
#include "xmalloc.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <ctype.h>

struct auth_ops {
	char *type;
	auth_assert_t assert;
	auth_accept_t accept;
	struct auth_ops *next;
};

static struct auth_ops *list=0;

static struct auth_ops * type_lookup( char *type )
{
	struct auth_ops *a;

	for(a=list;a;a=a->next) {
		if(!strcmp(a->type,type)) return a;
	}

	return 0;
}

/*
Regardless of what the individual authentication modules do,
we need to have sanitized subject names that don't have spaces,
newlines, and other crazy stuff like that.
*/

static void auth_sanitize( char *s )
{
	while(*s) {
		if( isspace((int)(*s)) || !isprint((int)(*s)) ) {
			*s = '_';
		}
		s++;
	}

}

int auth_assert( struct link *link, char **type, char **subject, time_t stoptime )
{
	char line[AUTH_LINE_MAX];
	struct auth_ops *a;

	for( a=list; a; a=a->next ) {

		debug(D_AUTH,"requesting '%s' authentication",a->type);

		link_putfstring(link,"%s\n",stoptime,a->type);

		if(!link_readline(link,line,AUTH_LINE_MAX,stoptime)) break;
		if(!strcmp(line,"yes")) {
			debug(D_AUTH,"server agrees to try '%s'",a->type);
			if(a->assert(link,stoptime)) {
				debug(D_AUTH,"successfully authenticated");
				if(!link_readline(link,line,AUTH_LINE_MAX,stoptime)) break;
				if(!strcmp(line,"yes")) {
					debug(D_AUTH,"reading back auth info from server");
					if(!link_readline(link,line,sizeof(line),stoptime)) return 0;
					*type = xstrdup(line);
					if(!link_readline(link,line,sizeof(line),stoptime)) return 0;
					*subject = xstrdup(line);
					auth_sanitize(*subject);
					debug(D_AUTH,"server thinks I am %s:%s",*type,*subject);
					return 1;
				} else {
					debug(D_AUTH,"but not authorized to continue");
				}
			} else {
				debug(D_AUTH,"failed to authenticate");
			}
		} else {
			debug(D_AUTH,"server refuses to try '%s'",a->type);
		}
		debug(D_AUTH,"still trying...");
	}

	if(!a) {
		debug(D_AUTH,"ran out of authenticators");
	} else {
		debug(D_AUTH,"lost connection");
	}

	return 0;
}

int auth_accept( struct link *link, char **typeout, char **subject, time_t stoptime )
{
	struct auth_ops *a;
	char type[AUTH_TYPE_MAX];
	char addr[LINK_ADDRESS_MAX];
	int port;

	*subject = 0;

	link_address_remote(link,addr,&port);

	while(link_readline(link,type,AUTH_TYPE_MAX,stoptime)) {

		string_chomp(type);

		debug(D_AUTH,"%s:%d requests '%s' authentication",addr,port,type);

		a = type_lookup( type );
		if(a) {
			debug(D_AUTH,"I agree to try '%s' ",type);
			link_putliteral(link,"yes\n",stoptime);
		} else {
			debug(D_AUTH,"I do not agree to '%s' ",type);
			link_putliteral(link,"no\n",stoptime);
			continue;
		}

		if(a->accept(link,subject,stoptime)) {
			auth_sanitize(*subject);
			debug(D_AUTH,"'%s' authentication succeeded",type);
			debug(D_AUTH,"%s:%d is %s:%s\n",addr,port,type,*subject);
			link_putfstring(link,"yes\n%s\n%s\n",stoptime,type,*subject);
			*typeout = xstrdup(type);
			return 1;
		} else {
			debug(D_AUTH,"%s:%d could not authenticate using '%s'",addr,port,type);
		}
		debug(D_AUTH,"still trying");
	}

	debug(D_AUTH,"%s:%d disconnected",addr,port);

	return 0;
}


int auth_barrier( struct link *link, const char *response, time_t stoptime )
{
	char line[AUTH_LINE_MAX];

	link_putstring(link,response,stoptime);

	if(link_readline(link,line,sizeof(line),stoptime)) {
		if(!strcmp(line,"yes")) {
			return 1;
		}
	}

	return 0;
}

int auth_register( char *type, auth_assert_t assert, auth_accept_t accept )
{
	struct auth_ops *a, *l;

	a = (struct auth_ops *) malloc(sizeof(struct auth_ops));
	if(!a) return 0;


	a->type = type;
	a->assert = assert;
	a->accept = accept; 
	a->next = 0;

	if(!list) {
		list = a;
	} else {
		l = list;
		while(l->next) {
			l = l->next;
		}
		l->next = a;
	}

	return 1;
}

void auth_clear()
{
	struct auth_ops *n;

	while(list) {
		n = list->next;
		free(list);
		list = n;
	}
}
