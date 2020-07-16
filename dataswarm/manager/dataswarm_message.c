
#include "dataswarm_message.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "jx_print.h"
#include "jx_parse.h"

int dataswarm_message_send( struct link *l, const char *str, int length, time_t stoptime )
{
	char lenstr[16];
	sprintf(lenstr,"%d\n",length);
	int lenstrlen = strlen(lenstr);
	int result = link_write(l,lenstr,lenstrlen,stoptime);
	if(result!=lenstrlen) return 0;
	result = link_write(l,str,length,stoptime);
	return result==length;
}

char * dataswarm_message_recv( struct link *l, time_t stoptime )
{
	char lenstr[16];
	int result = link_readline(l,lenstr,sizeof(lenstr),stoptime);
	if(!result) return 0;

	int length = atoi(lenstr);
	char *str = malloc(length);
	result = link_read(l,str,length,stoptime);
	if(result!=length) {
		free(str);
		return 0;
	}
	return str;
}

int dataswarm_json_send( struct link *l, struct jx *j, time_t stoptime )
{
	char *str = jx_print_string(j);
	int result = dataswarm_message_send(l,str,strlen(str),stoptime);
	free(str);
	return result;
}

struct jx * dataswarm_json_recv( struct link *l, time_t stoptime )
{
	char *str = dataswarm_message_recv(l,stoptime);
	if(!str) return 0;
	struct jx *j = jx_parse_string(str);
	free(str);
	return j;
}

