#ifndef DATASWARM_CLIENT_REP_H
#define DATASWARM_CLIENT_REP_H

/*
ds_client_rep is the data structure representation
of the actual client process that runs somewhere else.
*/

struct ds_client_rep {
	struct link *link;
};

struct ds_client_rep * ds_client_rep_create( struct link *l );

#endif
