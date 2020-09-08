#ifndef DATASWARM_CLIENT_REP_H
#define DATASWARM_CLIENT_REP_H

/*
dataswarm_client_rep is the data structure representation
of the actual client process that runs somewhere else.
*/

struct dataswarm_client_rep {
	struct link *link;
};

struct dataswarm_client_rep * dataswarm_client_rep_create( struct link *l );

#endif
