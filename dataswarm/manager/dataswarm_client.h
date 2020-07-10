#ifndef DATASWARM_CLIENT_H
#define DATASWARM_CLIENT_H

struct dataswarm_client {
	struct link *link;
};

struct dataswarm_client * dataswarm_client_create( struct link *l );

#endif
