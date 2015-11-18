/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#ifndef CATALOG_QUERY_H
#define CATALOG_QUERY_H

#include "jx.h"
#include "macros.h"

#include <time.h>

/** @file catalog_query.h
Query the global catalog server for server descriptions.
*/

#define CATALOG_HOST_DEFAULT "catalog.cse.nd.edu"
#define CATALOG_PORT_DEFAULT 9097

#define CATALOG_HOST (getenv("CATALOG_HOST") ? getenv("CATALOG_HOST") : CATALOG_HOST_DEFAULT )
#define CATALOG_PORT (getenv("CATALOG_PORT") ? getenv("CATALOG_PORT") : xstr(CATALOG_PORT_DEFAULT) )

/** Create a catalog query.
Connects to a catalog server, issues a query, and waits for the results.
The caller may specify a specific catalog host and port.
If none is given, then the environment variables CATALOG_HOST and CATALOG_PORT will be consulted.
If neither is set, the system will contact chirp.cse.nd.edu on port 9097.
@param host The catalog server to query, or null for the default server.
@param port The port of the server, or 0 for the default port.
@param stoptime The absolute time at which to abort.
@return A catalog query object on success, or null on failure.
*/
struct catalog_query *catalog_query_create(const char *host, int port, time_t stoptime);

/** Read the next object from a query.
Returns the next @ref jx expressions from the issued query.
The caller may use @ref jx_lookup_string, @ref jx_lookup_integer and related
functions to manipulate the object, and then must call @ref jx_delete
when done.
@param q A query created by @ref catalog_query_create.
@param stoptime The absolute time at which to abort.
@return A @ref jx expression representing the next result, or null if the end of stream has been reached.
*/
struct jx *catalog_query_read(struct catalog_query *q, time_t stoptime);

/** Delete a completed query object.
@param q The query to delete.
*/
void catalog_query_delete(struct catalog_query *q);

#endif
