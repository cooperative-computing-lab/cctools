/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#ifndef CATALOG_QUERY_H
#define CATALOG_QUERY_H

#include <time.h>
#include "jx.h"

/** @file catalog_query.h
Query the global catalog server for server descriptions.
*/

#define CATALOG_HOST_DEFAULT "catalog.cse.nd.edu,backup-catalog.cse.nd.edu"
#define CATALOG_PORT_DEFAULT 9097

#define CATALOG_HOST (getenv("CATALOG_HOST") ? getenv("CATALOG_HOST") : CATALOG_HOST_DEFAULT )
#define CATALOG_PORT (getenv("CATALOG_PORT") ? atoi(getenv("CATALOG_PORT")) : CATALOG_PORT_DEFAULT )

/** Catalog update control flags.
These control the behavior of @ref catalog_query_send_update
*/

typedef enum {
      CATALOG_UPDATE_BACKGROUND=1,  /**< Send update via a background process if TCP is selected. */
      CATALOG_UPDATE_CONDITIONAL=2, /**< Fail if UDP is selected and update is too large to send. */
} catalog_update_flags_t;

/** Create a catalog query.
Connects to a catalog server, issues a query, and waits for the results.
The caller may specify a specific catalog host and port.
If none is given, then the environment variables CATALOG_HOST and CATALOG_PORT will be consulted.
If neither is set, the system will contact chirp.cse.nd.edu on port 9097.
@param hosts A comma delimited list of catalog servers to query, or null for the default server.
@param filter_expr An optional expression to filter the results in JX syntax.
 A null pointer indicates no filter.
@param stoptime The absolute time at which to abort.
@return A catalog query object on success, or null on failure.
*/
struct catalog_query *catalog_query_create(const char *hosts, struct jx *filter_expr, time_t stoptime);

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

/** Send update text to the given hosts
hosts is a comma delimited list of hosts, each of which can be host or host:port
@param hosts A list of hosts to which to send updates
@param text String to send
@param flags Any combination of CATALOG_UPDATE
@return The number of updates successfully sent, 
*/
int catalog_query_send_update(const char *hosts, const char *text, catalog_update_flags_t flags );

#endif
