/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_CATALOG_H
#define DS_CATALOG_H

#include "list.h"

int ds_catalog_parse( char *server_string, char **host, int *port );
struct list * ds_catalog_query( const char *catalog_host, int catalog_port, const char *project_regex );
struct list * ds_catalog_query_cached( const char *catalog_host, int catalog_port, const char *project_regex );

#endif
