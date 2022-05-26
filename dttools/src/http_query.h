/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef HTTP_QUERY_H
#define HTTP_QUERY_H

#include "link.h"
#include "int_sizes.h"

struct link *http_query(const char *url, const char *action, time_t stoptime);
struct link *http_query_no_cache(const char *url, const char *action, time_t stoptime);
struct link *http_query_size(const char *url, const char *action, INT64_T * size, time_t stoptime, int cache_reload);
struct link *http_query_size_via_proxy(const char *proxy, const char *url, const char *action, INT64_T * size, time_t stoptime, int cache_reload);

INT64_T http_fetch_to_file(const char *url, const char *filename, time_t stoptime);

#endif
