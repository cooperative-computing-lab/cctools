/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "http_query.h"

#include "buffer.h"
#include "stringtools.h"
#include "debug.h"
#include "domain_name_cache.h"
#include "url_encode.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

#define HTTP_LINE_MAX 4096
#define HTTP_PORT 80

static int http_response_to_errno(int response)
{
	if(response <= 299) {
		return 0;
	} else if(response <= 399) {
		return EBUSY;
	} else if(response == 400) {
		return EINVAL;
	} else if(response <= 403) {
		return EACCES;
	} else if(response == 404) {
		return ENOENT;
	} else if(response <= 406) {
		return EINVAL;
	} else if(response == 407) {
		return EACCES;
	} else if(response == 408) {
		return ETIMEDOUT;
	} else if(response <= 410) {
		return ENOENT;
	} else if(errno <= 499) {
		return EINVAL;
	} else {
		return EIO;
	}
}

struct link *http_query_no_cache(const char *url, const char *action, time_t stoptime)
{
	INT64_T size;
	return http_query_size(url, action, &size, stoptime, 1);
}

struct link *http_query(const char *url, const char *action, time_t stoptime)
{
	INT64_T size;
	return http_query_size(url, action, &size, stoptime, 0);
}

struct link *http_query_size(const char *url, const char *action, INT64_T * size, time_t stoptime, int cache_reload)
{
	if(!getenv("HTTP_PROXY")) {
		return http_query_size_via_proxy(0, url, action, size, stoptime, cache_reload);
	} else {
		char proxies[HTTP_LINE_MAX];
		char *proxy;

		strcpy(proxies, getenv("HTTP_PROXY"));
		proxy = strtok(proxies, ";");

		while(proxy) {
			struct link *result;
			result = http_query_size_via_proxy(proxy, url, action, size, stoptime, cache_reload);
			if(result)
				return result;
			proxy = strtok(0, ";");
		}
		return 0;
	}
}

struct link *http_query_size_via_proxy(const char *proxy, const char *urlin, const char *action, INT64_T * size, time_t stoptime, int cache_reload)
{
	char url[HTTP_LINE_MAX];
	char newurl[HTTP_LINE_MAX];
	char line[HTTP_LINE_MAX];
	char addr[LINK_ADDRESS_MAX];
	struct link *link;
	int save_errno;
	int response;
	char actual_host[HTTP_LINE_MAX];
	int actual_port;
	*size = 0;

	url_encode(urlin, url, sizeof(url));

	if(proxy && !strcmp(proxy, "DIRECT"))
		proxy = 0;

	if(proxy) {
		int fields = sscanf(proxy, "http://%[^:]:%d", actual_host, &actual_port);
		if(fields == 2) {
			/* host and port are good */
		} else if(fields == 1) {
			actual_port = HTTP_PORT;
		} else {
			debug(D_HTTP, "invalid proxy syntax: %s", proxy);
			return 0;
		}
	} else {
		int fields = sscanf(url, "http://%[^:]:%d", actual_host, &actual_port);
		size_t delta;
		if(fields != 2) {
			fields = sscanf(url, "http://%[^/]", actual_host);
			if(fields == 1) {
				actual_port = HTTP_PORT;
			} else {
				debug(D_HTTP, "malformed url: %s", url);
				return 0;
			}
		}

		/* When there is no proxy to be used, the Request-URI field should be abs_path. */
		delta = strlen("http://") + strlen(actual_host);
		if(fields == 2) {
			size_t s_port = snprintf(NULL, 0, "%d", actual_port);
			delta = delta + 1 + s_port; /* 1 is for the colon between host and port. */
		}
		memmove(url, url + delta, strlen(url) - delta + 1); /* 1: copy the terminating null character */
	}

	debug(D_HTTP, "connect %s port %d", actual_host, actual_port);
	if(!domain_name_cache_lookup(actual_host, addr))
		return 0;

	link = link_connect(addr, actual_port, stoptime);
	if(!link) {
		errno = ECONNRESET;
		return 0;
	}


	{
		buffer_t B;

		buffer_init(&B);
		buffer_abortonfailure(&B, 1);

		buffer_printf(&B, "%s %s HTTP/1.1\r\n", action, url);
		if(cache_reload)
			buffer_putliteral(&B, "Cache-Control: max-age=0\r\n");
		buffer_putliteral(&B, "Connection: close\r\n");
		buffer_printf(&B, "Host: %s\r\n", actual_host);
		if(getenv("HTTP_USER_AGENT"))
			buffer_printf(&B, "User-Agent: Mozilla/5.0 (compatible; CCTools %s Parrot; http://ccl.cse.nd.edu/ %s)\r\n", CCTOOLS_VERSION, getenv("HTTP_USER_AGENT"));
		else
			buffer_printf(&B, "User-Agent: Mozilla/5.0 (compatible; CCTools %s Parrot; http://ccl.cse.nd.edu/)\r\n", CCTOOLS_VERSION);
		buffer_putliteral(&B, "\r\n"); /* header terminator */

		debug(D_HTTP, "%s", buffer_tostring(&B));
		link_putstring(link, buffer_tostring(&B), stoptime);

		buffer_free(&B);
	}

	if(link_readline(link, line, HTTP_LINE_MAX, stoptime)) {
		string_chomp(line);
		debug(D_HTTP, "%s", line);
		if(sscanf(line, "HTTP/%*d.%*d %d", &response) == 1) {
			newurl[0] = 0;
			while(link_readline(link, line, HTTP_LINE_MAX, stoptime)) {
				string_chomp(line);
				debug(D_HTTP, "%s", line);
				sscanf(line, "Location: %s", newurl);
				sscanf(line, "Content-Length: %" SCNd64, size);
				if(strlen(line) <= 2) {
					break;
				}
			}

			switch (response) {
			case 200:
				return link;
				break;
			case 301:
			case 302:
			case 303:
			case 307:
				link_close(link);
				if(newurl[0]) {
					if(!strcmp(url, newurl)) {
						debug(D_HTTP, "error: server gave %d redirect from %s back to the same url!", response, url);
						errno = EIO;
						return 0;
					} else {
						return http_query_size_via_proxy(proxy,newurl,action,size,stoptime,cache_reload);
					}
				} else {
					errno = ENOENT;
					return 0;
				}
				break;
			default:
				link_close(link);
				errno = http_response_to_errno(response);
				return 0;
				break;
			}
		} else {
			debug(D_HTTP, "malformed response");
			save_errno = ECONNRESET;
		}
	} else {
		debug(D_HTTP, "malformed response");
		save_errno = ECONNRESET;
	}

	link_close(link);
	errno = save_errno;
	return 0;
}

INT64_T http_fetch_to_file(const char *url, const char *filename, time_t stoptime)
{
	FILE *file;
	INT64_T size;
	INT64_T actual;
	struct link *link;

	file = fopen(filename, "w");
	if(file) {
		link = http_query_size(url, "GET", &size, stoptime, 1);
		if(link) {
			actual = link_stream_to_file(link, file, size, stoptime);
			link_close(link);
			fclose(file);
			if(actual == size) {
				return actual;
			} else {
				unlink(filename);
				return -1;
			}
		} else {
			fclose(file);
			return -1;
		}
	} else {
		return -1;
	}

}

/* vim: set noexpandtab tabstop=8: */
