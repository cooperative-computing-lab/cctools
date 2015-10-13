/* Copyright (C) 2015- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
*/

#include "chirp_catalog.h"

#include "chirp_alloc.h"
#include "chirp_filesystem.h"
#include "chirp_server.h"
#include "chirp_stats.h"

#include "catalog_query.h"
#include "debug.h"
#include "host_disk_info.h"
#include "host_memory_info.h"
#include "jx.h"
#include "jx_print.h"
#include "list.h"
#include "load_average.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <sys/utsname.h>

#include <errno.h>
#include <time.h>

struct catalog {
	int fd;
	char nodename[HOST_NAME_MAX];
};

struct list *catalogs;

int chirp_catalog_add (const char *nodename)
{
	int rc;
	struct addrinfo *addr, *addri;
	struct addrinfo hints;

	if (!catalogs)
		catalogs = list_create();

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_DGRAM;
	hints.ai_flags = AI_CANONNAME;
	rc = getaddrinfo(nodename, CATALOG_PORT, &hints, &addr);
	if (rc)
		fatal("could not getaddrinfo: %s", gai_strerror(rc));
	for (addri = addr; addri; addri = addri->ai_next) {
		struct catalog *catalog;
		int fd = socket(addri->ai_family, addri->ai_socktype, addri->ai_protocol);
		if (fd == -1) {
			debug(D_DEBUG, "skipping, could not create socket: %s", strerror(errno));
			continue;
		}

		if (connect(fd, addri->ai_addr, addri->ai_addrlen) == -1) {
			debug(D_DEBUG, "skipping, could not connect socket: %s", strerror(errno));
			close(fd);
			continue;
		}

		catalog = xxmalloc(sizeof(struct catalog));
		catalog->fd = fd;
		snprintf(catalog->nodename, sizeof(catalog->nodename), "%s", nodename);
		list_push_tail(catalogs, catalog);
		break;
	}
	freeaddrinfo(addr);
	return 0;
}

static int update_one_catalog(void *c, const void *text)
{
	struct catalog *catalog = c;
	debug(D_DEBUG, "sending update to %s:%s", catalog->nodename, CATALOG_PORT);
	if (send(catalog->fd, text, strlen(text), 0) == -1)
		debug(D_DEBUG, "send to catalog failed: %s", strerror(errno));
	return 1;
}

int chirp_catalog_update (void)
{
	struct chirp_statfs info;
	struct utsname name;
	int cpus;
	double avg[3];
	UINT64_T memory_total, memory_avail;

	if(!catalogs)
		chirp_catalog_add(CATALOG_HOST); /* set default */

	if(chirp_alloc_statfs("/", &info) < 0) {
		memset(&info, 0, sizeof(info));
	}

	uname(&name);
	string_tolower(name.sysname);
	string_tolower(name.machine);
	string_tolower(name.release);
	load_average_get(avg);
	cpus = load_average_get_cpus();

	host_memory_info_get(&memory_avail, &memory_total);

	struct jx *j = jx_object(0);

	jx_insert_string (j,"type","chirp");
	jx_insert_integer(j,"avail",info.f_bavail * info.f_bsize);
	jx_insert_string (j,"backend",chirp_url);
	jx_insert_string (j,"cpu",name.machine);
	jx_insert_integer(j,"cpus", cpus);
	jx_insert_double (j,"load1",avg[0]);
	jx_insert_double (j,"load5",avg[1]);
	jx_insert_double (j,"load15",avg[2]);
	jx_insert_integer(j,"memory_avail",memory_avail);
	jx_insert_integer(j,"memory_total",memory_total);
	jx_insert_integer(j,"minfree",chirp_minimum_space_free);
	jx_insert_string (j,"name",chirp_hostname);
	jx_insert_string (j,"opsys",name.sysname);
	jx_insert_string (j,"opsysversion",name.release);
	jx_insert_string (j,"owner",chirp_owner);
	jx_insert_string (j,"port",chirp_port);
	jx_insert_integer(j,"starttime",chirp_starttime);
	jx_insert_integer(j,"total",info.f_blocks * info.f_bsize);

	if (chirp_project_name[0]) {
		jx_insert_string(j,"project",chirp_project_name);
	}

	jx_insert(j,
		jx_string("url"),
		jx_format("chirp://%s:%d", chirp_hostname, chirp_port));

	jx_insert(j,
		jx_string("version"),
		jx_format("%d.%d.%d", CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO));

	chirp_stats_summary(j);
	char *message = jx_print_string(j);
	list_iterate(catalogs, update_one_catalog, message);
	free(message);
	jx_delete(j);

	return 0;
}

const char *chirp_catalog_primary (void)
{
	if(!catalogs)
		chirp_catalog_add(CATALOG_HOST); /* set default */
	struct catalog *primary = list_peek_head(catalogs);
	return primary ? primary->nodename : NULL;
}

/* vim: set noexpandtab tabstop=4: */
