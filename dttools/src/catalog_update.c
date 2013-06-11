/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "buffer.h"
#include "catalog_server.h"
#include "datagram.h"
#include "debug.h"
#include "domain_name_cache.h"
#include "int_sizes.h"
#include "load_average.h"
#include "memory_info.h"
#include "stringtools.h"
#include "username.h"
#include "uptime.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/utsname.h>

#define DEFAULT_TYPE	"node"

void show_help(const char *cmd) {
	fprintf(stdout, "Use: %s [options] <name=value> ...\n", cmd);
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " -c <catalog>\n");
}

int main(int argc, char *argv[]) {
	char *host = CATALOG_HOST;
	int   port = CATALOG_PORT;

	signed int c;
	while ((c = getopt(argc, argv, "c:")) > -1) {
		switch (c) {
			case 'c':
				host = optarg;
				break;
			default:
				show_help(argv[0]);
				return EXIT_FAILURE;
		}
	}

	struct datagram *d;
	d = datagram_create(DATAGRAM_PORT_ANY);
	if (!d) {
		fatal("could not create datagram port!");
	}

	buffer_t *b;
	const char *text;
	size_t text_size;
	b = buffer_create();
	if (!b) {
		fatal("could not create buffer!");
	}

	struct utsname name;
	int cpus;
	int uptime;	
	double load[3];
	UINT64_T memory_total, memory_avail;
	char owner[USERNAME_MAX];

	uname(&name);
	string_tolower(name.sysname);
	string_tolower(name.machine);
	string_tolower(name.release);
	load_average_get(load);
	cpus = load_average_get_cpus();
	memory_info_get(&memory_avail, &memory_total);
	uptime = uptime_get();
	username_get(owner);

	buffer_printf(b, "type %s\nversion %d.%d.%d\ncpu %s\nopsys %s\nopsysversion %s\nload1 %0.02lf\nload5 %0.02lf\nload15 %0.02lf\nmemory_total %llu\nmemory_avail %llu\ncpus %d\nuptime %d\nowner %s\n",
		DEFAULT_TYPE,
		CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO,
		name.machine,
		name.sysname,
		name.release,
		load[0],
		load[1],
		load[2],
		memory_avail,
		memory_total,
		cpus,
		uptime,
		owner
	);

	int i;
	for (i = optind; i < argc; i++) {
		char *name;
		char *value;

		name  = argv[i];
		value = strchr(name, '=');
		if (!value) {
			fatal("invalid name/value pair = %s", name);
		} else {
			*value++ = 0;
		}

		buffer_printf(b, "%s %s\n", name, value);
	}

        text = buffer_tostring(b, &text_size);

	char address[DATAGRAM_ADDRESS_MAX];
	if (domain_name_cache_lookup(host, address)) {
		datagram_send(d, text, text_size, address, port);
	} else {
		fatal("unable to lookup address of host: %s", host);
	}

	buffer_delete(b);
	datagram_delete(d);
	return EXIT_SUCCESS;
}

/* vim: set sts=8 sw=8 ts=8 ft=c: */
