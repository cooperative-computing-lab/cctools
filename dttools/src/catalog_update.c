/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_print.h"
#include "catalog_query.h"
#include "datagram.h"
#include "debug.h"
#include "int_sizes.h"
#include "load_average.h"
#include "host_memory_info.h"
#include "macros.h"
#include "stringtools.h"
#include "username.h"
#include "uptime.h"
#include "getopt.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/utsname.h>

void show_help(const char *cmd) {
	fprintf(stdout, "Use: %s [options] <name=value> ...\n", cmd);
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " -c,--catalog=<catalog>\n");
}

int main(int argc, char *argv[]) {
	char *host = CATALOG_HOST;
	char *port = CATALOG_PORT;

	static const struct option long_options[] = {
		{"catalog", required_argument, 0, 'c'},
		{0,0,0,0}
	};

	signed int c;
	while ((c = getopt_long(argc, argv, "c:", long_options, NULL)) > -1) {
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
	d = datagram_create_address(NULL, NULL);
	if (!d) {
		fatal("could not create datagram port!");
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
	host_memory_info_get(&memory_avail, &memory_total);
	uptime = uptime_get();
	username_get(owner);

	struct jx *j = jx_object(0);

	jx_insert_string(j,"type","node");
	jx_insert(j,jx_string("version"),jx_format("%d.%d.%d",CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO));
	jx_insert_string(j,"cpu",name.machine);
	jx_insert_string(j,"opsys",name.sysname);
	jx_insert_string(j,"opsysversion",name.release);
	jx_insert_double(j,"load1",load[0]);
	jx_insert_double(j,"load5",load[1]);
	jx_insert_double(j,"load15",load[2]);
	jx_insert_integer(j,"memory_total",memory_total);
	jx_insert_integer(j,"memory_avail",memory_avail);
	jx_insert_integer(j,"cpus",cpus);
	jx_insert_integer(j,"uptime,",uptime);
	jx_insert_string(j,"owner",owner);

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
		jx_insert_string(j,name,value);
	}

	char *text = jx_print_string(j);

	datagram_send(d, text, strlen(text), host, port);

	jx_delete(j);

	datagram_delete(d);
	return EXIT_SUCCESS;
}

/* vim: set noexpandtab tabstop=4: */
