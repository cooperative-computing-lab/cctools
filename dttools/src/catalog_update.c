/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_print.h"
#include "jx_parse.h"

#include "catalog_query.h"
#include "debug.h"
#include "int_sizes.h"
#include "load_average.h"
#include "host_memory_info.h"
#include "stringtools.h"
#include "username.h"
#include "uptime.h"
#include "getopt.h"
#include "cctools.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/utsname.h>

void show_help(const char *cmd) {
	printf( "Use: %s [options]\n", cmd);
	printf( "where options are:\n");
	printf( " -c,--catalog=<catalog>\n");
	printf( " -f,--file=<json-file>\n");
	printf( " -d,--debug=<flags>\n");
	printf( " -o,--debug-file=<file>\n");
	printf( " -v,--version\n");
	printf( " -h,--help\n");
}

int main(int argc, char *argv[]) {
	char *host = CATALOG_HOST;
	const char *input_file = 0;

	static const struct option long_options[] = {
		{"catalog", required_argument, 0, 'c'},
		{"file", required_argument, 0, 'f' },
		{"debug", required_argument, 0, 'd' },
		{"debug-file", required_argument, 0, 'o' },
		{"version", no_argument, 0, 'h' },
		{"help", no_argument, 0, 'h' },
		{0,0,0,0}
	};

	signed int c;
	while ((c = getopt_long(argc, argv, "c:f:d:o:vh", long_options, NULL)) > -1) {
		switch (c) {
			case 'c':
				host = optarg;
				break;
			case 'f':
				input_file = optarg;
				break;
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'o':
				debug_config_file(optarg);	
				break;
			case 'v':
				cctools_version_print(stdout,"catalog_update");
				break;
			case 'h':
			default:
				show_help(argv[0]);
				return EXIT_FAILURE;
		}
	}

	struct jx *custom;

	if(input_file) {
		custom = jx_parse_file(input_file);
		if(!custom) {
			fprintf(stderr,"catalog_update: %s does not contain a valid json record!\n",input_file);
			return 1;
		}
	} else {
		custom = jx_object(0);
	}

	struct jx *j = jx_object(0);

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

	struct jx *merged = jx_merge(j,custom,0);

	char *text = jx_print_string(merged);

	if(catalog_query_send_update(host, text, 0) < 1) {
		fprintf(stderr, "Unable to send update");
	}

	jx_delete(j);

	return EXIT_SUCCESS;
}

/* vim: set noexpandtab tabstop=8: */
