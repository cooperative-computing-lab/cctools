/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <time.h>

#include "chirp_client.h"
#include "chirp_reli.h"
#include "chirp_recursive.h"

#include "cctools.h"
#include "debug.h"
#include "auth_all.h"
#include "getopt_aux.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "full_io.h"

static int timeout = 3600;

static void show_help(const char *cmd)
{
	fprintf(stdout, "use: %s [options] <hostname[:port]> <remote-file> <local-file>\n", cmd);
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " %-30s Require this authentication mode.\n", "-a,--auth=<flag>");
	fprintf(stdout, " %-30s Enable debugging for this subsystem.\n", "-d,--debug <flag>");
	fprintf(stdout, " %-30s Comma-delimited list of tickets to use for authentication.\n", "-i,--tickets=<files>");
	fprintf(stdout, " %-30s Timeout for failure. (default is %ds)\n", "-t,--timeout=<time>", timeout);
	fprintf(stdout, " %-30s Show program version.\n", "-v,--version");
	fprintf(stdout, " %-30s This message.\n", "-h,--help");
}

int main(int argc, char *argv[])
{
	int did_explicit_auth = 0;
	int stdout_mode = 0;
	const char *hostname, *source_file, *target_file;
	time_t stoptime;
	FILE *file;
	INT64_T result;
	signed char c;
	char *tickets = NULL;

	debug_config(argv[0]);

	static const struct option long_options[] = {
		{"auth", required_argument, 0, 'a'},
		{"debug", required_argument, 0, 'd'},
		{"tickets", required_argument, 0, 'i'},
		{"timeout", required_argument, 0, 't'},
		{"version", no_argument, 0, 'v'},
		{"help", no_argument, 0, 'h'},
		{0, 0, 0, 0}
	};

	while((c = getopt_long(argc, argv, "a:d:i:t:vh", long_options, NULL)) > -1) {
		switch (c) {
		case 'a':
			if (!auth_register_byname(optarg))
				fatal("could not register authentication method `%s': %s", optarg, strerror(errno));
			did_explicit_auth = 1;
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'i':
			tickets = strdup(optarg);
			break;
		case 't':
			timeout = string_time_parse(optarg);
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			exit(0);
			break;
		case 'h':
			show_help(argv[0]);
			exit(0);
			break;

		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if(!did_explicit_auth)
		auth_register_all();
	if(tickets) {
		auth_ticket_load(tickets);
		free(tickets);
	} else if(getenv(CHIRP_CLIENT_TICKETS)) {
		auth_ticket_load(getenv(CHIRP_CLIENT_TICKETS));
	} else {
		auth_ticket_load(NULL);
	}

	if((argc - optind) < 3) {
		show_help(argv[0]);
		exit(0);
	}

	hostname = argv[optind];
	source_file = argv[optind + 1];
	target_file = argv[optind + 2];
	stoptime = time(0) + timeout;

	if(!strcmp(target_file, "-")) {
		stdout_mode = 1;
		file = stdout;
	}

	if(stdout_mode) {
		result = chirp_reli_getfile(hostname, source_file, file, stoptime);
	} else {
		result = chirp_recursive_get(hostname, source_file, target_file, stoptime);
	}

	if(result < 0) {
		fprintf(stderr, "couldn't get %s:%s: %s\n", hostname, source_file, strerror(errno));
		return 1;
	} else {
		return 0;
	}
}

/* vim: set noexpandtab tabstop=8: */
