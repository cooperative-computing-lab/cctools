/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "auth.h"
#include "link.h"
#include "getopt.h"
#include "debug.h"
#include "domain_name_cache.h"
#include "auth_all.h"
#include "stringtools.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

static void show_help(const char *cmd)
{
	fprintf(stdout, "Use: %s [options]\n", cmd);
	fprintf(stdout, "Where options are:\n");
	fprintf(stdout, " %-30s This message\n", "-h,--help=<flag>");
	fprintf(stdout, " %-30s Debugging\n", "-d,--debug=<flag>");
	fprintf(stdout, " %-30s Send debugging to this file. (can also be :stderr, or :stdout)\n", "-o,--debug-file=<file>");
	fprintf(stdout, " %-30s Rotate debug files of this size (default 10M, 0 disables)\n", "-O,--debug-rotate-max=<bytes>");
	fprintf(stdout, " %-30s Allow this auth type\n", "-a,--auth=<type>");
	fprintf(stdout, " %-30s Port number\n", "-p,--port=<num>");
	fprintf(stdout, " %-30s Remote host\n", "-r,--host=<host>");
	fprintf(stdout, "Where debug flags arg: ");

	debug_flags_print(stderr);

	fprintf(stdout, "\n");
}


int main(int argc, char *argv[])
{
	struct link *link, *manager;
	char *subject = 0, *type = 0;
	time_t stoptime;
	char line[1024];
	signed char c;
	int portnum = 30000;
	char *hostname = 0;
	int timeout = 30;

	debug_config(argv[0]);

	static const struct option long_options[] = {
		{"auth", required_argument, 0, 'a'},
		{"port", required_argument, 0, 'p'},
		{"host", required_argument, 0, 'r'},
		{"help", required_argument, 0, 'h'},
		{"debug-file", required_argument, 0, 'o'},
		{"debug-rotate-max", required_argument, 0, 'O'},
		{"debug", required_argument, 0, 'd'},
		{0,0,0,0}
	};

	while((c = getopt_long(argc, argv, "a:p:r:d:o:O:", long_options, NULL)) > -1) {
		switch (c) {
		case 'p':
			portnum = atoi(optarg);
			break;
		case 'h':
			show_help(argv[0]);
			exit(0);
			break;
		case 'r':
			hostname = optarg;
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'O':
			debug_config_file_size(string_metric_parse(optarg));
			break;
		case 'a':
			if(!auth_register_byname(optarg))
				fatal("couldn't register %s authentication", optarg);
			break;
		default:
			show_help(argv[0]);
			exit(1);
			break;
		}
	}

	if(hostname) {
		char addr[LINK_ADDRESS_MAX];

		stoptime = time(0) + timeout;

		if(!domain_name_cache_lookup(hostname, addr))
			fatal("unknown host name: %s", hostname);

		link = link_connect(addr, portnum, stoptime);
		if(!link)
			fatal("couldn't connect to %s:%d: %s", hostname, portnum, strerror(errno));

		if(auth_assert(link, &type, &subject, stoptime)) {
			printf("server thinks I am %s %s\n", type, subject);
			if(link_readline(link, line, sizeof(line), stoptime)) {
				printf("got message: %s\n", line);
			} else {
				printf("lost connection!\n");
			}
		} else {
			printf("unable to authenticate.\n");
		}

		link_close(link);

	} else {
		stoptime = time(0) + timeout;

		manager = link_serve(portnum);
		if(!manager)
			fatal("couldn't serve port %d: %s\n", portnum, strerror(errno));

		while(time(0) < stoptime) {
			link = link_accept(manager, stoptime);
			if(!link)
				continue;

			if(auth_accept(link, &type, &subject, stoptime)) {
				time_t t = time(0);
				link_printf(link, stoptime, "Hello %s:%s, it is now %s", type, subject, ctime(&t));	/* ctime ends with newline */
			} else {
				printf("couldn't auth accept\n");
			}
			link_close(link);
		}
	}

	return 0;
}

/* vim: set noexpandtab tabstop=8: */
