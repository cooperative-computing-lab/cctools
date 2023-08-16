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
#include <sys/stat.h>

#include "chirp_client.h"
#include "chirp_reli.h"
#include "chirp_recursive.h"
#include "chirp_stream.h"

#include "cctools.h"
#include "debug.h"
#include "auth_all.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "full_io.h"
#include "getopt_aux.h"

#ifdef CCTOOLS_OPSYS_DARWIN
#define fopen64 fopen
#define open64 open
#define lseek64 lseek
#define stat64 stat
#define fstat64 fstat
#define lstat64 lstat
#define fseeko64 fseeko
#endif

static int timeout = 3600;
static size_t buffer_size = 65536;

static void show_help(const char *cmd)
{
	fprintf(stdout, "use: %s [options] <local-file> <hostname[:port]> <remote-file>\n", cmd);
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " %-30s Require this authentication mode.\n", "-a,--auth=<flag>");
	fprintf(stdout, " %-30s Set transfer buffer size. (default is %zu bytes)\n", "-b,--block-size=<size>", buffer_size);
	fprintf(stdout, " %-30s Enable debugging for this subsystem.\n", "-d,--debug <flag>");
	fprintf(stdout, " %-30s Follow input file like tail -f.\n", "-f,--follow");
	fprintf(stdout, " %-30s Comma-delimited list of tickets to use for authentication.\n", "-i,--tickets=<files>");
	fprintf(stdout, " %-30s Timeout for failure. (default is %ds)\n", "-t,--timeout=<time>", timeout);
	fprintf(stdout, " %-30s Show program version.\n", "-v,--version");
	fprintf(stdout, " %-30s This message.\n", "-h,--help");
}

int main(int argc, char *argv[])
{
	int did_explicit_auth = 0;
	int follow_mode = 0;
	int whole_file_mode = 1;
	const char *hostname, *source_file, *target_file;
	time_t stoptime;
	FILE *file;
	int c;
	char *tickets = NULL;

	debug_config(argv[0]);

	static const struct option long_options[] = {
		{"auth", required_argument, 0, 'a'},
		{"block-size", required_argument, 0, 'b'},
		{"debug", required_argument, 0, 'd'},
		{"follow", no_argument, 0, 'f'},
		{"tickets", required_argument, 0, 'i'},
		{"timeout", required_argument, 0, 't'},
		{"version", no_argument, 0, 'v'},
		{"help", no_argument, 0, 'h'},
		{0, 0, 0, 0}
	};

	while((c = getopt_long(argc, argv, "a:b:d:fi:t:vh", long_options, NULL)) > -1) {
		switch (c) {
		case 'a':
			if (!auth_register_byname(optarg))
				fatal("could not register authentication method `%s': %s", optarg, strerror(errno));
			did_explicit_auth = 1;
			break;
		case 'b':
			buffer_size = (size_t)strtoul(optarg, NULL, 0);
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'f':
			follow_mode = 1;
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

	source_file = argv[optind];
	hostname = argv[optind + 1];
	target_file = argv[optind + 2];
	stoptime = time(0) + timeout;

	if(!strcmp(source_file, "-")) {
		file = stdin;
		source_file = "/dev/stdin";
	} else {
		file = fopen(source_file, "r");
		if(!file) {
			fprintf(stderr, "chirp_put: couldn't open %s: %s\n", source_file, strerror(errno));
			return 1;
		}
	}

	if(follow_mode)
		whole_file_mode = 0;

	if(whole_file_mode) {
		INT64_T result = chirp_recursive_put(hostname, source_file, target_file, stoptime);
		if(result < 0) {
			fprintf(stderr, "chirp_put: couldn't put %s to host %s: %s\n", source_file, hostname, strerror(errno));
			return 1;
		} else {
			return 0;
		}
	} else {
		struct chirp_stream *stream;
		char *buffer = xxmalloc(buffer_size);
		INT64_T ractual, wactual;

		stream = chirp_stream_open(hostname, target_file, CHIRP_STREAM_WRITE, stoptime);
		if(!stream) {
			fprintf(stderr, "chirp_put: couldn't open %s for writing: %s\n", target_file, strerror(errno));
			return 1;
		}

		while(1) {
			ractual = full_fread(file, buffer, buffer_size);
			if(ractual == 0) {
				if(follow_mode) {
					debug(D_DEBUG, "waiting for more data...");
					sleep(1);
					continue;
				} else {
					break;
				}
			}
			wactual = chirp_stream_write(stream, buffer, (int)ractual, stoptime);
			if(wactual != ractual) {
				fprintf(stderr, "chirp_put: couldn't write to %s: %s\n", target_file, strerror(errno));
				return 1;
			}
		}
		chirp_stream_close(stream, stoptime);
		return 0;
	}
}

/* vim: set noexpandtab tabstop=8: */
