/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include "chirp_client.h"
#include "chirp_reli.h"
#include "chirp_stream.h"

#include "debug.h"
#include "cctools.h"
#include "full_io.h"
#include "auth_all.h"
#include "stringtools.h"
#include "getopt_aux.h"

#define MODE_SPLIT 0
#define MODE_COPY  1
#define MODE_JOIN  2

static int timeout = 3600;
static int buffer_size = 1048576;
static int stream_mode = MODE_SPLIT;

static void show_help(const char *cmd)
{
	fprintf(stdout, "use: %s [options] [copy|split|join] <local-file> { <hostname[:port]> <remote-file> }\n", cmd);
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " %-30s Require this authentication mode.\n", "-a,--auth=<flag>");
	fprintf(stdout, " %-30s Set transfer buffer size. (default is %d bytes)\n", "-b,--block-size=<size>", buffer_size);
	fprintf(stdout, " %-30s Enable debugging for this subsystem.\n", "-d,--debug=<flag>");
	fprintf(stdout, " %-30s Comma-delimited list of tickets to use for authentication.\n", "-i,--tickets=<files>");
	fprintf(stdout, " %-30s Timeout for failure. (default is %ds)\n", "-t,--timeout=<time>", timeout);
	fprintf(stdout, " %-30s Show program version.\n", "-v,--version");
	fprintf(stdout, " %-30s This message.\n", "-h,--help");
	fprintf(stdout, "\nThis tool is used to move data between a single local file and multiple remote files.\n");
	fprintf(stdout, "'chirp_stream_files copy'  duplicates a single file to multiple hsots.\n");
	fprintf(stdout, "'chirp_stream_files split' sends the lines of a file to multiple hosts, round robin.\n");
	fprintf(stdout, "'chirp_stream_files join'  performs the opposite of split, joining multiple files to one.\n");
	fprintf(stdout, "A local file of '-' will use stdin for splitting or copying and stdout for joining.\n");
}

int main(int argc, char *argv[])
{
	int did_explicit_auth = 0;
	time_t stoptime;
	signed char c;
	int i, srcindex, nstreams;
	FILE *localfile;
	struct chirp_stream *stream[argc - 2];
	const char *localmode;
	int remotemode;
	char *tickets = NULL;

	debug_config(argv[0]);

	static const struct option long_options[] = {
		{"auth", required_argument, 0, 'a'},
		{"block-size", required_argument, 0, 'b'},
		{"debug", required_argument, 0, 'd'},
		{"tickets", required_argument, 0, 'i'},
		{"timeout", required_argument, 0, 't'},
		{"version", no_argument, 0, 'v'},
		{"help", no_argument, 0, 'h'},
		{0, 0, 0, 0}
	};

	while((c = getopt_long(argc, argv, "a:b:d:i:t:vh", long_options, NULL)) > -1) {
		switch (c) {
		case 'a':
			if (!auth_register_byname(optarg))
				fatal("could not register authentication method `%s': %s", optarg, strerror(errno));
			did_explicit_auth = 1;
			break;
		case 'b':
			buffer_size = atoi(optarg);
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

	if((argc - optind) < 4) {
		show_help(argv[0]);
		return 1;
	}

	if(!strcmp(argv[optind], "split")) {
		stream_mode = MODE_SPLIT;
		localmode = "r";
		remotemode = CHIRP_STREAM_WRITE;
	} else if(!strcmp(argv[optind], "copy")) {
		stream_mode = MODE_COPY;
		localmode = "r";
		remotemode = CHIRP_STREAM_WRITE;
	} else if(!strcmp(argv[optind], "join")) {
		stream_mode = MODE_JOIN;
		localmode = "w";
		remotemode = CHIRP_STREAM_READ;
	} else {
		fprintf(stderr, "unknown operation: %s\n", argv[0]);
		show_help(argv[0]);
		return 1;
	}

	char *buffer = malloc(buffer_size);

	srcindex = optind + 1;
	nstreams = (argc - optind - 2) / 2;
	stoptime = time(0) + timeout;

	localfile = fopen(argv[srcindex], localmode);
	if(!localfile) {
		fprintf(stderr, "couldn't open %s: %s\n", argv[srcindex], strerror(errno));
		return 1;
	}

	char **hostname = malloc(sizeof(*hostname) * nstreams);
	char **filename = malloc(sizeof(*filename) * nstreams);

	for(i = 0; i < nstreams; i++) {
		hostname[i] = argv[srcindex + (2 * i) + 1];
		filename[i] = argv[srcindex + (2 * i) + 2];

		stream[i] = chirp_stream_open(hostname[i], filename[i], remotemode, stoptime);
		if(!stream[i]) {
			fprintf(stderr, "couldn't open %s:%s: %s\n", hostname[i], filename[i], strerror(errno));
			return 1;
		}
	}


	if(stream_mode == MODE_SPLIT) {
		i = 0;
		while(fgets(buffer, buffer_size, localfile)) {
			int length = strlen(buffer);
			int actual = chirp_stream_write(stream[i], buffer, length, stoptime);
			if(actual != length) {
				fprintf(stderr, "couldn't write to %s:%s: %s\n", hostname[i], filename[i], strerror(errno));
				return 1;
			}
			i = (i + 1) % nstreams;
		}
	} else if(stream_mode == MODE_COPY) {

		while(fgets(buffer, buffer_size, localfile)) {
			int length = strlen(buffer);
			for(i = 0; i < nstreams; i++) {
				int actual = chirp_stream_write(stream[i], buffer, length, stoptime);
				if(actual != length) {
					fprintf(stderr, "couldn't write to %s:%s: %s\n", hostname[i], filename[i], strerror(errno));
					return 1;
				}
			}
		}

	} else {
		int streams_left = nstreams;
		while(streams_left > 0) {
			for(i = 0; i < nstreams; i++) {
				if(!stream[i])
					continue;
				int length = chirp_stream_readline(stream[i], buffer, buffer_size, stoptime);
				if(length > 0) {
					length = strlen(buffer);
					fprintf(localfile, "%s\n", buffer);
				} else {
					streams_left--;
				}
			}
		}
	}

	for(i = 0; i < nstreams; i++) {
		chirp_stream_flush(stream[i], stoptime);
		chirp_stream_close(stream[i], stoptime);
	}

	return 0;
}

/* vim: set noexpandtab tabstop=8: */
