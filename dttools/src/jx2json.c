/*
Copyright (C) 2017- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "cctools.h"
#include "getopt.h"
#include "jx.h"
#include "jx_eval.h"
#include "jx_parse.h"
#include "jx_pretty_print.h"
#include "jx_print.h"

static void show_help() {
	const char *optfmt = "%2s %-20s %s\n";
	printf("usage: jx2json [OPTIONS] [INPUT]\n");
	printf("\n");
	printf("If INPUT is not specified, stdin is used.");
	printf("OPTIONS are:\n");
	printf(optfmt, "-c", "--context <FILE>", "Evaluate FILE and use it as the context");
	printf(optfmt, "-p", "--pretty", "Print more readable JSON");
	printf(optfmt, "-v", "--version", "Show version number");
	printf(optfmt, "-h", "--help", "Help: Show these options");
}

static const struct option long_options[] = {
	{"help", no_argument, 0, 'h'},
	{"version", no_argument, 0, 'v'},
	{"pretty", no_argument, 0, 'p'},
	{"context", required_argument, 0, 'c'},
	{0, 0, 0, 0}};

int main(int argc, char *argv[]) {
	char *context = NULL;
	char *input = NULL;
	FILE *stream = stdin;
	struct jx *ctx = NULL;
	struct jx *body = NULL;
	void (*print_stream)(struct jx *, FILE *) = jx_print_stream;

	int c;
	while ((c = getopt_long(argc, argv, "vhc:p", long_options, NULL)) > -1) {
		switch (c) {
			case 'c':
				context = strdup(optarg);
				break;
			case 'p':
				print_stream = jx_pretty_print_stream;
				break;
			case 'h':
				show_help();
				return 0;
			case 'v':
				cctools_version_print(stdout, "jx2json");
				return 0;
			default:
				show_help();
				return 1;
		}
	}

	if (argc - optind == 1) {
		input = strdup(argv[optind]);
	} else if (argc - optind > 1) {
		show_help();
		return 1;
	}

	if (context) {
		FILE *f = fopen(context, "r");
		if (!f) {
			fprintf(stderr, "failed to open context file %s: %s\n",
				context, strerror(errno));
			return 1;
		}
		ctx = jx_parse_stream(f);
		fclose(f);
		if (!ctx) return 1;
	}

	ctx = jx_eval(ctx, NULL);
	if (jx_istype(ctx, JX_ERROR)) {
		printf("invalid context\n");
		print_stream(ctx, stdout);
		printf("\n");
		return 1;
	}

	if (input) {
		stream = fopen(input, "r");
		if (!stream) {
			fprintf(stderr, "failed to open input file %s: %s\n",
				input, strerror(errno));
			return 1;
		}
	}

	body = jx_parse_stream(stream);
	fclose(stream);
	if (!body) return 1;

	body = jx_eval(body, ctx);
	print_stream(body, stdout);
	printf("\n");
	return 0;
}

/* vim: set noexpandtab tabstop=4: */
