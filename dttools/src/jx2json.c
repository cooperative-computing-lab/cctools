/*
Copyright (C) 2022 The University of Notre Dame
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
	printf(optfmt, "-a", "--args <FILE>", "Evaluate FILE and use it as the context");
	printf(optfmt, "-d", "--define <VAR>=<EXPR>", "Bind EXPR to the variable VAR.");
	printf(optfmt, "-c", "--context <FILE>", "Deprecated.");
	printf(optfmt, "-p", "--pretty", "Print more readable JSON");
	printf(optfmt, "-n", "--noeval", "Don't evaluate, just print parsed JX.");
	printf(optfmt, "-v", "--version", "Show version number");
	printf(optfmt, "-h", "--help", "Help: Show these options");
}

static const struct option long_options[] = {
	{"help", no_argument, 0, 'h'},
	{"version", no_argument, 0, 'v'},
	{"pretty", no_argument, 0, 'p'},
	{"noeval", no_argument, 0, 'n'},
	{"context", required_argument, 0, 'a'},
	{"define", required_argument, 0, 'd'},
	{"args", required_argument, 0, 'a'},
	{0, 0, 0, 0}};

int main(int argc, char *argv[]) {
	struct jx *ctx = jx_object(NULL);
	char *input = NULL;
	FILE *stream = stdin;
	struct jx *body = NULL;
	struct jx *tmp = NULL;
	char *s;
	void (*print_stream)(struct jx *, FILE *) = jx_print_stream;
	int do_eval = 1;

	jx_eval_enable_external(1);

	int c;
	while ((c = getopt_long(argc, argv, "vha:d:pn", long_options, NULL)) > -1) {
		switch (c) {
			case 'a': {
				FILE *f = fopen(optarg, "r");
				if (!f) {
					fprintf(stderr, "failed to open args file %s: %s\n",
						optarg, strerror(errno));
					return 1;
				}
				body = jx_parse_stream(f);
				fclose(f);
				if (!body) {
					fprintf(stderr, "invalid args file %s\n",
						optarg);
					return 1;
				}

				if(do_eval) {
					tmp = jx_eval(body, ctx);
					jx_delete(body);
					body = tmp;
				}

				if (jx_istype(body, JX_ERROR)) {
					printf("invalid args\n");
					print_stream(ctx, stdout);
					printf("\n");
					return 1;
				}
				tmp = jx_merge(ctx, body, NULL);
				jx_delete(body);
				jx_delete(ctx);
				ctx = tmp;
				break;
			}
			case 'd':
				s = strchr(optarg, '=');
				if (!s) {
					fprintf(stderr, "malformed variable\n");
					return 1;
				}
				*s = '\0';
				body = jx_parse_string(s + 1);
				if (!body) {
					fprintf(stderr, "malformed JX expression\n");
					return 1;
				}
				if(do_eval) {
					tmp = jx_eval(body, ctx);
					jx_delete(body);
					body = tmp;
				}

				if (jx_istype(body, JX_ERROR)) {
					printf("invalid expression\n");
					print_stream(ctx, stdout);
					printf("\n");
					return 1;
				}
				jx_insert(ctx, jx_string(optarg), body);
				break;
			case 'p':
				print_stream = jx_pretty_print_stream;
				break;
			case 'n':
				do_eval = 0;
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
	if (!body) {
		fprintf(stderr, "malformed JX\n");
		return 1;
	}

	if(do_eval) {
		tmp = jx_eval_with_defines(body, ctx);
		jx_delete(body);
		body = tmp;
	}

	print_stream(body, stdout);
	printf("\n");
	jx_delete(body);
	jx_delete(ctx);
	return 0;
}

/* vim: set noexpandtab tabstop=8: */
