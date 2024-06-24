/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "confuga.h"

#include "catch.h"
#include "cctools.h"
#include "debug.h"
#include "getopt.h"
#include "random.h"

#include <errno.h>
#include <limits.h>
#include <string.h>

static int process (confuga *C, int argc, char *argv[])
{
	int rc;
	int c;

	optind = 0;
	if (strcmp(argv[0], "sn-add") == 0) {
		static const struct option long_options[] = {
			{"help", no_argument, 0, 'h'},
			{"password", required_argument, 0, 'p'},
			{"root", required_argument, 0, 'r'},
			{0, 0, 0, 0}
		};
		static const char usage[] = "sn-add [-p password-file] [-r root] <\"uuid\"|\"address\"> <uuid|address>";
		char password[PATH_MAX] = "";
		char root[PATH_MAX] = "";
		int flag = 0;

		while((c = getopt_long(argc, argv, "+hp:r:", long_options, NULL)) > -1) {
			switch (c) {
				case 'h':
					CATCHUNIX(fprintf(stdout, "%s\n", usage));
					rc = 0;
					goto out;
				case 'p':
					CATCHUNIX(snprintf(password, sizeof password, "%s", optarg));
					break;
				case 'r':
					CATCHUNIX(snprintf(root, sizeof root, "%s", optarg));
					break;
				default:
					CATCHUNIX(fprintf(stderr, "invalid command: %s\n", usage));
					CATCH(EINVAL);
					break;
			}
		}
		if (optind != (argc-2) ) {
			CATCHUNIX(fprintf(stderr, "invalid command: %s\n", usage));
			CATCH(EINVAL);
		}
		if (strcmp(argv[optind], "uuid") == 0)
			flag |= CONFUGA_SN_UUID;
		else if (strcmp(argv[optind], "address") == 0)
			flag |= CONFUGA_SN_ADDR;
		else CATCH(EINVAL);
		CATCH(confuga_snadd(C, argv[optind+1], root[0] ? root : NULL, password[0] ? password : NULL, flag));
	} else if (strcmp(argv[0], "sn-rm") == 0) {
		static const struct option long_options[] = {
			{"help", no_argument, 0, 'h'},
			{0, 0, 0, 0}
		};
		static const char usage[] = "sn-rm <\"uuid\"|\"address\"> <uuid|address>";
		int flag = 0;

		while((c = getopt_long(argc, argv, "+h", long_options, NULL)) > -1) {
			switch (c) {
				case 'h':
					CATCHUNIX(fprintf(stdout, "%s\n", usage));
					rc = 0;
					goto out;
				default:
					CATCHUNIX(fprintf(stderr, "%s\n", usage));
					CATCH(EINVAL);
					break;
			}
		}
		if (optind != (argc-2) ) {
			CATCHUNIX(fprintf(stderr, "invalid command: %s\n", usage));
			CATCH(EINVAL);
		}
		if (strcmp(argv[optind], "uuid") == 0)
			flag |= CONFUGA_SN_UUID;
		else if (strcmp(argv[optind], "address") == 0)
			flag |= CONFUGA_SN_ADDR;
		else CATCH(EINVAL);
		CATCH(confuga_snrm(C, argv[optind+1], flag));
	} else {
		CATCHUNIX(fprintf(stderr, "invalid command: %s\n", argv[0]));
		CATCH(EINVAL);
	}

	rc = 0;
	goto out;
out:
	return rc;
}

static void help (const char *argv0)
{
	fprintf(stdout, "use: %s [options] <Confuga root> <cmd> [...]\n", argv0);
	fprintf(stdout, "The most common options are:\n");
	fprintf(stdout, " %-30s Enable debugging for this subsystem.\n", "-d,--debug=<name>");
	fprintf(stdout, " %-30s Send debugging to this file. (can also be :stderr, or :stdout)\n", "-o,--debug-file=<file>");
	fprintf(stdout, " %-30s Show version info.\n", "-v,--version");
	fprintf(stdout, " %-30s This message.\n", "-h,--help");
	fprintf(stdout, "\n");
	fprintf(stdout, "Where debug flags are: ");
	debug_flags_print(stdout);
	fprintf(stdout, "\n\n");
}

int main (int argc, char *argv[])
{
	int rc;
	int c;
	confuga *C = NULL;
	const char *root = NULL;

	random_init();
	debug_config(argv[0]);

	static const struct option long_options[] = {
		{"debug", required_argument, 0, 'd'},
		{"debug-file", required_argument, 0, 'o'},
		{"help", no_argument, 0, 'h'},
		{"version", no_argument, 0, 'v'},
		{0, 0, 0, 0}
	};

	while((c = getopt_long(argc, argv, "+d:ho:v", long_options, NULL)) > -1) {
		switch (c) {
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'h':
				help(argv[0]);
				exit(EXIT_SUCCESS);
				break;
			case 'o':
				debug_config_file(optarg);
				break;
			case 'v':
				cctools_version_print(stdout, argv[0]);
				exit(EXIT_SUCCESS);
				break;
			default:
				help(argv[0]);
				exit(EXIT_FAILURE);
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if (optind >= argc) {
		help(argv[0]);
		exit(EXIT_FAILURE);
	}

	root = argv[optind];
	rc = confuga_connect(&C, root, NULL);
	if (rc) {
		fprintf(stderr, "could not connect to %s: %s\n", root, strerror(rc));
		exit(EXIT_FAILURE);
	}
	optind++;

	if (optind < argc) {
		process(C, argc-optind, &argv[optind]);
	}

	rc = confuga_disconnect(C);
	if (rc) {
		fprintf(stderr, "warning: could not disconnect from %s: %s\n", argv[optind], strerror(rc));
	}

	return 0;
}

/* vim: set noexpandtab tabstop=8: */
