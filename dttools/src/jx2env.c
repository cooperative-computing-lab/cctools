/*
  Copyright (C) 2022 The University of Notre Dame This software is
  distributed under the GNU General Public License.  See the file
  COPYING for details.


  Given a JSON object in a file, it prints...

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "getopt.h"
#include "int_sizes.h"
#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "stringtools.h"
#include "xxmalloc.h"

void show_help(const char *exe) {
	fprintf(stderr, "Usage:\n%s [--csh] input-file NAME=json.key.path [NAME=json.key.path ...]\n", exe);
}

static const struct option long_options[] =
{
	{"csh", no_argument, 0, 'c'},
	{0, 0, 0, 0}
};

char *value_of_simple(struct jx *j, const char *spec) {
	int found = 0;

	struct jx *k = jx_lookup_guard(j, spec, &found);

	if(!found) {

		return xxstrdup("");

	} else if(jx_istype(k, JX_NULL)) {

		return xxstrdup("");

	} else if(jx_istype(k, JX_STRING)) {

			return xxstrdup(k->u.string_value);

	} else if(jx_istype(k, JX_BOOLEAN)) {

			return xxstrdup(jx_istrue(k) ? "1" : "0");

	} else if(jx_istype(k, JX_INTEGER)) {

			return string_format("%" PRId64 "", k->u.integer_value);

	} else if(jx_istype(k, JX_DOUBLE)) {

			return string_format("%lf", k->u.double_value);

	}

	fprintf(stderr, "error: %s does not point to a scalar value", spec);
	exit(4);

	return NULL;
}

char *value_of_dotted(struct jx *j, const char *spec) {

	int found = 0;
	char *spec_mod = xxstrdup(spec);
	char *next_dot = strchr(spec_mod, '.');

	if(next_dot) {
		*next_dot = '\0';
		next_dot++;

		struct jx *l = jx_lookup_guard(j, spec_mod, &found);
		char *value;

		if(found) {
			value = value_of_dotted(l, next_dot);
		} else {
			value = xxstrdup("x");
		}

		free(spec_mod);
		return value;
	} else {
		return value_of_simple(j, spec);
	}
}

int main(int argc, char **argv) {

	int csh = 0;

	signed char c;
    while((c = getopt_long(argc, argv, "c", long_options, NULL)) >= 0)
    {
		switch (c) {
			case 'c':
				csh = 1;
				break;
			default:
				break;
		}
	}

	if(optind >= argc) {
		show_help(argv[0]);
		exit(1);
	}


	const char *filename = argv[optind];
	optind++;

	struct jx *j = jx_parse_file(filename);

	if(!j) {
		fprintf(stderr, "%s: Could not process  file '%s'\n", argv[0], filename);
		exit(2);
	}

	while(optind < argc) {
		char *spec = xxstrdup(argv[optind]);
		optind++;

		char *path = strchr(spec, '=');
		if(!path || strlen(path+1) < 1) {
			fprintf(stderr, "Malformed specification: %s\n", spec);
			show_help(argv[0]);
			exit(1);
		}

		*path = '\0';
		path++;

		char *value = value_of_dotted(j, path);

		if(csh) {
			fprintf(stdout, "setenv %s \"%s\"\n", spec, value); 
		} else {
			fprintf(stdout, "export %s=\"%s\"\n", spec, value); 
		}

		free(spec);
		free(value);
	}


	jx_delete(j);
}

/* vim: set noexpandtab tabstop=8: */
