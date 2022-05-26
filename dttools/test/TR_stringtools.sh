#!/bin/sh

. ../../dttools/test/test_runner_common.sh

exe="stringtools.test"

prepare()
{
	${CC} -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none -I ../src ../src/libdttools.a -lm <<EOF
#include "stringtools.h"

#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

#define a_streql(a,b) \\
	do {\\
		const char *_a = (a);\\
		const char *_b = (b);\\
		if (strcmp(_a, _b) != 0) {\\
			fprintf(stderr, "%s:%d: wanted \\"%s\\", got \\"%s\\"\\n", __FILE__, __LINE__, _b, _a);\\
			exit(EXIT_FAILURE);\\
		}\\
	} while (0)

#define a_int64eql(a,b) \\
	do {\\
		int64_t _a = (a);\\
		int64_t _b = (b);\\
		if (_a != _b) {\\
			fprintf(stderr, "%s:%d: wanted %" PRId64 ", got %" PRId64 "\\n", __FILE__, __LINE__, _b, _a);\\
			exit(EXIT_FAILURE);\\
		}\\
	} while (0)

void t_string_metric (void)
{
	a_streql(string_metric(1.0, -1, NULL), "1.0");
	a_streql(string_metric(pow(2, 10)-1, -1, NULL), "1023.0");
	a_streql(string_metric(pow(2, 10), -1, NULL), "1.0 K");
	a_streql(string_metric(pow(2, 10)*1.5, -1, NULL), "1.5 K");
	a_streql(string_metric(pow(2, 11), -1, NULL), "2.0 K");
	a_streql(string_metric(pow(2, 20), -1, NULL), "1.0 M");
	a_streql(string_metric(pow(2, 40), -1, NULL), "1.0 T");
	a_streql(string_metric(pow(2, 50), -1, NULL), "1.0 P");
	a_streql(string_metric(pow(2, 60), -1, NULL), "1024.0 P");
}

void t_string_parse (void)
{
	a_int64eql(string_metric_parse("1"), 1LL);
	a_int64eql(string_metric_parse("100"), 100LL);
	a_int64eql(string_metric_parse("1023 "), 1023LL);
	a_int64eql(string_metric_parse("1023 K"), 1023LL<<10);
	a_int64eql(string_metric_parse("1023 M"), 1023LL<<20);
	a_int64eql(string_metric_parse("1023 P"), 1023LL<<50);
}

void t_string_time_parse (void)
{
	a_int64eql(string_time_parse("1"), 1LL);
	a_int64eql(string_time_parse("100"), 100LL);
	a_int64eql(string_time_parse("1023 s"), 1023LL);
	a_int64eql(string_time_parse("1023s"), 1023LL);
	a_int64eql(string_time_parse("1023m"), 1023LL*60);
	a_int64eql(string_time_parse("1023 M"), 1023LL*60);
	a_int64eql(string_time_parse("1023 h"), 1023LL*60*60);
	a_int64eql(string_time_parse("1023 d"), 1023LL*60*60*24);
}

void t_string_escape_shell (void)
{
	a_streql(string_escape_shell("$ test var"), "\\"\\\\$ test var\\"");
	a_streql(string_escape_shell("\`test var\`"), "\\"\\\\\`test var\\\\\`\\"");
	a_streql(string_escape_shell("\\\\test var"), "\\"\\\\\\\\test var\\"");
	a_streql(string_escape_shell("\"test var\""), "\\"\\\\\\"test var\\\\\\"\\"");
}

void t_string_escape_condor (void)
{
	a_streql(string_escape_condor("test var"), "\\"test var \\"");
	a_streql(string_escape_condor("test \\"var\\""), "\\"test \\"\\"var\\"\\" \\"");
	a_streql(string_escape_condor("test 'var'"), "\\"test '''var''' \\"");
	a_streql(string_escape_condor("\\"test 'var'\\""), "\\"\\"\\"test '''var'''\\"\\" \\"");
}

int main (int argc, char *argv[])
{
	t_string_metric();
	t_string_parse();
	t_string_time_parse();
	t_string_escape_shell();
	t_string_escape_condor();

	return 0;
}
EOF
	return $?
}

run()
{
	./"$exe"
	return $?
}

clean()
{
	rm -f "$exe"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
