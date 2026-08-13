/*
Copyright (C) 2026- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
vine_test_stdout_capture is a small internal test harness that exercises
the taskvine manager directly through the C API, without requiring the
Python bindings.

It currently checks one thing: that a task's captured stdout comes back
byte-for-byte correct at and around the worker's inline stdout-capture
threshold. vine_worker.c's send_complete_tasks() embeds a completed task's
stdout directly into the "complete" message it sends to the manager when
that stdout is <= 1024 bytes (see the output_length check in
send_complete_tasks); that message used to be built with vsprintf() into a
fixed-size buffer with no bound. This program submits tasks whose stdout
straddles that threshold and fails loudly if the manager ever receives
truncated or corrupted output.

This is a starting point, not a full unit-test suite -- see the "test
debt" item in the taskvine tech-debt audit for the larger plan. More
scenarios can be added here as coverage grows.
*/

#include "taskvine.h"

#include "cctools.h"
#include "stringtools.h"

#include <errno.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Sizes to probe around the worker's inline stdout-capture threshold. */
static int output_sizes[] = {1, 500, 1023, 1024, 1025, 2048};
#define NUM_SIZES ((int)(sizeof(output_sizes) / sizeof(output_sizes[0])))

static char *expected_output(int size)
{
	char *s = malloc(size + 1);
	memset(s, 'x', size);
	s[size] = 0;
	return s;
}

static void show_help(const char *cmd)
{
	printf("Usage: %s [options]\n", cmd);
	printf("Where options are:\n");
	printf("-Z <file>  Write listening port to this file. (required)\n");
	printf("-v         Show version information.\n");
	printf("-h         Show this help screen.\n");
}

int main(int argc, char *argv[])
{
	const char *port_file = 0;
	int c;

	while ((c = getopt(argc, argv, "Z:vh")) != -1) {
		switch (c) {
		case 'Z':
			port_file = optarg;
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			return 0;
		case 'h':
			show_help(argv[0]);
			return 0;
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	if (!port_file) {
		show_help(argv[0]);
		return 1;
	}

	struct vine_manager *q = vine_create(0);
	if (!q) {
		fprintf(stderr, "vine_test_stdout_capture: couldn't listen on any port!\n");
		return 1;
	}

	FILE *file = fopen(port_file, "w");
	if (!file) {
		fprintf(stderr, "vine_test_stdout_capture: couldn't open %s: %s\n", port_file, strerror(errno));
		return 1;
	}
	fprintf(file, "%d\n", vine_port(q));
	fclose(file);

	printf("vine_test_stdout_capture: listening on port %d\n", vine_port(q));

	int i;
	for (i = 0; i < NUM_SIZES; i++) {
		int size = output_sizes[i];
		char command[256];
		char tag[32];

		/* Emit exactly `size` bytes of 'x' on stdout, no trailing newline. */
		snprintf(command, sizeof(command), "head -c %d /dev/zero | tr '\\0' 'x'", size);
		snprintf(tag, sizeof(tag), "%d", size);

		struct vine_task *t = vine_task_create(command);
		vine_task_set_cores(t, 1);
		vine_task_set_tag(t, tag);
		vine_submit(q, t);
	}

	int failures = 0;
	int remaining = NUM_SIZES;
	while (remaining > 0) {
		struct vine_task *t = vine_wait(q, 30);
		if (!t) {
			fprintf(stderr, "vine_test_stdout_capture: timed out waiting for a task to complete (%d still outstanding)\n", remaining);
			failures++;
			break;
		}
		remaining--;

		int size = atoi(vine_task_get_tag(t));
		char *expected = expected_output(size);
		const char *actual = vine_task_get_stdout(t);

		if (vine_task_get_result(t) != VINE_RESULT_SUCCESS) {
			fprintf(stderr, "vine_test_stdout_capture: FAIL size=%d: task did not succeed (result=%d)\n", size, vine_task_get_result(t));
			failures++;
		} else if (!actual) {
			fprintf(stderr, "vine_test_stdout_capture: FAIL size=%d: no stdout captured\n", size);
			failures++;
		} else if ((int)strlen(actual) != size) {
			fprintf(stderr, "vine_test_stdout_capture: FAIL size=%d: expected %d bytes of stdout, got %d\n", size, size, (int)strlen(actual));
			failures++;
		} else if (strcmp(actual, expected) != 0) {
			fprintf(stderr, "vine_test_stdout_capture: FAIL size=%d: stdout content does not match expected pattern\n", size);
			failures++;
		} else {
			printf("vine_test_stdout_capture: PASS size=%d: stdout captured correctly (%d bytes)\n", size, size);
		}

		free(expected);
		vine_task_delete(t);
	}

	vine_delete(q);

	if (failures) {
		fprintf(stderr, "vine_test_stdout_capture: %d of %d size checks failed\n", failures, NUM_SIZES);
		return 1;
	}

	printf("vine_test_stdout_capture: all %d size checks passed\n", NUM_SIZES);
	return 0;
}

/* vim: set noexpandtab tabstop=8: */
