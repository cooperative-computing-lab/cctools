/*
  Copyright (C) 2022 The University of Notre Dame
  This software is distributed under the GNU General Public License.
  See the file COPYING for details.
*/

#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/stat.h>

#include "buffer.h"
#include "debug.h"
#include "get_line.h"
#include "rmonitor_file_watch.h"
#include "rmonitor_helper_comm.h"
#include "stringtools.h"
#include "xxmalloc.h"

int parse_boolean(const char *fname, struct jx *spec, const char *key, int default_value)
{

	int found = 0;
	struct jx *val = jx_lookup_guard(spec, key, &found);

	int result = default_value;
	if (found) {
		if (jx_istype(val, JX_BOOLEAN)) {
			result = jx_istrue(val);
		} else {
			fatal("Value of %s for '%s' is not boolean.", key, fname);
		}
	}

	return result;
}

char *parse_str(const char *fname, struct jx *spec, const char *key, const char *default_value)
{

	int found = 0;
	struct jx *val = jx_lookup_guard(spec, key, &found);

	const char *result = default_value;
	if (found) {
		if (jx_istype(val, JX_STRING)) {
			result = val->u.string_value;
		} else {
			fatal("Value of %s for '%s' is not a string.", key, fname);
		}
	}

	if (result) {
		return xxstrdup(result);
	} else {
		return NULL;
	}
}

jx_int_t parse_int(const char *fname, struct jx *spec, const char *key, jx_int_t default_value)
{

	int found = 0;
	struct jx *val = jx_lookup_guard(spec, key, &found);

	int result = default_value;
	if (found) {
		if (jx_istype(val, JX_INTEGER)) {
			result = val->u.integer_value;
		} else {
			fatal("Value of %s for '%s' is not an integer.", key, fname);
		}
	}

	return result;
}

struct rmonitor_file_watch_event *parse_event(const char *fname, struct jx *spec)
{

	struct rmonitor_file_watch_event *e = calloc(1, sizeof(*e));

	// defaults:
	e->on_creation = 0;
	e->on_deletion = 0;
	e->on_truncate = 0;
	e->max_count = -1;
	e->total_count = 0;
	e->cycle_count = 0;
	e->on_pattern = NULL;
	e->label = NULL;

	e->label = parse_str(fname, spec, "label", NULL);
	e->max_count = parse_int(fname, spec, "count", -1);
	e->on_pattern = parse_str(fname, spec, "on-pattern", e->on_pattern);
	e->on_creation = parse_boolean(fname, spec, "on-create", e->on_creation);
	e->on_deletion = parse_boolean(fname, spec, "on-delete", e->on_deletion);
	e->on_truncate = parse_boolean(fname, spec, "on-truncate", e->on_truncate);

	int error = 0;

	if (!e->label) {
		error = 1;
		warn(D_RMON | D_NOTICE, "A label for '%s' was not given.", fname);
	} else if (string_match_regex(e->label, "[^A-Za-z0-9_-]")) {
		error = 1;
		warn(D_RMON | D_NOTICE, "Label for '%s' has characters not in [A-Za-z0-9_-]", fname);
	} else {
		int defined = 0;
		if (e->on_creation) {
			defined++;
		}
		if (e->on_deletion) {
			defined++;
		}
		if (e->on_truncate) {
			defined++;
		}
		if (e->on_pattern) {
			defined++;
		}

		if (defined != 1) {
			error = 1;
			warn(D_RMON | D_NOTICE, "Exactly one of on-create, on-delete, on-truncate, or on-pattern should be specified for '%s'", fname);
		}
	}

	if (error) {
		free(e->label);
		free(e->on_pattern);
		free(e);

		return NULL;
	}

	return e;
}

void reset_events_counts(struct rmonitor_file_watch_info *f)
{
	struct rmonitor_file_watch_event *e;

	// reset counts for cycle
	list_first_item(f->events);
	while ((e = list_next_item(f->events))) {
		e->cycle_count = 0;
	}
}

int at_least_one_event_still_active(struct rmonitor_file_watch_info *f)
{
	struct rmonitor_file_watch_event *e;
	int at_least_one_active = 0;

	list_first_item(f->events);
	while ((e = list_next_item(f->events))) {
		if (e->max_count < 0 || e->total_count < e->max_count) {
			at_least_one_active = 1;
			break;
		}
	}

	return at_least_one_active;
}

const char *construct_label(struct rmonitor_file_watch_info *f)
{
	struct rmonitor_file_watch_event *e;
	static buffer_t *b = NULL;

	if (!b) {
		b = malloc(sizeof(*b));
		buffer_init(b);
	}

	buffer_rewind(b, 0);

	int event_count = 0;
	char *sep = "";

	list_first_item(f->events);
	while ((e = list_next_item(f->events))) {
		if (e->cycle_count > 0) {
			e->total_count += e->cycle_count;
			event_count += e->cycle_count;

			buffer_printf(b, "%s%s(%" PRId64 ")", sep, e->label, e->cycle_count);
			sep = ",";
		}
	}

	if (event_count) {
		return buffer_tostring(b);
	} else {
		return NULL;
	}
}

int request_snapshot(struct rmonitor_file_watch_info *f)
{
	const char *label = construct_label(f);

	if (!label) {
		// no snapshot to request
		return 0;
	}

	struct rmonitor_msg msg;
	bzero(&msg, sizeof(msg));

	msg.type = SNAPSHOT;
	msg.error = 0;
	msg.origin = -1;

	strncpy(msg.data.s, label, sizeof(msg.data.s) - 1);
	int status = send_monitor_msg(&msg);

	return status;
}

void rmonitor_watch_file_aux(struct rmonitor_file_watch_info *f)
{

	struct rmonitor_file_watch_event *e;
	struct stat s;

	for (;;) {

		int created = 0;
		int deleted = 0;
		int shrank = 0;

		reset_events_counts(f);

		// if file is there, check if it was there before
		if (stat(f->filename, &s) == 0) {
			if (!f->exists) {
				created = 1;
			}
			f->exists = 1;
		} else {
			if (f->exists) {
				deleted = 1;
			}
			f->exists = 0;
			f->position = 0;
			f->last_mtime = 0;
			f->last_size = 0;
			f->last_ino = 0;
		}

		// check if file was truncated
		if (f->exists && f->last_mtime < s.st_mtime) {
			shrank = (f->last_size > s.st_size) || (f->last_ino != 0 && f->last_ino != s.st_ino);
			if (shrank) {
				if (f->from_start_if_truncated) {
					f->position = 0;
				} else {
					debug(D_RMON, "File '%s' was truncated. Some events may be lost.", f->filename);
					f->position = s.st_size;
				}
			}

			f->last_mtime = s.st_mtime;
			f->last_size = s.st_size;
			f->last_ino = s.st_ino;
		}

		// count created, deleted and shrank events
		if (created || deleted || shrank) {
			list_first_item(f->events);
			while ((e = list_next_item(f->events))) {
				if (e->on_creation && created) {
					e->cycle_count++;
					e->total_count++;
				}

				if (e->on_deletion && deleted) {
					e->cycle_count++;
					e->total_count++;
				}

				if (e->on_truncate && shrank) {
					e->cycle_count++;
					e->total_count++;
				}
			}
		}

		if (f->exists && f->event_with_pattern) {
			FILE *fp = fopen(f->filename, "r");
			if (!fp) {
				fatal("Could not open file '%s': %s.", strerror(errno));
			}

			f->position = fseek(fp, f->position, SEEK_SET);
			if (f->position < 0) {
				fatal("Could not seek file '%s': %s.", strerror(errno));
			}

			char *line;
			while ((line = get_line(fp))) {
				list_first_item(f->events);
				while ((e = list_next_item(f->events))) {
					if (e->max_count < 0 || e->max_count < e->total_count) {
						if (string_match_regex(line, e->on_pattern)) {
							e->cycle_count++;
							e->total_count++;
						}
					}
				}
				free(line);
			}

			f->position = ftell(fp);
			fclose(fp);
		}

		if (request_snapshot(f) < 0) {
			fatal("Could not contact resource_monitor.");
		}

		if (!at_least_one_event_still_active(f)) {
			debug(D_RMON, "No more active events for '%s'.", f->filename);
			exit(0);
		}

		if (f->delete_if_found) {
			unlink(f->filename);

			f->exists = 0;
			f->position = 0;
			f->last_size = 0;
			f->last_mtime = 0;
			f->last_ino = 0;
		}

		sleep(1);
	}
}

void initialize_watch_events(struct rmonitor_file_watch_info *f, struct jx *watch_spec)
{

	struct jx *events_array = jx_lookup(watch_spec, "events");

	if (!events_array) {
		fatal("File watch for '%s' did not define any events", f->filename);
	}

	if (!jx_istype(events_array, JX_ARRAY)) {
		fatal("Value for key 'events' in file watch for '%s' is not an array.", f->filename);
	}

	f->events = list_create();

	struct jx *event_spec;
	int error = 0;
	for (void *i = NULL; (event_spec = jx_iterate_array(events_array, &i));) {
		struct rmonitor_file_watch_event *e = parse_event(f->filename, event_spec);
		if (e) {
			if (e->on_pattern) {
				// at least one event defines a pattern, thus we need line by
				// line processing.
				f->event_with_pattern = 1;
			}
			list_push_tail(f->events, e);
			debug(D_RMON, "Added event for file '%s', label '%s', max_count %" PRId64, f->filename, e->label, e->max_count);
		} else {
			error = 1;
		}
	}

	if (error) {
		fatal("Error parsing file watch for '%s'.", f->filename);
	}
}

struct rmonitor_file_watch_info *initialize_watch(const char *fname, struct jx *watch_spec)
{

	struct rmonitor_file_watch_info *f;
	f = calloc(1, sizeof(*f));

	f->delete_if_found = parse_boolean(fname, watch_spec, "delete-if-found", 0 /* default false */);
	f->from_start = parse_boolean(fname, watch_spec, "from-start", 0 /* default false */);
	f->from_start_if_truncated = parse_boolean(fname, watch_spec, "from-start-if-truncated", 1 /* default true */);

	f->filename = fname;

	f->exists = 0;
	f->position = 0;
	f->last_size = 0;
	f->last_mtime = 0;
	f->last_ino = 0;

	f->event_with_pattern = 0;
	initialize_watch_events(f, watch_spec);

	struct stat s;
	if (stat(fname, &s) == 0) {
		f->exists = 1;
		f->last_ino = s.st_ino;

		if (!f->from_start) {
			f->position = s.st_size;
			f->last_size = s.st_size;
			f->last_mtime = s.st_mtime;
		}
	}

	return f;
}

void rmonitor_file_watch_event_free(struct rmonitor_file_watch_event *e)
{
	free(e->label);
	free(e->on_pattern);
	free(e);
}

void rmonitor_file_watch_info_free(struct rmonitor_file_watch_info *f)
{
	struct rmonitor_file_watch_event *e;
	list_first_item(f->events);
	while ((e = list_pop_head(f->events))) {
		rmonitor_file_watch_event_free(e);
	}
	list_delete(f->events);
	free(f);
}

pid_t rmonitor_watch_file(const char *fname, struct jx *watch_spec)
{

	struct rmonitor_file_watch_info *f = initialize_watch(fname, watch_spec);

	pid_t pid = fork();
	if (pid > 0) {
		rmonitor_file_watch_info_free(f);
		return pid;
	} else if (pid < 0) {
		fatal("Could not start watch for: %s %s", fname, strerror(errno));
		return pid;
	} else {
		/* terminate this process when main process monitor goes away */
		prctl(PR_SET_PDEATHSIG, SIGKILL);

		signal(SIGCHLD, SIG_DFL);
		signal(SIGINT, SIG_DFL);
		signal(SIGQUIT, SIG_DFL);
		signal(SIGTERM, SIG_DFL);

		rmonitor_watch_file_aux(f);
		exit(0);
	}
}
