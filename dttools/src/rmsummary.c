/*
  Copyright (C) 2013- The University of Notre Dame This software is
  distributed under the GNU General Public License.  See the file
  COPYING for details.
*/

/* In a summary file, all time fields are written as double, with
   units in seconds. Internally, however, time fields are kept as
   int64_t, with units in microseconds. Therefore we need to convert back
   and forth when reading/printing summaries.

   Similarly, disk is reported as double, in megabytes,
   but it is kept internally as int64_t, in bytes.*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stddef.h>
#include <math.h>

#include <ctype.h>

#include "buffer.h"
#include "debug.h"
#include "int_sizes.h"
#include "jx.h"
#include "jx_parse.h"
#include "jx_pretty_print.h"
#include "jx_print.h"
#include "list.h"
#include "macros.h"
#include "rmsummary.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "hash_table.h"

static int units_initialized = 0;
static struct hash_table *conversion_fields = NULL;
static struct hash_table *multiplier_of     = NULL;

struct conversion_field {
	char *name;
	char *internal_unit;
	char *external_unit;
	int  float_flag;
};

void add_conversion_field(const char *name, const char *internal, const char *external, int float_flag) {
	struct conversion_field *c = malloc(sizeof(struct conversion_field));

	c->name          = xxstrdup(name);
	c->internal_unit = xxstrdup(internal);
	c->external_unit = xxstrdup(external);
	c->float_flag    = float_flag;

	hash_table_insert(conversion_fields, name, (void *) c);
}

void initialize_units() {
	units_initialized = 1;

	conversion_fields = hash_table_create(32, 0);
	multiplier_of     = hash_table_create(32, 0);

	add_conversion_field("wall_time",                "us",     "s",       1);
	add_conversion_field("cpu_time",                 "us",     "s",       1);
	add_conversion_field("start",                    "us",     "us",      0);
	add_conversion_field("end",                      "us",     "us",      0);
	add_conversion_field("memory",                   "MB",     "MB",      0);
	add_conversion_field("virtual_memory",           "MB",     "MB",      0);
	add_conversion_field("swap_memory",              "MB",     "MB",      0);
	add_conversion_field("disk",                     "MB",     "MB",      0);
	add_conversion_field("bytes_read",               "B",      "MB",      1);
	add_conversion_field("bytes_written",            "B",      "MB",      1);
	add_conversion_field("bytes_received",           "B",      "MB",      1);
	add_conversion_field("bytes_sent",               "B",      "MB",      1);
	add_conversion_field("bandwidth",                "bps",    "Mbps",    1);
	add_conversion_field("cores",                    "cores",  "cores",   0);
	add_conversion_field("max_concurrent_processes", "procs",  "procs",   0);
	add_conversion_field("total_processes",          "procs",  "procs",   0);
	add_conversion_field("total_files",              "files",  "files",   0);

	/* to internal units */
	hash_table_insert(multiplier_of, "us",    (void **) 1);
	hash_table_insert(multiplier_of, "B",     (void **) 1);
	hash_table_insert(multiplier_of, "bps",   (void **) 1);
	hash_table_insert(multiplier_of, "cores", (void **) 1);
	hash_table_insert(multiplier_of, "procs", (void **) 1);
	hash_table_insert(multiplier_of, "files", (void **) 1);

	/* size units */
	hash_table_insert(multiplier_of, "kB", (void **) KILOBYTE);
	hash_table_insert(multiplier_of, "MB", (void **) MEGABYTE);
	hash_table_insert(multiplier_of, "GB", (void **) GIGABYTE);

	/* time units */
	hash_table_insert(multiplier_of, "s",  (void **) USECOND);
	hash_table_insert(multiplier_of, "ms", (void **) 1000);

	/* rate units */
	hash_table_insert(multiplier_of, "Gbps",  (void **) 1000000000);
	hash_table_insert(multiplier_of, "Mbps",  (void **) 1000000);
}

int rmsummary_to_internal_unit(const char *field, double input_number, int64_t *output_number, const char *unit) {

	if(!units_initialized)
		initialize_units();

	uintptr_t multiplier = (uintptr_t) hash_table_lookup(multiplier_of, unit);

	if(!multiplier) {
		warn(D_DEBUG, "Unknown units: '%s'", unit);
		return 0;
	}

	input_number *= multiplier;

	struct conversion_field *cf = hash_table_lookup(conversion_fields, field);
	if(!cf) {
		warn(D_DEBUG, "Unknown field: '%s'", field);
		return 0;
	}

	const char *to_unit = cf->internal_unit;
	if(to_unit && strcmp("MB", to_unit) == 0) {
		input_number /= MEGABYTE;
	}

	*output_number = (int64_t) input_number;

	return 1;
}

double rmsummary_to_external_unit(const char *field, int64_t n) {

	if(!units_initialized)
		initialize_units();

	struct conversion_field *cf = hash_table_lookup(conversion_fields, field);

	const char *to_unit   = cf->external_unit;
	const char *from_unit = cf->internal_unit;

	if(from_unit && to_unit && (strcmp(from_unit, to_unit) == 0)) {
		return (double) n;
	}

	uintptr_t divider = (uintptr_t) hash_table_lookup(multiplier_of, to_unit);

	if(!divider)
		fatal("Unknown units: '%s'", to_unit);

	double nd = ((double) n) / divider;

	return nd;
}

int rmsummary_assign_char_field(struct rmsummary *s, const char *key, char *value) {
	if(strcmp(key, "category") == 0) {
		if(s->category)
			free(s->category);
		s->category = xxstrdup(value);
		return 1;
	}

	if(strcmp(key, "command") == 0) {
		if(s->command)
			free(s->command);
		s->command = xxstrdup(value);
		return 1;
	}

	if(strcmp(key, "exit_type") == 0) {
		if(s->exit_type)
			free(s->exit_type);
		s->exit_type = xxstrdup(value);
		return 1;
	}

	if(strcmp(key, "task_id") == 0) {
		if(s->task_id)
			free(s->task_id);
		s->task_id = xxstrdup(value);
		return 1;
	}


	return 0;
}

int64_t rmsummary_get_int_field(struct rmsummary *s, const char *key) {
	if(strcmp(key, "start") == 0) {
		return s->start;
	}

	if(strcmp(key, "end") == 0) {
		return s->end;
	}

	if(strcmp(key, "wall_time") == 0) {
		return s->wall_time;
	}

	if(strcmp(key, "cpu_time") == 0) {
		return s->cpu_time;
	}

	if(strcmp(key, "signal") == 0) {
		return s->signal;
	}

	if(strcmp(key, "exit_status") == 0) {
		return s->exit_status;
	}

	if(strcmp(key, "last_error") == 0) {
		return s->last_error;
	}

	if(strcmp(key, "max_concurrent_processes") == 0) {
		return s->max_concurrent_processes;
	}

	if(strcmp(key, "total_processes") == 0) {
		return s->total_processes;
	}

	if(strcmp(key, "virtual_memory") == 0) {
		return s->virtual_memory;
	}

	if(strcmp(key, "memory") == 0) {
		return s->memory;
	}

	if(strcmp(key, "swap_memory") == 0) {
		return s->swap_memory;
	}

	if(strcmp(key, "bytes_read") == 0) {
		return s->bytes_read;
	}

	if(strcmp(key, "bytes_written") == 0) {
		return s->bytes_written;
	}

	if(strcmp(key, "bytes_received") == 0) {
		return s->bytes_received;
	}

	if(strcmp(key, "bytes_sent") == 0) {
		return s->bytes_sent;
	}

	if(strcmp(key, "bandwidth") == 0) {
		return s->bandwidth;
	}

	if(strcmp(key, "total_files") == 0) {
		return s->total_files;
	}

	if(strcmp(key, "disk") == 0) {
		return s->disk;
	}

	if(strcmp(key, "cores") == 0) {
		return s->cores;
	}

	if(strcmp(key, "gpus") == 0) {
		return s->gpus;
	}

	return 0;
}

const char *rmsummary_get_char_field(struct rmsummary *s, const char *key) {
	if(strcmp(key, "category") == 0) {
		return s->category;
	}

	if(strcmp(key, "command") == 0) {
		return s->command;
	}

	if(strcmp(key, "exit_type") == 0) {
		return s->exit_type;
	}

	if(strcmp(key, "task_id") == 0) {
		return s->task_id;
	}

	return NULL;
}

int rmsummary_assign_int_field(struct rmsummary *s, const char *key, int64_t value) {
	if(strcmp(key, "start") == 0) {
		s->start = value;
		return 1;
	}

	if(strcmp(key, "end") == 0) {
		s->end = value;
		return 1;
	}

	if(strcmp(key, "wall_time") == 0) {
		s->wall_time = value;
		return 1;
	}

	if(strcmp(key, "cpu_time") == 0) {
		s->cpu_time = value;
		return 1;
	}

	if(strcmp(key, "signal") == 0) {
		s->signal = value;
		return 1;
	}

	if(strcmp(key, "exit_status") == 0) {
		s->exit_status = value;
		return 1;
	}

	if(strcmp(key, "last_error") == 0) {
		s->last_error = value;
		return 1;
	}

	if(strcmp(key, "max_concurrent_processes") == 0) {
		s->max_concurrent_processes = value;
		return 1;
	}

	if(strcmp(key, "total_processes") == 0) {
		s->total_processes = value;
		return 1;
	}

	if(strcmp(key, "virtual_memory") == 0) {
		s->virtual_memory = value;
		return 1;
	}

	if(strcmp(key, "memory") == 0) {
		s->memory = value;
		return 1;
	}

	if(strcmp(key, "swap_memory") == 0) {
		s->swap_memory = value;
		return 1;
	}

	if(strcmp(key, "bytes_read") == 0) {
		s->bytes_read = value;
		return 1;
	}

	if(strcmp(key, "bytes_written") == 0) {
		s->bytes_written = value;
		return 1;
	}

	if(strcmp(key, "bytes_received") == 0) {
		s->bytes_received = value;
		return 1;
	}

	if(strcmp(key, "bytes_sent") == 0) {
		s->bytes_sent = value;
		return 1;
	}

	if(strcmp(key, "bandwidth") == 0) {
		s->bandwidth = value;
		return 1;
	}

	if(strcmp(key, "total_files") == 0) {
		s->total_files = value;
		return 1;
	}

	if(strcmp(key, "disk") == 0) {
		s->disk = value;
		return 1;
	}

	if(strcmp(key, "cores") == 0) {
		s->cores = value;
		return 1;
	}

	if(strcmp(key, "gpus") == 0) {
		s->gpus = value;
		return 1;
	}

	return 0;
}

int rmsummary_assign_summary_field(struct rmsummary *s, char *key, struct jx *value) {

	if(strcmp(key, "limits_exceeded") == 0) {
		s->limits_exceeded = json_to_rmsummary(value);
		return 1;
	} else if(strcmp(key, "peak_times") == 0) {
		s->peak_times = json_to_rmsummary(value);
		return 1;
	}

	return 0;
}

#define peak_time_to_json(o, u, s, f)\
	if((s)->f > -1) {\
		jx_insert(o, jx_string(#f), u->float_flag ? jx_double(rmsummary_to_external_unit("wall_time", (s)->f)) : jx_integer(rmsummary_to_external_unit("wall_time", (s)->f)));\
	}

struct jx *peak_times_to_json(struct rmsummary *s) {
	if(!units_initialized)
		initialize_units();

	struct jx *output = jx_object(NULL);

	struct conversion_field *cf = hash_table_lookup(conversion_fields, "wall_time");

	peak_time_to_json(output, cf, s, disk);
	peak_time_to_json(output, cf, s, total_files);
	peak_time_to_json(output, cf, s, bandwidth);
	peak_time_to_json(output, cf, s, bytes_sent);
	peak_time_to_json(output, cf, s, bytes_received);
	peak_time_to_json(output, cf, s, bytes_written);
	peak_time_to_json(output, cf, s, bytes_read);
	peak_time_to_json(output, cf, s, swap_memory);
	peak_time_to_json(output, cf, s, virtual_memory);
	peak_time_to_json(output, cf, s, memory);
	peak_time_to_json(output, cf, s, total_processes);
	peak_time_to_json(output, cf, s, max_concurrent_processes);
	peak_time_to_json(output, cf, s, cores);
	peak_time_to_json(output, cf, s, cpu_time);

	jx_insert(output, jx_string("units"), jx_string(cf->external_unit));

	return output;
}


#define field_to_json(o, s, f)\
	if((s)->f > -1) {\
		struct conversion_field *cf = hash_table_lookup(conversion_fields, #f);\
		if(cf) {\
			struct jx *array = jx_arrayv(\
					cf->float_flag ? jx_double(rmsummary_to_external_unit(#f, (s)->f)) : jx_integer(rmsummary_to_external_unit(#f, (s)->f))\
					, jx_string(cf->external_unit), NULL);\
			jx_insert(o, jx_string(#f), array);\
		}\
	}

struct jx *rmsummary_to_json(const struct rmsummary *s, int only_resources) {
	if(!units_initialized)
		initialize_units();

	struct jx *output = jx_object(NULL);

	if(!only_resources) {
		if(s->peak_times) {
			struct jx *peaks = peak_times_to_json(s->peak_times);
			jx_insert(output, jx_string("peak_times"), peaks);
		}
	}

	field_to_json(output, s, disk);
	field_to_json(output, s, total_files);
	field_to_json(output, s, bandwidth);
	field_to_json(output, s, bytes_sent);
	field_to_json(output, s, bytes_received);
	field_to_json(output, s, bytes_written);
	field_to_json(output, s, bytes_read);
	field_to_json(output, s, swap_memory);
	field_to_json(output, s, virtual_memory);
	field_to_json(output, s, memory);
	field_to_json(output, s, total_processes);
	field_to_json(output, s, max_concurrent_processes);
	field_to_json(output, s, cores);
	field_to_json(output, s, cpu_time);
	field_to_json(output, s, wall_time);
	field_to_json(output, s, end);
	field_to_json(output, s, start);

	if(!only_resources) {
		if(s->exit_type)
		{
			if( strcmp(s->exit_type, "signal") == 0 ) {
				jx_insert_integer(output, "signal", s->signal);
				jx_insert_string(output, "exit_type", "signal");
			} else if( strcmp(s->exit_type, "limits") == 0 ) {
				if(s->limits_exceeded) {
					struct jx *lim = rmsummary_to_json(s->limits_exceeded, 1);
					jx_insert(output, jx_string("limits_exceeded"), lim);
				}
				jx_insert_string(output, "exit_type", "limits");
			} else {
				jx_insert_string(output, "exit_type", s->exit_type);
			}
		}

		if(s->last_error)
			jx_insert_integer(output, "last_error", s->last_error);

		jx_insert_integer(output, "exit_status", s->exit_status);

		if(s->command)
			jx_insert_string(output, "command",   s->command);

		if(s->task_id)
			jx_insert_string(output, "task_id",  s->task_id);

		if(s->category)
			jx_insert_string(output, "category",  s->category);
	}

	return output;
}

static int json_number_of_array(struct jx *array, char *field, int64_t *number) {

	struct jx_item *first = array->u.items;

	if(!first)
		return 0;

	double result;

	if(jx_istype(first->value, JX_DOUBLE)) {
		result = first->value->u.double_value;
	} else if(jx_istype(first->value, JX_INTEGER)) {
		result = (double) first->value->u.integer_value;
	} else {
		return 0;
	}

	struct jx_item *second = first->next;

	if(!second)
		return 0;

	if(!jx_istype(second->value, JX_STRING))
		return 0;

	char *unit = second->value->u.string_value;

	return rmsummary_to_internal_unit(field, result, number, unit);
}

struct rmsummary *json_to_rmsummary(struct jx *j) {
	if(!j || !jx_istype(j, JX_OBJECT))
		return NULL;

	struct rmsummary *s = rmsummary_create(-1);

	struct jx_pair *head = j->u.pairs;
	while(head) {
		if(!jx_istype(head->key, JX_STRING))
			continue;

		char *key = head->key->u.string_value;
		struct jx *value = head->value;

		if(jx_istype(value, JX_STRING)) {
			rmsummary_assign_char_field(s, key, value->u.string_value);
		} else if(jx_istype(value, JX_INTEGER)) {
			rmsummary_assign_int_field(s, key, value->u.integer_value);
		} else if(jx_istype(value, JX_ARRAY)) {
			int64_t number;
			int status = json_number_of_array(value, key, &number);
			if(status) {
				rmsummary_assign_int_field(s, key, number);
			}
		} else if(jx_istype(value, JX_OBJECT)) {
			rmsummary_assign_summary_field(s, key, value);
		}

		head = head->next;
	}

	return s;
}

/* Parse the file, assuming there is a single summary in it. */
struct rmsummary *rmsummary_parse_file_single(const char *filename)
{
	FILE *stream;
	stream = fopen(filename, "r");

	if(!stream)
	{
		debug(D_NOTICE, "Cannot open resources summary file: %s : %s\n", filename, strerror(errno));
		return NULL;
	}

	struct rmsummary *s = rmsummary_parse_next(stream);
	fclose(stream);

	return s;
}

struct rmsummary *rmsummary_parse_string(const char *str) {
	if(!str)
		return NULL;

	struct jx *j = jx_parse_string(str);

	if(!j)
		return NULL;

	struct rmsummary *s = json_to_rmsummary(j);

	jx_delete(j);
	return s;
}


/* Parse the file assuming there are multiple summaries in it. Summary
   boundaries are lines starting with # */
struct list *rmsummary_parse_file_multiple(const char *filename)
{
	FILE *stream;
	stream = fopen(filename, "r");
	if(!stream)
	{
		debug(D_NOTICE, "Cannot open resources summary file: %s : %s\n", filename, strerror(errno));
		return NULL;
	}

	struct list      *lst = list_create(0);
	struct rmsummary *s;

	do
	{
		s = rmsummary_parse_next(stream);

		if(s)
			list_push_tail(lst, s);
	} while(s);

	fclose(stream);

	return lst;
}

/* Parse the stream for the next summary */
struct rmsummary *rmsummary_parse_next(FILE *stream)
{
	struct jx *j = jx_parse_stream(stream);

	if(!j)
		return NULL;

	struct rmsummary *s = json_to_rmsummary(j);

	jx_delete(j);
	return s;
}

void rmsummary_print(FILE *stream, struct rmsummary *s, int pprint, struct jx *verbatim_fields)
{
	struct jx *jsum = rmsummary_to_json(s, 0);

	if(verbatim_fields) {
		if(!jx_istype(verbatim_fields, JX_OBJECT)) {
			fatal("Vebatim fields are not a json object.");
		}
		struct jx_pair *head = verbatim_fields->u.pairs;

		while(head) {
			jx_insert(jsum, jx_copy(head->key), jx_copy(head->value));
			head = head->next;
		}
	}

	if(pprint) {
		jx_pretty_print_stream(jsum, stream);
	} else {
		jx_print_stream(jsum, stream);
	}

	jx_delete(jsum);
}

void rmsummary_print_buffer(struct buffer *B, const struct rmsummary *s, int only_resources) {
	if(!s)
		return;

	struct jx *jsum = rmsummary_to_json(s, only_resources);

	if(jsum) {
		char *str = jx_print_string(jsum);
		buffer_printf(B, "%s", str);
		jx_delete(jsum);
		free(str);
	}
}

/* Create summary filling all numeric fields with default_value, and
all string fields with NULL. Usual values are 0, or -1. */
struct rmsummary *rmsummary_create(signed char default_value)
{
	struct rmsummary *s = malloc(sizeof(struct rmsummary));
	memset(s, default_value, sizeof(struct rmsummary));

	s->command   = NULL;
	s->category  = NULL;
	s->exit_type = NULL;
	s->task_id   = NULL;
	s->limits_exceeded = NULL;
	s->peak_times = NULL;

	s->last_error  = 0;
	s->exit_status = 0;
	s->signal = 0;

	return s;
}

void rmsummary_delete(struct rmsummary *s)
{
	if(!s)
		return;

	if(s->command)
		free(s->command);

	if(s->category)
		free(s->category);

	if(s->exit_type)
		free(s->exit_type);

	if(s->task_id)
		free(s->task_id);

	if(s->limits_exceeded)
		rmsummary_delete(s->limits_exceeded);

	if(s->peak_times)
		rmsummary_delete(s->peak_times);

	free(s);
}

void rmsummary_read_env_vars(struct rmsummary *s)
{
	char *value;
	if((value = getenv( RESOURCES_CORES  )))
		s->cores  = atoi(value);
	if((value = getenv( RESOURCES_MEMORY )))
		s->memory = atoi(value);
	if((value = getenv( RESOURCES_DISK )))
		s->disk   = atoi(value);
}

#define rmsummary_apply_op(dest, src, fn, field) (dest)->field = fn((dest)->field, (src)->field)

typedef int64_t (*rm_bin_op)(int64_t, int64_t);
void rmsummary_bin_op(struct rmsummary *dest, const struct rmsummary *src, rm_bin_op fn)
{
	if(!src || !dest)
		return;

	rmsummary_apply_op(dest, src, fn, start);
	rmsummary_apply_op(dest, src, fn, end);
	rmsummary_apply_op(dest, src, fn, exit_status);
	rmsummary_apply_op(dest, src, fn, last_error);
	rmsummary_apply_op(dest, src, fn, wall_time);
	rmsummary_apply_op(dest, src, fn, max_concurrent_processes);
	rmsummary_apply_op(dest, src, fn, total_processes);
	rmsummary_apply_op(dest, src, fn, cpu_time);
	rmsummary_apply_op(dest, src, fn, virtual_memory);
	rmsummary_apply_op(dest, src, fn, memory);
	rmsummary_apply_op(dest, src, fn, swap_memory);
	rmsummary_apply_op(dest, src, fn, bytes_read);
	rmsummary_apply_op(dest, src, fn, bytes_written);
	rmsummary_apply_op(dest, src, fn, bytes_sent);
	rmsummary_apply_op(dest, src, fn, bytes_received);
	rmsummary_apply_op(dest, src, fn, bandwidth);
	rmsummary_apply_op(dest, src, fn, total_files);
	rmsummary_apply_op(dest, src, fn, disk);

	rmsummary_apply_op(dest, src, fn, cores);
	rmsummary_apply_op(dest, src, fn, fs_nodes);
}

/* Copy the value for all the fields in src > -1 to dest */
static int64_t override_field(int64_t d, int64_t s)
{
	return (s > -1) ? s : d;
}

void rmsummary_merge_override(struct rmsummary *dest, const struct rmsummary *src)
{
	if(!src) {
		return;
	}

	rmsummary_bin_op(dest, src, override_field);
}

/* only update limit when new field value is larger than old, regardless of old
 * limits. */
#define merge_limit(d, s, field)\
{\
	int64_t df = d->field;\
	int64_t sf = s->field;\
	int64_t dl = -1;\
	int64_t sl = -1;\
	if(d->limits_exceeded) { dl = d->limits_exceeded->field; }\
	if(s->limits_exceeded) { sl = s->limits_exceeded->field; }\
	if(sf >= df) {\
		if(sf > -1 && !d->limits_exceeded) { d->limits_exceeded = rmsummary_create(-1);}\
		d->limits_exceeded->field = sl < 0 ? -1 : MAX(sl, dl);\
	}\
}

static void merge_limits(struct rmsummary *dest, const struct rmsummary *src)
{
	if(!dest || !src)
		return;

	if(!dest->limits_exceeded && !src->limits_exceeded)
		return;

	merge_limit(dest, src, max_concurrent_processes);
	merge_limit(dest, src, total_processes);
	merge_limit(dest, src, cpu_time);
	merge_limit(dest, src, virtual_memory);
	merge_limit(dest, src, memory);
	merge_limit(dest, src, swap_memory);
	merge_limit(dest, src, bytes_read);
	merge_limit(dest, src, bytes_written);
	merge_limit(dest, src, bytes_sent);
	merge_limit(dest, src, bytes_received);
	merge_limit(dest, src, bandwidth);
	merge_limit(dest, src, total_files);
	merge_limit(dest, src, disk);
	merge_limit(dest, src, cores);
	merge_limit(dest, src, fs_nodes);

}



/* Select the max of the fields */
static int64_t max_field(int64_t d, int64_t s)
{
	return (d > s) ? d : s;
}

void rmsummary_merge_max(struct rmsummary *dest, const struct rmsummary *src)
{
	if(!dest || !src)
		return;

	rmsummary_bin_op(dest, src, max_field);
	merge_limits(dest, src);

	if(src->peak_times) {
		if(!dest->peak_times) {
			dest->peak_times = rmsummary_create(-1);
		}
		rmsummary_merge_max(dest->peak_times, src->peak_times);
	}
}

#define max_op_w_time(dest, src, field)\
	if((dest)->field < (src)->field) {\
		(dest)->field = (src)->field;\
		(dest)->peak_times->field = (dest)->wall_time;\
	}

void rmsummary_merge_max_w_time(struct rmsummary *dest, const struct rmsummary *src)
{
	if(!src || !dest)
		return;

	if(!dest->peak_times)
		dest->peak_times = rmsummary_create(-1);

	rmsummary_apply_op(dest, src, max_field, start);
	rmsummary_apply_op(dest, src, max_field, end);
	rmsummary_apply_op(dest, src, max_field, wall_time);

	max_op_w_time(dest, src, max_concurrent_processes);
	max_op_w_time(dest, src, total_processes);
	max_op_w_time(dest, src, cpu_time);
	max_op_w_time(dest, src, virtual_memory);
	max_op_w_time(dest, src, memory);
	max_op_w_time(dest, src, swap_memory);
	max_op_w_time(dest, src, bytes_read);
	max_op_w_time(dest, src, bytes_written);
	max_op_w_time(dest, src, bytes_sent);
	max_op_w_time(dest, src, bytes_received);
	max_op_w_time(dest, src, bandwidth);
	max_op_w_time(dest, src, total_files);
	max_op_w_time(dest, src, disk);
	max_op_w_time(dest, src, cores);
	max_op_w_time(dest, src, fs_nodes);
}

/* Select the min of the fields, ignoring negative numbers */
static int64_t min_field(int64_t d, int64_t s)
{
	if(d < 0 || s < 0) {
		return MAX(-1, MAX(s, d)); /* return at least -1. treat -1 as undefined.*/
	} else {
		return MIN(s, d);
	}
}

void rmsummary_merge_min(struct rmsummary *dest, const struct rmsummary *src)
{
	if(!dest || !src)
		return;

	rmsummary_bin_op(dest, src, min_field);
	merge_limits(dest, src);

	if(src->peak_times) {
		if(!dest->peak_times) {
			dest->peak_times = rmsummary_create(-1);
		}
		rmsummary_merge_min(dest->peak_times, src->peak_times);
	}
}

void rmsummary_debug_report(const struct rmsummary *s)
{
	if(!s)
		return;

	if(s->cores != -1)
		debug(D_DEBUG, "max resource %-18s   : %" PRId64 "\n", "cores", s->cores);
	if(s->start != -1)
		debug(D_DEBUG, "max resource %-18s  s: %lf\n", "start", ((double) s->start / 1000000));
	if(s->end != -1)
		debug(D_DEBUG, "max resource %-18s  s: %lf\n", "end",   ((double) s->end   / 1000000));
	if(s->wall_time != -1)
		debug(D_DEBUG, "max resource %-18s  s: %lf\n", "wall_time", ((double) s->wall_time / 1000000));
	if(s->max_concurrent_processes != -1)
		debug(D_DEBUG, "max resource %-18s   : %" PRId64 "\n", "max_processes_processes", s->max_concurrent_processes);
	if(s->total_processes != -1)
		debug(D_DEBUG, "max resource %-18s   : %" PRId64 "\n", "total_processes", s->total_processes);
	if(s->cpu_time != -1)
		debug(D_DEBUG, "max resource %-18s  s: %lf\n", "cpu_time",  ((double) s->cpu_time  / 1000000));
	if(s->virtual_memory != -1)
		debug(D_DEBUG, "max resource %-18s MB: %" PRId64 "\n", "virtual_memory", s->virtual_memory);
	if(s->memory != -1)
		debug(D_DEBUG, "max resource %-18s MB: %" PRId64 "\n", "memory", s->memory);
	if(s->swap_memory != -1)
		debug(D_DEBUG, "max resource %-18s MB: %" PRId64 "\n", "swap_memory", s->swap_memory);
	if(s->bytes_read != -1)
		debug(D_DEBUG, "max resource %-18s B: %" PRId64 "\n", "bytes_read", s->bytes_read);
	if(s->bytes_written != -1)
		debug(D_DEBUG, "max resource %-18s MB: %" PRId64 "\n", "bytes_written", s->bytes_written);
	if(s->bytes_received != -1)
		debug(D_DEBUG, "max resource %-18s MB: %" PRId64 "\n", "bytes_received", s->bytes_received);
	if(s->bytes_sent != -1)
		debug(D_DEBUG, "max resource %-18s MB: %" PRId64 "\n", "bytes_sent", s->bytes_sent);
	if(s->bandwidth != -1)
		debug(D_DEBUG, "max resource %-18s bps: %" PRId64 "\n", "bandwidth", s->bandwidth);
	if(s->total_files != -1)
		debug(D_DEBUG, "max resource %-18s   : %" PRId64 "\n", "total_files", s->total_files);
	if(s->disk != -1)
		debug(D_DEBUG, "max resource %-18s MB: %" PRId64 "\n", "disk", s->disk);
}

/* vim: set noexpandtab tabstop=4: */
