/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <float.h>
#include <omp.h>

#include "resource_monitor_tools.h"

#include "category_internal.h"
#include "create_dir.h"
#include "copy_stream.h"
#include "jx.h"
#include "jx_pretty_print.h"
#include "macros.h"
#include "timestamp.h"

#define MAX_LINE 1024

#define OUTLIER_DIR "outliers"
#define OUTLIER_N    5

#define MAX_P        1.00

#define value_at_index(h, idx) (rmsummary_get_int_field((h)->summaries_sorted[(idx)], (h)->field))

const char *field_order[] = {
	"cores",
	"cores_avg",
	"disk",
	"memory",
	"virtual_memory",
	"swap_memory",
	"wall_time",
	"cpu_time",
	"bytes_read",
	"bytes_written",
	"bytes_received",
	"bytes_sent",
	"bandwidth",
	"total_files",
	"max_concurrent_processes",
	"total_processes",
	NULL
};

int gnuplots_running = 0;

static int width  = 900;
static int height = 600;

static int width_thumb  = 372; //186;
static int height_thumb = 248; //124;

char *format       = "png";
char *gnuplot_path = "gnuplot";

int webpage_mode   = 1;

char *output_directory = NULL;

uint64_t input_overhead;

struct allocation {
	int64_t first;
	double waste;
	uint64_t committed;
	double throughput;
	double tasks_done;
	double time_taken;
	int    retries;

	uint64_t overhead;
};

struct field_stats {
	const char *field;
	size_t offset;

	struct histogram *histogram;

	struct rmsummary_set *source;
	struct rmsummary **summaries_sorted;
	int    total_count;

	double mean;
	double variance;

	struct allocation fa_perfect;
	struct allocation fa_max;
	struct allocation fa_95;
	struct allocation fa_min_waste_time_dependence;
	struct allocation fa_min_waste_time_independence;
	struct allocation fa_min_waste_brute_force;
	struct allocation fa_max_throughput;
	struct allocation fa_max_throughput_brute_force;

	uint64_t usage;

	char *output_directory;
};

struct list *all_sets;

struct rmsummary_set *all_summaries;

struct hash_table *categories;

int brute_force = 0;

void split_summaries_on_category(struct rmsummary_set *source)
{
	debug(D_RMON, "Splitting categories.");

	struct hash_table *splits = hash_table_create(0, 0);

	struct rmsummary *s;
	struct rmsummary_set *bucket;

	list_first_item(source->summaries);
	while((s = list_next_item(source->summaries)))
	{
		bucket = hash_table_lookup(splits, s->category);

		struct category *c = category_lookup_or_create(categories, s->category);
		category_accumulate_summary(c, s, NULL);

		if(!bucket)
		{
			bucket = make_new_set(s->category);
			hash_table_insert(splits, s->category, bucket);
			list_push_tail(all_sets, bucket);
		}

		list_push_tail(bucket->summaries, s);
	}

	hash_table_delete(splits);
}

static size_t sort_field_offset;
int less_than(const void *a, const void *b)
{
	struct rmsummary *sa = * (struct rmsummary * const *) a;
	struct rmsummary *sb = * (struct rmsummary * const *) b;

	int64_t fa = rmsummary_get_int_field_by_offset(sa, sort_field_offset);
	int64_t fb = rmsummary_get_int_field_by_offset(sb, sort_field_offset);

	return (fa > fb);
}

void sort_by_field(struct field_stats *h, const char *field) {
	sort_field_offset = rmsummary_field_offset(field);

	qsort(h->summaries_sorted, h->total_count, sizeof(struct rmsummary *), less_than);
}

int index_of_p(struct field_stats *h, double p)
{
	return (int) ceil((h->total_count - 1) * p);
}

double value_of_p(struct field_stats *h, double p)
{
	return value_at_index(h, index_of_p(h, p));
}

double bucket_size_by_iqr(struct field_stats *h)
{
	int64_t v_25 = value_of_p(h, 0.25);
	int64_t v_75 = value_of_p(h, 0.75);

	double bin_size = 1;

	if(v_75 > v_25) {
		bin_size = 2*(v_75 - v_25)*pow((double) h->total_count, (-1.0/3.0));
	}

	return bin_size;
}

double set_average_of_field(struct field_stats *h) {
	double accum  = 0;
	size_t offset = rmsummary_field_offset(h->field);

	int i;
	for(i = 0; i < h->total_count; i++) {
		accum += rmsummary_get_int_field_by_offset(h->summaries_sorted[i], offset);
	}

	h->mean = accum/h->total_count;

	return h->mean;
}

double set_variance_of_field(struct field_stats *h) {
	double accum  = 0;
	size_t offset = rmsummary_field_offset(h->field);

	int i;
	for(i = 0; i < h->total_count; i++)
		accum += pow(rmsummary_get_int_field_by_offset(h->summaries_sorted[i], offset) - h->mean, 2);

	if(h->total_count > 1)
	{
		h->variance = accum/(h->total_count - 1);
	}
	else
	{
		h->variance = -1;
	}

	return h->variance;
}

char *path_common(struct field_stats *h, int only_base_name)
{
	char *category = sanitize_path_name(h->source->category_name);

	char *prefix;
	if(only_base_name)
	{
		prefix = "";
	}
	else
	{
		prefix = h->output_directory;
	}

	char *path = string_format("%s%s_%s", prefix, category, h->field);

	free(category);

	return path;
}

char *path_of_table(struct field_stats *h, int only_base_name)
{
	char *common = path_common(h, only_base_name);
	char *path   = string_format("%s_table.data", common);

	free(common);

	return path;
}

char *path_of_variables_script(struct field_stats *h, int only_base_name)
{
	char *common = path_common(h, only_base_name);
	char *path   = string_format("%s_vars.gnuplot", common);
	free(common);

	return path;
}

char *path_of_thumbnail_script(struct field_stats *h, int only_base_name)
{
	char *common = path_common(h, only_base_name);
	char *path   = string_format("%s_%dx%d.gnuplot", common, width_thumb, height_thumb);
	free(common);

	return path;
}

char *path_of_thumbnail_image(struct field_stats *h, int only_base_name)
{
	char *common = path_common(h, only_base_name);
	char *path   = string_format("%s_%dx%d.%s", common, width_thumb, height_thumb, format);
	free(common);

	return path;
}

char *path_of_image_script(struct field_stats *h, int only_base_name)
{
	char *common = path_common(h, only_base_name);
	char *path   = string_format("%s_%dx%d.gnuplot", common, width, height);
	free(common);

	return path;
}

char *path_of_image(struct field_stats *h, int only_base_name)
{
	char *common = path_common(h, only_base_name);
	char *path   = string_format("%s_%dx%d.%s", common, width, height, format);
	free(common);

	return path;
}

char *path_of_page(struct field_stats *h, int only_base_name)
{
	char *common = path_common(h, only_base_name);
	char *path   = string_format("%s.html", common);
	free(common);

	return path;
}


void create_output_directory(struct field_stats *h)
{
	char *category = sanitize_path_name(h->source->category_name);
	char *all_path = string_format("%s/%s/", output_directory, category);

	if(create_dir(all_path, 0755) < 0 && errno != EEXIST)
		fatal("Could not create directory: %s\n", all_path);

	h->output_directory = all_path;

	free(category);
}

FILE *open_file(char *filename)
{
	FILE *file = fopen(filename, "w");
	if(!file)
		fatal("Could not open file for writing: %s\n", filename);

	return file;
}

void write_histogram_table(struct field_stats *h)
{
	char *fname = path_of_table(h, 0);
	FILE *f     = open_file(fname);
	free(fname);

	double *buckets = histogram_buckets(h->histogram);

	int i;
	for(i = 0; i < histogram_size(h->histogram); i++) {
		int count = histogram_count(h->histogram, buckets[i]);
		fprintf(f, "%lf %d\n", rmsummary_to_external_unit(h->field, buckets[i]), count);
	}

	fclose(f);
}

void write_variables_gnuplot(struct field_stats *h, struct field_stats *all)
{
	char *fname = path_of_variables_script(h, 0);
	FILE *f     = open_file(fname);
	free(fname);

	fprintf(f, "%s = %d\n",  "current_buckets",    histogram_size(h->histogram));
	fprintf(f, "%s = %lf\n", "current_minimum",    floor(rmsummary_to_external_unit(h->field, histogram_min_value(h->histogram))));
	fprintf(f, "%s = %lf\n", "current_maximum",    ceil(rmsummary_to_external_unit(h->field, histogram_max_value(h->histogram))));
	fprintf(f, "%s = %lf\n", "current_mode",       rmsummary_to_external_unit(h->field, histogram_mode(h->histogram)));
	fprintf(f, "%s = %d\n",  "current_mode_count", histogram_count(h->histogram, histogram_mode(h->histogram)));
	fprintf(f, "%s = %d\n",  "current_max_count",  histogram_count(h->histogram, histogram_max_value(h->histogram)));
	fprintf(f, "%s = %d\n",  "current_min_count",  histogram_count(h->histogram, histogram_min_value(h->histogram)));
	fprintf(f, "%s = %lf\n", "current_mean",       rmsummary_to_external_unit(h->field, h->mean));
	fprintf(f, "%s = %lf\n", "current_percentile75", rmsummary_to_external_unit(h->field, value_of_p(h, 0.75)));
	fprintf(f, "%s = %lf\n", "current_percentile25", rmsummary_to_external_unit(h->field, value_of_p(h, 0.25)));

	fprintf(f, "%s = %lf\n", "current_first_allocation", rmsummary_to_external_unit(h->field, h->fa_max_throughput.first));
	fprintf(f, "%s = %lf\n", "current_first_allocation_min_waste", rmsummary_to_external_unit(h->field, h->fa_min_waste_time_dependence.first));

	fprintf(f, "%s = %lf\n", "current_bin_size",   rmsummary_to_external_unit(h->field, histogram_bucket_size(h->histogram)));

	if(all) {

		fprintf(f, "%s = %lf\n", "all_minimum",    floor(rmsummary_to_external_unit(h->field, histogram_min_value(all->histogram))));
		fprintf(f, "%s = %lf\n", "all_maximum",    ceil(rmsummary_to_external_unit(h->field, histogram_max_value(all->histogram))));
		fprintf(f, "%s = %lf\n", "all_mode",       rmsummary_to_external_unit(h->field, histogram_mode(all->histogram)));
		fprintf(f, "%s = %d\n",  "all_mode_count", histogram_count(all->histogram, histogram_mode(all->histogram)));
		fprintf(f, "%s = %lf\n", "all_mean",         rmsummary_to_external_unit(h->field, all->mean));
		fprintf(f, "%s = %lf\n", "all_percentile75", rmsummary_to_external_unit(h->field, value_of_p(h, 0.75)));
		fprintf(f, "%s = %lf\n", "all_percentile25", rmsummary_to_external_unit(h->field, value_of_p(h, 0.25)));

		fprintf(f, "%s = %lf\n", "all_first_allocation", rmsummary_to_external_unit(h->field, all->fa_max_throughput.first));

		fprintf(f, "%s = %lf\n", "all_first_allocation_min_waste", rmsummary_to_external_unit(h->field, all->fa_min_waste_time_dependence.first));
	}

	fclose(f);
}

void write_thumbnail_gnuplot(struct field_stats *h, struct field_stats *all)
{
	char *fname = path_of_thumbnail_script(h, 0);
	FILE *f     = open_file(fname);
	free(fname);

	fname = path_of_variables_script(h, 1);
	fprintf(f, "load \"%s\"\n", fname);
	free(fname);

	fprintf(f, "set terminal pngcairo truecolor rounded size %d,%d enhanced font \"times,10\"\n",
			width_thumb, height_thumb);

	fname = path_of_thumbnail_image(h, 1);
	fprintf(f, "set output \"%s\"\n", fname);
	free(fname);

	fprintf(f, "unset key\n");
	fprintf(f, "unset border\n");
	fprintf(f, "set style line 1 lc 16\n");
	fprintf(f, "set style fill solid noborder\n");
	fprintf(f, "set tmargin 2\n");
	fprintf(f, "set bmargin 2\n");
	fprintf(f, "unset tics\n");

	fprintf(f, "set arrow from current_minimum,graph -0.01 to current_percentile25,graph -0.01 nohead lc 16\n");
	fprintf(f, "set arrow from current_percentile75,graph -0.01 to current_maximum,graph -0.01 nohead lc 16\n");

	/* square for mean */
	fprintf(f, "set label \"\" at current_mean,graph 0.00 tc ls 1 center front point pt 4\n");

	/* up triangle for mode */
	fprintf(f, "set label sprintf(\"%%.0f\", current_mode) at current_mode,graph -0.05 tc ls 1 center front point pt 8 offset 0,character -0.90\n");

	/* down triangle for first allocation */
	fprintf(f, "set label \"\" at current_first_allocation,graph -0.025 tc ls 1 center front point pt 10\n");

	fprintf(f, "set label sprintf(\"%%.0f\", current_minimum) at current_minimum,graph -0.01 tc ls 1 right front nopoint offset character -1.0,character -0.25\n");
	fprintf(f, "set label sprintf(\"%%.0f\", current_maximum) at current_maximum,graph -0.01 tc ls 1 left front nopoint offset character 1.0,character -0.25\n");

	if( histogram_size(all->histogram) == 1 )
	{
		fprintf(f, "set boxwidth 1.0*(all_maximum - all_minimum + 1)/50 absolute\n");
		fprintf(f, "set xrange [all_minimum - 1 : all_maximum + 2]\n");
	}
	else
	{
		fprintf(f, "gap = (all_maximum - all_minimum)/5.0\n");
		fprintf(f, "set boxwidth (0.1 > current_bin_size ? 0.1 : current_bin_size) absolute\n");
		fprintf(f, "set xrange [all_minimum - gap : all_maximum + gap]\n");
	}

	char *table_name = path_of_table(h, 1);

	if(histogram_max_value(all->histogram) > 10000*histogram_min_value(all->histogram))
	{
		fprintf(f, "set yrange [0:(log10(all_mode_count))]\n");
		fprintf(f, "set label sprintf(\"log(%%d)\",current_mode_count) at current_mode,(log10(current_mode_count)) tc ls 1 left front nopoint offset 0,character 0.5\n");

		fprintf(f, "plot \"%s\" using 1:(log10($2)) w boxes\n", table_name);
	}
	else
	{
		fprintf(f, "set yrange [0:all_mode_count]\n");
		fprintf(f, "set label sprintf(\"%%d\", current_mode_count) at current_mode,current_mode_count tc ls 1 left front nopoint offset 0,character 0.5\n");

		fprintf(f, "plot \"%s\" using 1:2 w boxes\n", table_name);
	}

	free(table_name);

	fprintf(f, "\n");
	fclose(f);
}


void write_image_gnuplot(struct field_stats *h, struct field_stats *all)
{
	char *fname = path_of_image_script(h, 0);
	FILE *f     = open_file(fname);
	free(fname);

	fname = path_of_variables_script(h, 1);
	fprintf(f, "load \"%s\"\n", fname);
	free(fname);

	fprintf(f, "set terminal pngcairo truecolor rounded size %d,%d enhanced font \"times,12\"\n",
			width, height);

	fname = path_of_image(h, 1);
	fprintf(f, "set output \"%s\"\n", fname);
	free(fname);

	fprintf(f, "unset key\n");
	fprintf(f, "unset border\n");
	fprintf(f, "set style line 1 lc 16\n");
	fprintf(f, "set style fill solid noborder\n");
	fprintf(f, "set tmargin 2\n");
	fprintf(f, "set bmargin 2\n");
	fprintf(f, "unset tics\n");

	fprintf(f, "set arrow from current_minimum,graph -0.01 to current_percentile25,graph -0.01 nohead lc 16\n");
	fprintf(f, "set arrow from current_percentile75,graph -0.01 to current_maximum,graph -0.01 nohead lc 16\n");

	/* square for mean */
	fprintf(f, "set label \"\" at current_mean,graph -0.00 tc ls 1 center front point pt 4\n");

	/* up triangle for mode */
	fprintf(f, "set label sprintf(\"%%.0f\", current_mode) at current_mode,graph -0.05 tc ls 1 center front point pt 8 offset 0,character -0.90\n");

	/* down triangle for first allocation */
	fprintf(f, "set label \"\" at current_first_allocation,graph -0.025 tc ls 1 center front point pt 10\n");

	fprintf(f, "set label sprintf(\"%%.0f\", all_minimum) at all_minimum,graph -0.01 tc ls 1 right front nopoint offset character -1.0,character -0.25\n");
	fprintf(f, "set label sprintf(\"%%.0f\", all_maximum) at all_maximum,graph -0.01 tc ls 1 left  front nopoint offset character  1.0,character -0.25\n");

	if( histogram_size(all->histogram) )
	{
		fprintf(f, "set boxwidth (all_maximum - all_minimum + 1)/50 absolute\n");
		fprintf(f, "set xrange [all_minimum - 1 : all_maximum + 2]\n");
	}
	else
	{
		fprintf(f, "gap = (all_maximum - all_minimum)/5.0\n");
		fprintf(f, "set boxwidth (0.1 > current_bin_size ? 0.1 : current_bin_size) absolute\n");
		fprintf(f, "set xrange [all_minimum - gap : all_maximum + gap]\n");
	}

	char *table_name = path_of_table(h, 1);
	if(histogram_max_value(h->histogram) > 10000*histogram_min_value(h->histogram))
	{
		fprintf(f, "set yrange [0:(log10(all_mode_count))]\n");
		fprintf(f, "set label sprintf(\"log(%%d)\",current_mode_count) at current_mode,(log10(current_mode_count)) tc ls 1 left front nopoint offset 0,character 0.5\n");

		fprintf(f, "plot \"%s\" using 1:(log10($2)) w boxes\n", table_name);
	}
	else
	{
		fprintf(f, "set yrange [0:all_mode_count]\n");
		fprintf(f, "set label sprintf(\"%%d\", current_mode_count) at current_mode,current_mode_count tc ls 1 left front nopoint offset 0,character 0.5\n");

		fprintf(f, "plot \"%s\" using 1:2 w boxes\n", table_name);
	}

	free(table_name);

	fprintf(f, "\n");
	fclose(f);
}

void write_images(struct field_stats *h)
{
	pid_t pid;

	pid = fork();
	if(pid < 0)
	{
		fatal("Could not fork when creating thumbnail: %s\n", path_of_thumbnail_image(h, 0));
	}

	if(pid == 0) {
		char *path = string_format("%s/%s", output_directory, sanitize_path_name(h->source->category_name));
		if(chdir(path) == 0) {
			execlp(gnuplot_path, "gnuplot", path_of_thumbnail_script(h, 1), NULL);
		}

		fatal("Could not exec when creating thumbnail: %s\n", path_of_thumbnail_image(h, 0));
	}

	pid = fork();
	if(pid < 0)
	{
		fatal("Could not fork when creating image: %s\n", path_of_image(h, 0));
	}

	if(pid == 0) {
		char *path = string_format("%s/%s", output_directory, sanitize_path_name(h->source->category_name));
		if(chdir(path) == 0) {
			execlp(gnuplot_path, "gnuplot", path_of_image_script(h, 1), NULL);
		}
		fatal("Could not exec when creating image: %s\n", path_of_image(h, 0));
	}

	gnuplots_running += 2;
}

struct field_stats *histogram_of_field(struct rmsummary_set *source, const char *field, char *out_dir)
{
	struct field_stats *h = malloc(sizeof(struct field_stats));
	struct rmsummary *s;

	h->field       = field;
	h->source      = source;
	h->offset      = rmsummary_field_offset(field);

	h->summaries_sorted = malloc(list_size(source->summaries) * sizeof(struct rmsummary *));

	h->total_count = 0;
	list_first_item(source->summaries);
	while((s = list_next_item(source->summaries)))
	{
		double value = rmsummary_get_int_field_by_offset(s, h->offset);
		if(value >= 0) {
			h->summaries_sorted[h->total_count] = s;
			h->total_count++;
		}
	}

	h->summaries_sorted = realloc(h->summaries_sorted, h->total_count * sizeof(struct rmsummary *));
	h->histogram        = histogram_create(bucket_size_by_iqr(h));

	list_first_item(source->summaries);
	while((s = list_next_item(source->summaries)))
	{
		// if a value is negative, skip it.
		double value = rmsummary_get_int_field_by_offset(s, h->offset);
		if(value >= 0) {
			histogram_insert(h->histogram, value);
		}
	}

	sort_by_field(h, field);
	create_output_directory(h);

	set_average_of_field(h);
	set_variance_of_field(h);

	hash_table_insert(source->stats, field, (void *) h);

	debug(D_RMON, "%s-%s:\n buckets: %d bin_size: %lf max_count: %d mode: %.0lf\n",
		h->source->category_name,
		h->field,
		histogram_size(h->histogram),
		histogram_bucket_size(h->histogram),
		histogram_count(h->histogram, histogram_mode(h->histogram)),
		histogram_mode(h->histogram));

	return h;
}

struct jx *allocation_to_json(struct field_stats *h, struct allocation *alloc) {

	struct jx *j = jx_object(NULL);

	jx_insert_double(j, "allocation", rmsummary_to_external_unit(h->field, alloc->first));
	jx_insert_double(j, "waste",      alloc->waste);
	jx_insert_double(j, "throughput", alloc->throughput);
	jx_insert_double(j, "retries",    alloc->retries);
	jx_insert_double(j, "time_taken", rmsummary_to_external_unit("wall_time", alloc->time_taken));
	jx_insert_double(j, "committed",  alloc->committed);
	jx_insert_double(j, "usage",      h->usage);
	jx_insert_double(j, "tasks_done", alloc->tasks_done);

	return j;
}

struct jx *field_to_json(struct field_stats *h)
{
	struct jx *j = jx_object(NULL);

	jx_insert_string(j, "units",    rmsummary_unit_of(h->field));

	jx_insert_double(j,  "mean", h->mean);
	jx_insert_double(j,  "std-dev", sqrt(h->variance));
	jx_insert_double(j,  "min", histogram_min_value(h->histogram));
	jx_insert_double(j, "usage", h->usage);

	struct jx *policies = jx_object(NULL);

	jx_insert(policies, jx_string("perfect"), allocation_to_json(h, &(h->fa_perfect)));
	jx_insert(policies, jx_string("maximum"), allocation_to_json(h, &(h->fa_max)));
	jx_insert(policies, jx_string("P95"),     allocation_to_json(h, &(h->fa_95)));

	if(brute_force) {
		jx_insert(policies, jx_string("min_waste_brute_force"),      allocation_to_json(h, &(h->fa_min_waste_brute_force)));
		jx_insert(policies, jx_string("max_throughput_brute_force"), allocation_to_json(h, &(h->fa_max_throughput_brute_force)));
	}

	jx_insert(policies, jx_string("min_waste"),       allocation_to_json(h, &(h->fa_min_waste_time_dependence)));
	jx_insert(policies, jx_string("min_waste_naive"), allocation_to_json(h, &(h->fa_min_waste_time_independence)));
	jx_insert(policies, jx_string("max_throughput"),  allocation_to_json(h, &(h->fa_max_throughput)));

	jx_insert(j, jx_string("policies"), policies);

	return j;
}

#define LOOP_FIELD(field) {int ifc = 0; const char *field; for(field = field_order[ifc]; field; ifc++, field = field_order[ifc])
#define LOOP_END }

void histograms_of_category(struct rmsummary_set *ss)
{
	/* construct field_stats of category across all resources */
	LOOP_FIELD(field_name) {
		if(!field_is_active(field_name)) {
			continue;
		}
		debug(D_RMON, "Computing histogram of %s.%s", ss->category_name, field_name);
		histogram_of_field(ss, field_name, output_directory);

	} LOOP_END

}

void plots_of_category(struct rmsummary_set *s)
{
	struct field_stats     *h;

	/* construct field_statss of category across all resources */
	LOOP_FIELD(field_name) {
		if(!field_is_active(field_name)) {
			continue;
		}

		h = hash_table_lookup(s->stats, field_name);

		struct field_stats *all = hash_table_lookup(all_summaries->stats, field_name);
		if(!all)
			all = h;

		write_histogram_table(h);
		write_variables_gnuplot(h, all);
		write_thumbnail_gnuplot(h, all);
		write_image_gnuplot(h, all);

		write_images(h);
	} LOOP_END

	while(gnuplots_running)
	{
		wait(NULL);
		gnuplots_running--;
	}
}

double total_waste(struct field_stats *h, double first_alloc) {

	struct field_stats *all = hash_table_lookup(all_summaries->stats, h->field);
	if(!all) {
		all = h;
	}

	double waste   = 0;
	double max_candidate = value_of_p(all, 1.0);

	if(first_alloc < 0)
		return 0;

	int cumulative = field_is_cumulative(h->field);

	int i;
#pragma omp parallel for private(i) reduction(+: waste)
	for(i = 0; i < h->total_count; i+=1) {
		double current   = rmsummary_get_int_field_by_offset(h->summaries_sorted[i], h->offset);

		double wall_time;
		if(cumulative) {
			wall_time = 1;
		} else {
			wall_time = h->summaries_sorted[i]->wall_time;
		}

		double current_waste;
		if(current > first_alloc) {
			current_waste = (max_candidate - current + first_alloc)*wall_time;
		} else {
			current_waste = (first_alloc - current)*wall_time;
		}

		waste += current_waste;
	}

	waste = rmsummary_to_external_unit("wall_time", waste);
	waste = rmsummary_to_external_unit(h->field,   waste);

	return waste;
}

double total_committed(struct field_stats *h, double first_alloc) {

	struct field_stats *all = hash_table_lookup(all_summaries->stats, h->field);
	if(!all) {
		all = h;
	}

	double committed = 0;
	double max_allocation = value_of_p(all, 1.0);

	int i;
#pragma omp parallel for private(i) reduction(+: committed)
	for(i = 0; i < h->total_count; i+=1) {
		double current   = rmsummary_get_int_field_by_offset(h->summaries_sorted[i], h->offset);
		double wall_time = h->summaries_sorted[i]->wall_time;

		if(first_alloc > 0) {
			committed += first_alloc * wall_time;
			if(current > first_alloc) {
				committed += max_allocation * wall_time;
			}
		} else {
			/* for perfect case */
			committed += current * wall_time;
		}
	}

	committed = rmsummary_to_external_unit("wall_time", committed);
	committed = rmsummary_to_external_unit(h->field,   committed);

	return committed;
}

double total_usage(struct field_stats *h) {
	double usage   = 0;

	int cumulative = field_is_cumulative(h->field);

	int i;
#pragma omp parallel for private(i) reduction(+: usage)
	for(i = 0; i < h->total_count; i+=1) {
		double current = rmsummary_get_int_field_by_offset(h->summaries_sorted[i], h->offset);
		double wall_time;
		if(cumulative) {
			wall_time = 1;
		} else {
			wall_time = h->summaries_sorted[i]->wall_time;
		}

		usage += current * wall_time;
	}

	usage = rmsummary_to_external_unit("wall_time", usage);
	usage = rmsummary_to_external_unit(h->field,   usage);

	return usage;
}

void set_usage(struct rmsummary_set *s) {
	LOOP_FIELD(field_name) {
		if(!field_is_active(field_name)) {
			continue;
		}

		struct field_stats *h = hash_table_lookup(s->stats, field_name);
		h->usage = total_usage(h);
	} LOOP_END
}

double throughput(struct field_stats *h, double first_alloc, struct allocation *alloc) {

	if(alloc) {
		alloc->first      = first_alloc;
		alloc->tasks_done = 0;
		alloc->time_taken = 0;
		alloc->throughput = 0;
	}

	double tasks_accum     = 0;
	double wall_time_accum = 0;

	if(first_alloc == 0)
		return 0;

	struct field_stats *all = hash_table_lookup(all_summaries->stats, h->field);
	if(!all) {
		all = h;
	}
	double max_allocation  = value_of_p(all, 1.0);

	int i;
#pragma omp parallel for private(i) reduction(+: wall_time_accum, tasks_accum)
	for(i = 0; i < h->total_count; i+=1) {
		double current = rmsummary_get_int_field_by_offset(h->summaries_sorted[i], h->offset);

		if(current <= 0) {
			continue;
		}

		double wall_time = h->summaries_sorted[i]->wall_time;
		wall_time_accum += wall_time;

		double current_task;

		/* for perfect throughput */
		if(first_alloc < 0) {
			current_task = max_allocation / current;
		} else if(current > first_alloc) {
			current_task = 1;
			wall_time_accum += wall_time;
		} else {
			current_task = max_allocation / first_alloc;
		}

		tasks_accum += current_task;
	}

	double th;
	if(wall_time_accum > 0) {
		th = tasks_accum/rmsummary_to_external_unit("wall_time", wall_time_accum);
	} else {
		th = 0;
		tasks_accum = 0;
	}

	if(alloc) {
		alloc->throughput = th;
		alloc->tasks_done = tasks_accum;
		alloc->time_taken = wall_time_accum;
	}

	return th;
}

int retries(struct field_stats *h, double first_alloc) {
	int tasks_retried     = 0;

	int i;
#pragma omp parallel for private(i) reduction(+: tasks_retried)
	for(i = 0; i < h->total_count; i+=1) {
		double current = rmsummary_get_int_field_by_offset(h->summaries_sorted[i], h->offset);

		if(current > first_alloc) {
			tasks_retried += 1;
		}
	}

	return tasks_retried;
}

void set_category_maximum(struct rmsummary_set *s, struct hash_table *categories) {
	struct field_stats *h;

	struct category *c = category_lookup_or_create(categories, s->category_name);
	if(!c->max_allocation)
		c->max_allocation = rmsummary_create(-1);

	LOOP_FIELD(field_name) {
		if(!field_is_active(field_name)) {
			continue;
		}

		h = hash_table_lookup(s->stats, field_name);

		int64_t value = histogram_max_value(h->histogram);
		rmsummary_assign_int_field(c->max_allocation, h->field, value);
	} LOOP_END
}

void set_fa_values(struct field_stats *h, struct allocation *alloc, double first_allocation) {
	alloc->first      = first_allocation;
	alloc->committed  = total_committed(h, alloc->first);
	alloc->waste      = total_waste(h, alloc->first);
	alloc->throughput =  throughput(h, alloc->first, alloc);
	alloc->retries    =     retries(h, alloc->first);

}

void set_fa_min_waste_time_dependence(struct rmsummary_set *s, struct hash_table *categories) {
	struct category *c = category_lookup_or_create(categories, s->category_name);
	c->time_peak_independece = 0;
	c->allocation_mode       = CATEGORY_ALLOCATION_MODE_MIN_WASTE;

	category_update_first_allocation(c, NULL);

	if(!c->first_allocation)
		return;

	LOOP_FIELD(field_name) {
		if(!field_is_active(field_name)) {
			continue;
		}

		struct field_stats *h = hash_table_lookup(s->stats, field_name);
		set_fa_values(h, &(h->fa_min_waste_time_dependence), rmsummary_get_int_field(c->first_allocation, field_name));

		debug(D_RMON, "first allocation '%s' min waster: %" PRId64, h->field,  rmsummary_get_int_field(c->first_allocation, h->field));
	} LOOP_END
}

void set_fa_min_waste_time_independence(struct rmsummary_set *s, struct hash_table *categories) {
	struct category *c = category_lookup_or_create(categories, s->category_name);
	c->time_peak_independece = 1;
	c->allocation_mode       = CATEGORY_ALLOCATION_MODE_MIN_WASTE;

	category_update_first_allocation(c, NULL);

	if(!c->first_allocation)
		return;

	LOOP_FIELD(field_name) {
		if(!field_is_active(field_name)) {
			continue;
		}

		struct field_stats *h = hash_table_lookup(s->stats, field_name);
		set_fa_values(h, &(h->fa_min_waste_time_independence), rmsummary_get_int_field(c->first_allocation, h->field));

	} LOOP_END
}

void set_fa_max_throughput(struct rmsummary_set *s, struct hash_table *categories) {
	struct category *c = category_lookup_or_create(categories, s->category_name);
	c->time_peak_independece = 0;
	c->allocation_mode       = CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT;

	category_update_first_allocation(c, NULL);

	if(!c->first_allocation)
		return;

	LOOP_FIELD(field_name) {
		if(!field_is_active(field_name)) {
			continue;
		}

		struct field_stats *h = hash_table_lookup(s->stats, field_name);
		set_fa_values(h, &(h->fa_max_throughput), rmsummary_get_int_field(c->first_allocation, h->field));

		debug(D_RMON, "first allocation '%s' max throughput: %" PRId64, h->field,  rmsummary_get_int_field(c->first_allocation, h->field));
	} LOOP_END

}

void set_fa_95(struct rmsummary_set *s, struct hash_table *categories) {

	LOOP_FIELD(field_name) {
		if(!field_is_active(field_name)) {
			continue;
		}

		struct field_stats *h = hash_table_lookup(s->stats, field_name);
		set_fa_values(h, &(h->fa_95), value_of_p(h, 0.95));
	} LOOP_END
}

void set_fa_max(struct rmsummary_set *s, struct hash_table *categories) {

	LOOP_FIELD(field_name) {
		if(!field_is_active(field_name)) {
			continue;
		}

		struct field_stats *h = hash_table_lookup(s->stats, field_name);
		set_fa_values(h, &(h->fa_max), value_of_p(h, 1.0));
	} LOOP_END
}

void set_fa_perfect(struct rmsummary_set *s, struct hash_table *categories) {

	LOOP_FIELD(field_name) {
		if(!field_is_active(field_name)) {
			continue;
		}

		struct field_stats *h = hash_table_lookup(s->stats, field_name);

		set_fa_values(h, &(h->fa_perfect), -1);

		h->fa_perfect.first      = -1;
		h->fa_perfect.waste      =  0;
		h->fa_perfect.retries    =  0;
	} LOOP_END
}

int64_t min_waste_brute_force_field(struct field_stats *h) {
	uint64_t max           = value_of_p(h, 1.0);
	double   min_waste     = total_waste(h, max);

	uint64_t min_candidate = max;

	uint64_t step = category_get_bucket_size(h->field);

	for(uint64_t i = step; i < max; i+=step) {
		double candidate_waste = total_waste(h, i);

		if(candidate_waste < min_waste) {
			min_candidate = i;
			min_waste     = candidate_waste;
		}
	}

	debug(D_RMON, "first allocation '%s' brute force min waste: %" PRId64, h->field, min_candidate);

	return min_candidate;
}

void set_fa_min_waste_brute_force(struct rmsummary_set *s, struct hash_table *categories) {

	LOOP_FIELD(field_name) {
		if(!field_is_active(field_name)) {
			continue;
		}

		struct field_stats *h = hash_table_lookup(s->stats, field_name);

		if(brute_force) {
			set_fa_values(h, &(h->fa_min_waste_brute_force), min_waste_brute_force_field(h));
		}
	} LOOP_END
}

int64_t max_throughput_brute_force_field(struct field_stats *h) {
	uint64_t max = value_of_p(h, 1.0);
	double   best_throughput = throughput(h, max, NULL);

	uint64_t max_candidate = max;

	uint64_t step = category_get_bucket_size(h->field);

	for(uint64_t i = step; i < max; i+=step) {

		double candidate_throughput = throughput(h, i, NULL);

		if(candidate_throughput > best_throughput) {
			max_candidate  = i;
			best_throughput = candidate_throughput;
		}
	}

	debug(D_RMON, "first allocation '%s' brute force throughput max: %" PRId64, h->field, max_candidate);

	return max_candidate;
}


void set_fa_max_throughput_brute_force(struct rmsummary_set *s, struct hash_table *categories) {

	LOOP_FIELD(field_name) {
		if(!field_is_active(field_name)) {
			continue;
		}

		struct field_stats *h = hash_table_lookup(s->stats, field_name);
		if(brute_force) {
			set_fa_values(h, &(h->fa_max_throughput_brute_force), max_throughput_brute_force_field(h));
		}
	} LOOP_END
}

void set_first_allocations_of_category(struct rmsummary_set *s, struct hash_table *categories) {

	/* activate first allocation for all resources. */
	char *name;
	struct category *c;
	hash_table_firstkey(categories);
	while(hash_table_nextkey(categories, &name, (void *) &c)) {
		rmsummary_delete(c->autolabel_resource);
		c->autolabel_resource = rmsummary_create(1);
	}

	set_category_maximum(s, categories);

	s->overhead_min_waste_time_dependence = timestamp_get();
	set_fa_min_waste_time_dependence(s, categories);
	s->overhead_min_waste_time_dependence = timestamp_get() - s->overhead_min_waste_time_dependence;

	s->overhead_min_waste_time_independence = timestamp_get();
	set_fa_min_waste_time_independence(s, categories);
	s->overhead_min_waste_time_independence = timestamp_get() - s->overhead_min_waste_time_independence;

	s->overhead_min_waste_brute_force = timestamp_get();
	set_fa_min_waste_brute_force(s, categories);
	s->overhead_min_waste_brute_force = timestamp_get() - s->overhead_min_waste_brute_force;

	s->overhead_max_throughput = timestamp_get();
	set_fa_max_throughput(s, categories);
	s->overhead_max_throughput = timestamp_get() - s->overhead_max_throughput;

	s->overhead_max_throughput_brute_force = timestamp_get();
	set_fa_max_throughput_brute_force(s, categories);
	s->overhead_max_throughput_brute_force = timestamp_get() - s->overhead_max_throughput_brute_force;

	set_fa_perfect(s, categories);
	set_fa_95(s, categories);
	set_fa_max(s, categories);
}

struct jx *overheads_to_json(struct rmsummary_set *s)
{
	debug(D_RMON, "Writing overheads for %s", s->category_name);

	struct jx *j = jx_object(NULL);

	jx_insert_double(j, "input", rmsummary_to_external_unit("wall_time", input_overhead));

	if(brute_force) {
		jx_insert_double(j, "min_waste_brute_force",      rmsummary_to_external_unit("wall_time", s->overhead_min_waste_brute_force));
		jx_insert_double(j, "max_throughput_brute_force", rmsummary_to_external_unit("wall_time", s->overhead_max_throughput_brute_force));
	}

	jx_insert_double(j, "min_waste",       rmsummary_to_external_unit("wall_time", s->overhead_min_waste_time_independence));
	jx_insert_double(j, "min_waste_naive", rmsummary_to_external_unit("wall_time", s->overhead_min_waste_time_dependence));
	jx_insert_double(j, "max_throughput",  rmsummary_to_external_unit("wall_time", s->overhead_max_throughput));

	return j;
}



struct jx *category_to_json(struct rmsummary_set *s)
{
	struct jx *j = jx_object(NULL);

	debug(D_RMON, "Writing stats for %s", s->category_name);

	jx_insert_integer(j, "count", list_size(s->summaries));

	struct jx *resources = jx_object(NULL);

	LOOP_FIELD(field_name) {
		if(!field_is_active(field_name)) {
			continue;
		}

		struct field_stats *h = hash_table_lookup(s->stats, field_name);

		jx_insert(resources, jx_string(field_name), field_to_json(h));

	} LOOP_END

	jx_insert(j, jx_string("resources"), resources);
	jx_insert(j, jx_string("overheads"),  overheads_to_json(s));

	return j;
}

char *copy_outlier(struct rmsummary *s) {
	static int count = 0;
	count++;

	char *base = string_format("outlier-%d.summary", count);
	char *outlier = string_format("%s/%s/%s", output_directory, OUTLIER_DIR, base);

	char dir[PATH_MAX];
	path_dirname(outlier, dir);
	create_dir(dir, S_IRWXU);

	FILE *output = fopen(outlier, "w");
	if(output) {
		rmsummary_print(output, s, 1, NULL);
		fclose(output);
	} else {
		debug(D_NOTICE, "Could not create outlier summary: %s\n", outlier);
		outlier = NULL;
	}

	free(outlier);

	return base;
}


void write_outlier(FILE *stream, struct rmsummary *s, const char *field, char *prefix) {
	char *outlier_name;

	outlier_name = copy_outlier(s);

	if(!outlier_name)
		return;

	if(!prefix)
	{
		prefix = "";
	}

	fprintf(stream, "<td class=\"data\">\n");
	fprintf(stream, "<a href=%s%s/%s>(%s)</a>", prefix, OUTLIER_DIR, outlier_name, s->taskid);
	fprintf(stream, "<br><br>\n");

	fprintf(stream, "%6.0lf\n", rmsummary_to_external_unit(field, rmsummary_get_int_field(s, field)));
	fprintf(stream, "</td>\n");
}

void write_css_style(FILE *stream)
{
	fprintf(stream,
			"\n<style media=\"screen\" type=\"text/css\">\n"
			"table { font-size: small; border-collapse: collapse; }\n"
			"td    { text-align: right; padding: 5px; border: 1px solid rgb(216,216,216); }\n"
			"td.datahdr { text-align: center; border-top: 0; }\n"
			"td.task    { text-align: left;   border-right: 0; }\n"
			"td.data    { text-align: center;  border-left:  0; }\n"
			"\n</style>\n"
		   );

}

void write_webpage_stats_header(FILE *stream, struct field_stats *h)
{
	fprintf(stream, "<td class=\"data\">%s", h->field);
	fprintf(stream, " (%s)", rmsummary_unit_of(h->field));
	fprintf(stream, "</td>");

	fprintf(stream, "<td class=\"datahdr\" >mode <br> &#9653;</td>");
	fprintf(stream, "<td class=\"datahdr\" >&mu; <br> &#9643; </td>");
	fprintf(stream, "<td class=\"datahdr\" >(&mu;+&sigma;)/&mu;</td>");

	fprintf(stream, "<td class=\"datahdr\" >1<sup>st</sup> alloc. max value<br> &#9663; </td>");
	fprintf(stream, "<td class=\"datahdr\" >1<sup>st</sup> alloc. max through<br> &#9663; </td>");
	fprintf(stream, "<td class=\"datahdr\" >1<sup>st</sup> alloc. min waste </td>");

	if(brute_force) {
		fprintf(stream, "<td class=\"datahdr\" >1<sup>st</sup> alloc. b.f. m.t.</td>");
		fprintf(stream, "<td class=\"datahdr\" >1<sup>st</sup> alloc. b.f. m.w. </td>");
	}
}

void write_webpage_stats(FILE *stream, struct field_stats *h, char *prefix, int include_thumbnail)
{
	fprintf(stream, "<td>");
	if(include_thumbnail)
	{
		fprintf(stream, "<a href=\"../%s\">",  path_of_page(h, 0));
		fprintf(stream, "<img src=\"../%s\">", path_of_thumbnail_image(h, 0));
		fprintf(stream, "</a>");
	}
	fprintf(stream, "</td>");

	const char *fmt       = (rmsummary_field_is_float(h->field) ? "%.3lf\n" : "%.0lf\n");
	const char *fmt_alloc = (rmsummary_field_is_float(h->field) ? "alloc:&nbsp;%.3lf\n" : "alloc:&nbsp;%.0lf\n");
	const char *fmt_stats = "throu:&nbsp;%.2lf waste:&nbsp;%.0lf%%\n";

	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, fmt, rmsummary_to_external_unit(h->field, histogram_mode(h->histogram)));
	fprintf(stream, "</td>\n");

	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, fmt, rmsummary_to_external_unit(h->field, h->mean));
	fprintf(stream, "</td>\n");

	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, "%6.2lf\n", h->mean > 0 ? (h->mean + sqrt(h->variance))/h->mean : -1);
	fprintf(stream, "</td>\n");

	fprintf(stream, "<td class=\"data\">\n");
	fprintf(stream, fmt_stats, h->fa_max.throughput/h->fa_max.throughput, ((100.0*h->fa_max.waste)/(h->fa_max.waste + h->usage)));
	fprintf(stream, "<br><br>\n");
	fprintf(stream, fmt_alloc, rmsummary_to_external_unit(h->field, h->fa_max.first));
	fprintf(stream, "</td>\n");

	fprintf(stream, "<td class=\"data\">\n");
	fprintf(stream, fmt_stats, h->fa_max_throughput.throughput/h->fa_max.throughput, ((100.0*h->fa_max_throughput.waste)/(h->fa_max_throughput.waste + h->usage)));
	fprintf(stream, "<br><br>\n");
	fprintf(stream, fmt_alloc, rmsummary_to_external_unit(h->field, h->fa_max_throughput.first));
	fprintf(stream, "</td>\n");

	fprintf(stream, "<td class=\"data\">\n");
	fprintf(stream, fmt_stats, h->fa_min_waste_time_dependence.throughput/h->fa_max.throughput, ((100.0*h->fa_min_waste_time_dependence.waste)/(h->fa_min_waste_time_dependence.waste + h->usage)));
	fprintf(stream, "<br><br>\n");
	fprintf(stream, fmt_alloc, rmsummary_to_external_unit(h->field, h->fa_min_waste_time_dependence.first));
	fprintf(stream, "</td>\n");

	if(brute_force) {
		fprintf(stream, "<td class=\"data\">\n");
		fprintf(stream, fmt_stats, h->fa_max_throughput_brute_force.throughput/h->fa_max.throughput, ((100.0*h->fa_max_throughput_brute_force.waste)/(h->fa_max_throughput_brute_force.waste + h->usage)));
		fprintf(stream, "<br><br>\n");
		fprintf(stream, fmt_alloc, rmsummary_to_external_unit(h->field, h->fa_max_throughput_brute_force.first));
		fprintf(stream, "</td>\n");

		fprintf(stream, "<td class=\"data\">\n");
		fprintf(stream, fmt_stats, h->fa_min_waste_brute_force.throughput/h->fa_max.throughput, ((100.0*h->fa_min_waste_brute_force.waste)/(h->fa_min_waste_brute_force.waste + h->usage)));
		fprintf(stream, "<br><br>\n");
		fprintf(stream, fmt_alloc, rmsummary_to_external_unit(h->field, h->fa_min_waste_brute_force.first));
		fprintf(stream, "</td>\n");
	}
}

void write_individual_histogram_webpage(struct field_stats *h)
{

	char *fname = path_of_page(h, 0);
	FILE *fo    = open_file(fname);
	free(fname);

	fprintf(fo, "<head>\n");
	fprintf(fo, "<title> %s : %s </title>\n", h->source->category_name, h->field);
	write_css_style(fo);
	fprintf(fo, "</head>\n");

	fprintf(fo, "<body>\n");

	fprintf(fo, "<tr>\n");
	fprintf(fo, "<table>\n");
	fprintf(fo, "<td rowspan=\"%d\">\n", OUTLIER_N + 2);
	fprintf(fo, "<img src=\"%s\">", path_of_image(h, 1));
	fprintf(fo, "</td>\n");
	fprintf(fo, "</tr>\n");

	fprintf(fo, "<tr>\n");
	fprintf(fo, "<td class=\"data\"> maxs </td> <td> </td> <td class=\"data\"> mins </td>\n");
	fprintf(fo, "</tr>\n");

	int i;
	struct rmsummary *s;
	int outliers = h->total_count < OUTLIER_N  ? h->total_count : OUTLIER_N;
	for(i = 0; i < outliers; i++)
	{
		fprintf(fo, "<tr>\n");

		s = h->summaries_sorted[h->total_count - i - 1];
		write_outlier(fo, s, h->field, "../");

		fprintf(fo, "<td> </td>");

		s = h->summaries_sorted[i];
		write_outlier(fo, s, h->field, "../");

		fprintf(fo, "</tr>\n");
	}

	fprintf(fo, "</table>\n");

	fprintf(fo, "<table>\n");
	fprintf(fo, "<tr>\n");
	write_webpage_stats_header(fo, h);
	fprintf(fo, "</tr>\n");

	fprintf(fo, "<tr>\n");
	write_webpage_stats(fo, h, "../", 0);
	fprintf(fo, "</tr>\n");
	fprintf(fo, "</table>\n");

	fprintf(fo, "</body>\n");

	fclose(fo);
}

void write_front_page(char *workflow_name)
{
	FILE *fo;
	char *filename = string_format("%s/index.html", output_directory);
	fo = fopen(filename, "w");

	int columns = brute_force ? 9 : 7;

	if(!fo)
		fatal("Could not open file %s for writing: %s\n", strerror(errno));

	fprintf(fo, "<head>\n");

	fprintf(fo, "<title> %s </title>\n", workflow_name);
	write_css_style(fo);
	fprintf(fo, "</head>\n");

	fprintf(fo, "<body>\n");

	fprintf(fo, "<table>\n");

	fprintf(fo, "<tr>\n");

	struct rmsummary_set *s;

	list_first_item(all_sets);
	while((s = list_next_item(all_sets)))
	{
		fprintf(fo, "<td class=\"datahdr\" colspan=\"%d\">%s: %d</td>", columns, s->category_name, list_size(s->summaries));
	}
	fprintf(fo, "</tr>\n");

	LOOP_FIELD(field_name) {
		if(!field_is_active(field_name)) {
			continue;
		}

		fprintf(fo, "<tr>\n");
		list_first_item(all_sets);
		while((s = list_next_item(all_sets)))
		{
			struct field_stats *h = hash_table_lookup(s->stats, field_name);
			write_webpage_stats_header(fo, h);
		}
		fprintf(fo, "</tr>\n");

		fprintf(fo, "<tr>\n");
		list_first_item(all_sets);
		while((s = list_next_item(all_sets)))
		{
			struct field_stats *h = hash_table_lookup(s->stats, field_name);
			write_webpage_stats(fo, h, NULL, 1);
		}
		fprintf(fo, "</tr>\n");
	} LOOP_END

	fprintf(fo, "</table>\n");
	fprintf(fo, "</body>\n");

}


void write_webpage(char *workflow_name)
{
	debug(D_RMON, "Writing html pages.");

	write_front_page(workflow_name);

	struct rmsummary_set *s;
	list_first_item(all_sets);
	while((s = list_next_item(all_sets)))
	{
		LOOP_FIELD(field_name) {
			if(!field_is_active(field_name)) {
				continue;
			}

			struct field_stats *h = hash_table_lookup(s->stats, field_name);
			write_individual_histogram_webpage(h);
		} LOOP_END
	}
}

static void show_usage(const char *cmd)
{
	fprintf(stdout, "\nUse: %s [options] output_directory [workflow_name]\n\n", cmd);
	fprintf(stdout, "\nIf -L is specified, read the summary file list from standard input.\n\n");
	fprintf(stdout, "%-20s Enable debugging for this subsystem.\n", "-d <subsystem>");
	fprintf(stdout, "%-20s Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal)\n", "-o <file>");
	fprintf(stdout, "%-20s Read summaries filenames from file <list>.\n", "-L <list>");
	fprintf(stdout, "%-20s Split on task categories.\n", "-s");
	fprintf(stdout, "%-20s Use brute force to compute proposed resource allocations. (slow)\n", "-b");
	fprintf(stdout, "%-20s Do not plot histograms.\n", "-n");
	fprintf(stdout, "%-20s Select these fields for the histograms.     (Default is: cores,memory,disk).\n\n", "-f <fields>");
	fprintf(stdout, "%-20s Show this message.\n", "-h,--help");
}

int main(int argc, char **argv)
{
	char *input_list      = NULL;
	char *workflow_name   = NULL;

	debug_config(argv[0]);

	signed char c;
	while( (c = getopt(argc, argv, "bd:f:j:hL:no:")) > -1 )
	{
		switch(c)
		{
			case 'L':
				input_list      = xxstrdup(optarg);
				break;
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'o':
				debug_config_file(optarg);
				break;
			case 'f':
				parse_fields_options(optarg);
				break;
			case 'b':
				brute_force = 1;
				break;
			case 'j':
				/* smaller bucket size */
				omp_set_num_threads(atoi(optarg));
				break;
			case 'n':
				webpage_mode = 0;
				break;
			case 'h':
				show_usage(argv[0]);
				exit(0);
				break;
			default:
				show_usage(argv[0]);
				exit(1);
				break;
		}
	}

	if(argc - optind < 1)
	{
		show_usage(argv[0]);
		exit(1);
	}

	if(!input_list)
	{
		input_list = "-";
	}

	output_directory = argv[optind];

	char *outlier_dir = string_format("%s/%s", output_directory, OUTLIER_DIR);
	if(create_dir(outlier_dir, 0755) < 0 && errno != EEXIST)
		fatal("Could not create outliers directory.");
	free(outlier_dir);

	if(argc - optind > 1)
	{
		workflow_name    = argv[optind + 1];
	}
	else
	{
		workflow_name = output_directory;
	}

	categories = hash_table_create(0, 0);
	all_sets = list_create();

	/* read and parse all input summaries */
	all_summaries = make_new_set(ALL_SUMMARIES_CATEGORY);

	input_overhead = timestamp_get();

	debug(D_RMON, "Reading summaries.");

	category_tune_bucket_size("category-steady-n-tasks", 10000000000);

	if(input_list)
	{
		parse_summary_from_filelist(all_summaries, input_list, categories);
	}
	list_push_head(all_sets, all_summaries);


	/* partition summaries on category name */
	split_summaries_on_category(all_summaries);

	input_overhead = timestamp_get() - input_overhead;


	struct jx *report = jx_object(NULL);
	if(list_size(all_summaries->summaries) > 0)
	{
		/* construct field_statss across all categories/resources. */
		struct rmsummary_set *s;
		list_first_item(all_sets);
		while((s = list_next_item(all_sets)))
		{
			histograms_of_category(s);
			set_first_allocations_of_category(s, categories);

			set_usage(s);

			jx_insert(report, jx_string(s->category_name), category_to_json(s));

			if(webpage_mode)
			{
				plots_of_category(s);
			}
		}
	}

	char *output_file = string_format("%s/stats.json", output_directory);
	FILE *f_stats  = open_file(output_file);
	jx_pretty_print_stream(report, f_stats);
	fclose(f_stats);
	free(output_file);

	if(webpage_mode)
	{
		write_webpage(workflow_name);
	}

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
