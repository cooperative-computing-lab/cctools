/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <float.h>
#include <omp.h>

#include "resource_monitor_tools.h"
#include "create_dir.h"
#include "category_internal.h"
#include "macros.h"
#include "copy_stream.h"
#include "timestamp.h"

#define MAX_LINE 1024

#define OUTLIER_DIR "outliers"
#define OUTLIER_N    5

#define MAX_P        1.00

#define value_at_index(h, idx) (value_of_field((h)->summaries_sorted[(idx)], (h)->resource))

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
	double throughput;
	double tasks_done;
	double time_taken;
	int    retries;
};

struct field_stats {
	struct field *resource;

	struct rmsummary_set *source;
	struct rmsummary **summaries_sorted;
	int    total_count;

	double bin_size;

	uint64_t min_value;
	uint64_t max_value;
	uint64_t count_at_min_value;
	uint64_t count_at_max_value;

	uint64_t  max_count;             //i.e., how many times the mode occurs.
	uint64_t  min_count;
	double    value_at_max_count;    //i.e., mode
	double    value_at_min_count;

	double mean;
	double variance;
	double std_dev;

	struct allocation fa_perfect;
	struct allocation fa_max;
	struct allocation fa_95;
	struct allocation fa_min_waste_time_dependence;
	struct allocation fa_min_waste_time_independence;
	struct allocation fa_min_waste_brute_force;
	struct allocation fa_max_throughput;
	struct allocation fa_max_throughput_brute_force;

	uint64_t usage;

	struct itable *buckets;
	uint64_t  nbuckets;

	char *output_directory;
};

struct list *all_sets;

struct rmsummary *max_values;
struct rmsummary_set *all_summaries;

struct hash_table *unique_strings;

int split_categories  = 0;
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

static struct field *sort_field;
int less_than(const void *a, const void *b)
{
	struct rmsummary *sa = * (struct rmsummary * const *) a;
	struct rmsummary *sb = * (struct rmsummary * const *) b;

	int64_t fa = value_of_field(sa, sort_field);
	int64_t fb = value_of_field(sb, sort_field);

	return (fa > fb);
}

void sort_by_field(struct field_stats *h, struct field *f)
{
	sort_field = f;

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

int64_t set_bin_size_by_iqr(struct field_stats *h)
{
	int64_t v_25 = value_of_p(h, 0.25);
	int64_t v_75 = value_of_p(h, 0.75);

	double bin_size = 1;

	if(v_75 > v_25) {
		bin_size = 2*(v_75 - v_25)*pow((double) h->total_count, (-1.0/3.0));
	}

	return (int64_t) ceil(bin_size);
}

uint64_t get_bucket_count(struct field_stats *h, uint64_t bucket)
{
	return (uint64_t) ((uintptr_t) itable_lookup(h->buckets, bucket + 1));
}

double get_bucket_value(struct field_stats *h, uint64_t bucket)
{
	return h->bin_size * (bucket);
}

uint64_t bucket_of(struct field_stats *h, double value)
{
	return (uint64_t) floor(value/h->bin_size);
}

uint64_t increment_bucket(struct field_stats *h, double value)
{
	uint64_t bucket = bucket_of(h, value);
	uint64_t count = get_bucket_count(h, bucket);
	count += 1;

	itable_insert(h->buckets, bucket + 1, (void *) ((uintptr_t) count));

	return count;
}

void set_min_max_value_of_field(struct field_stats *h, struct field *f)
{
	h->min_value = floor(value_of_field(h->summaries_sorted[0], f));
	h->max_value = ceil(value_of_field(h->summaries_sorted[h->total_count - 1], f));

	h->count_at_min_value = (uintptr_t) get_bucket_count(h, bucket_of(h, h->min_value));
	h->count_at_max_value = (uintptr_t) get_bucket_count(h, bucket_of(h, h->max_value));
}

double set_average_of_field(struct field_stats *h, struct field *f)
{
	double accum = 0;

	int i;
	for(i = 0; i < h->total_count; i++)
		accum += value_of_field(h->summaries_sorted[i], f);

	h->mean = accum/h->total_count;

	return h->mean;
}

double set_variance_of_field(struct field_stats *h, struct field *f)
{
	double accum = 0;

	int i;
	for(i = 0; i < h->total_count; i++)
		accum += pow(value_of_field(h->summaries_sorted[i], f) - h->mean, 2);

	if(h->total_count > 1)
	{
		h->variance = accum/(h->total_count - 1);
		h->std_dev  = sqrt(h->variance);
	}
	else
	{
		h->variance = -1;
		h->std_dev  = -1;
	}

	return h->variance;
}

uint64_t set_min_max_count(struct field_stats *h)
{
	uint64_t  bucket;
	uint64_t  count;

	h->max_count = 0;
	h->min_count = INT_MAX;

	h->value_at_max_count = 0;

	itable_firstkey(h->buckets);
	while(itable_nextkey(h->buckets, &bucket, (void *) &count))
	{
		if(count > h->max_count)
		{
			h->value_at_max_count = get_bucket_value(h, bucket - 1);
			h->max_count          = count;
		}

		if(count < h->min_count)
		{
			h->value_at_min_count = get_bucket_value(h, bucket - 1);
			h->min_count          = count;
		}
	}

	return h->max_count;
}

char *path_common(struct field_stats *h, int only_base_name)
{
	char *category = sanitize_path_name(h->source->category);

	char *prefix;
	if(only_base_name)
	{
		prefix = "";
	}
	else
	{
		prefix = h->output_directory;
	}

	char *path = string_format("%s%s_%s", prefix, category, h->resource->name);

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
	char *category = sanitize_path_name(h->source->category);
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

	uint64_t  bucket;
	uint64_t  count;

	itable_firstkey(h->buckets);
	while(itable_nextkey(h->buckets, &bucket, (void *) &count))
		fprintf(f, "%lf %" PRIu64 "\n", get_bucket_value(h, bucket - 1), count);

	fclose(f);
}

void write_variables_gnuplot(struct field_stats *h, struct field_stats *all)
{
	char *fname = path_of_variables_script(h, 0);
	FILE *f     = open_file(fname);
	free(fname);

	fprintf(f, "%s = %" PRId64"\n", "current_buckets",    h->nbuckets);
	fprintf(f, "%s = %" PRId64"\n", "current_minimum",    h->min_value);
	fprintf(f, "%s = %" PRId64"\n", "current_maximum",    h->max_value);
	fprintf(f, "%s = %lf\n",        "current_mode",       h->value_at_max_count);
	fprintf(f, "%s = %" PRId64"\n", "current_mode_count", h->max_count);
	fprintf(f, "%s = %" PRId64"\n", "current_min_count",  h->min_count);
	fprintf(f, "%s = %lf\n",        "current_mean",       h->mean);
	fprintf(f, "%s = %lf\n",        "current_percentile75", value_of_p(h, 0.75));
	fprintf(f, "%s = %lf\n",        "current_percentile25", value_of_p(h, 0.25));

	char *value;
	value = field_str(h->resource, h->fa_max_throughput.first);
	fprintf(f, "%s = %s\n", "current_first_allocation", value);
	free(value);

	value = field_str(h->resource, h->fa_min_waste_time_dependence.first);
	fprintf(f, "%s = %s\n", "current_first_allocation_min_waste", value);
	free(value);

	fprintf(f, "%s = %lf\n",        "current_bin_size",   h->bin_size);

	if(all) {
		fprintf(f, "%s = %" PRId64"\n", "all_minimum",    all->min_value);
		fprintf(f, "%s = %" PRId64"\n", "all_maximum",    all->max_value);
		fprintf(f, "%s = %lf\n",        "all_mode",       all->value_at_max_count);
		fprintf(f, "%s = %" PRId64"\n", "all_mode_count", all->max_count);
		fprintf(f, "%s = %lf\n",        "all_mean",       all->mean);
		fprintf(f, "%s = %lf\n",        "all_percentile75", value_of_p(all, 0.75));
		fprintf(f, "%s = %lf\n",        "all_percentile25", value_of_p(all, 0.25));

		value = field_str(h->resource, all->fa_max_throughput.first);
		fprintf(f, "%s = %s\n", "all_first_allocation", value);
		free(value);

		value = field_str(h->resource, all->fa_min_waste_time_dependence.first);
		fprintf(f, "%s = %s\n", "all_first_allocation_min_waste", value);
		free(value);
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

	if(h == all)
	{
		fprintf(f, "set label sprintf(\"%%.0f\", all_minimum) at all_minimum,graph -0.01 tc ls 1 right front nopoint offset character -1.0,character -0.25\n");
		fprintf(f, "set label sprintf(\"%%.0f\", all_maximum) at all_maximum,graph -0.01 tc ls 1 left front nopoint offset character 1.0,character -0.25\n");
	}

	if( all->nbuckets == 1 )
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

	if(all->max_count > 10000*all->min_count)
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

	if( all->nbuckets == 1 )
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
	if(h->max_count > 10000*h->min_count)
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
		char *path = string_format("%s/%s", output_directory, sanitize_path_name(h->source->category));
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
		char *path = string_format("%s/%s", output_directory, sanitize_path_name(h->source->category));
		if(chdir(path) == 0) {
			execlp(gnuplot_path, "gnuplot", path_of_image_script(h, 1), NULL);
		}
		fatal("Could not exec when creating image: %s\n", path_of_image(h, 0));
	}

	gnuplots_running += 2;
}

struct field_stats *histogram_of_field(struct rmsummary_set *source, struct field *f, char *out_dir)
{
	struct field_stats *h = malloc(sizeof(struct field_stats));

	h->total_count = list_size(source->summaries);
	h->summaries_sorted = malloc(h->total_count * sizeof(struct rmsummary *));

	struct rmsummary *s;
	list_first_item(source->summaries);
	int i = 0;
	while((s = list_next_item(source->summaries)))
	{
		h->summaries_sorted[i] = s;
		i++;
	}
	sort_by_field(h, f);

	h->source   = source;
	h->buckets  = itable_create(0);

	h->resource = f;

	create_output_directory(h);

	set_bin_size_by_iqr(h);

	double value;

	list_first_item(source->summaries);
	while((s = list_next_item(source->summaries)))
	{
		value = value_of_field(s, f);
		increment_bucket(h, value);
	}

	h->nbuckets = itable_size(h->buckets);

	set_min_max_value_of_field(h, f);
	set_min_max_count(h);

	set_average_of_field(h,  f);
	set_variance_of_field(h, f);

	itable_insert(source->histograms, (uint64_t) ((uintptr_t) f), (void *) h);

	debug(D_RMON, "%s-%s:\n buckets: %" PRIu64 " bin_size: %lf max_count: %" PRIu64 " mode: %lf\n", h->source->category, h->resource->caption, h->nbuckets, h->bin_size, h->max_count, h->value_at_max_count);

	return h;
}

#define write_stats_header_cols(file, name)\
	fprintf(file, "fa_%s,waste,through,retries,time,done,", #name)

void write_histogram_stats_header(FILE *stream)
{
	fprintf(stream, "resource,units,");
	fprintf(stream, "count,mean,std_dev,");
	fprintf(stream, "min,usage,");

	write_stats_header_cols(stream, perfect);
	write_stats_header_cols(stream, max);
	write_stats_header_cols(stream, 95);

	if(brute_force) {
		write_stats_header_cols(stream, min_waste_bf);
		write_stats_header_cols(stream, max_throu_bf);
	}

	write_stats_header_cols(stream, min_waste_ti);
	write_stats_header_cols(stream, min_waste_td);
	write_stats_header_cols(stream, max_throu);

	fprintf(stream, "p_25,p_50,p_75,p_99\n");
}

#define write_stats_row(file, alloc)\
	fprintf(file, "%" PRId64 ",%.0lf,%lf,%d,%0.3lf,%0.3lf,", alloc.first, ceil(alloc.waste), alloc.throughput, alloc.retries, alloc.time_taken, alloc.tasks_done)

void write_histogram_stats(FILE *stream, struct field_stats *h)
{
	fprintf(stream, "%s,%s,", sanitize_path_name(h->resource->name),h->resource->units);
	fprintf(stream, "%d,%.0lf,%.2lf,", h->total_count, ceil(h->mean), h->std_dev);
	fprintf(stream, "%.0lf,%" PRId64 ",", floor(h->min_value), h->usage);

	write_stats_row(stream, h->fa_perfect);
	write_stats_row(stream, h->fa_max);
	write_stats_row(stream, h->fa_95);

	if(brute_force) {
		write_stats_row(stream, h->fa_min_waste_brute_force);
		write_stats_row(stream, h->fa_max_throughput_brute_force);
	}

	write_stats_row(stream, h->fa_min_waste_time_independence);
	write_stats_row(stream, h->fa_min_waste_time_dependence);
	write_stats_row(stream, h->fa_max_throughput);

	fprintf(stream, "%.2lf,%.2lf,%.2lf,%.2lf\n",
			value_of_p(h, 0.25),
			value_of_p(h, 0.50),
			value_of_p(h, 0.75),
			value_of_p(h, 0.99));
}

void histograms_of_category(struct rmsummary_set *ss)
{
	/* construct field_statss of category across all resources */
	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(!f->active)
			continue;

		debug(D_RMON, "Computing histogram of %s.%s", ss->category, f->name);

		histogram_of_field(ss, f, output_directory);
	}

}

void plots_of_category(struct rmsummary_set *s)
{
	struct field_stats     *h;

	/* construct field_statss of category across all resources */
	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));

		struct field_stats *all = itable_lookup(all_summaries->histograms, (uint64_t) ((uintptr_t) f));
		if(!all)
			all = h;

		write_histogram_table(h);
		write_variables_gnuplot(h, all);
		write_thumbnail_gnuplot(h, all);
		write_image_gnuplot(h, all);

		write_images(h);
	}

	while(gnuplots_running)
	{
		wait(NULL);
		gnuplots_running--;
	}
}

double total_waste(struct field_stats *h, struct field *f, double first_alloc) {
	double waste   = 0;
	double max_candidate = h->max_value;

	if(first_alloc < 0)
		return 0;

	int i;
#pragma omp parallel for private(i) reduction(+: waste)
	for(i = 0; i < h->total_count; i+=1) {
		double current   = value_of_field(h->summaries_sorted[i], f);

		double wall_time;
		if(f->cummulative) {
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

	return waste;
}

double total_usage(struct field_stats *h, struct field *f) {
	double usage   = 0;

	int i;
#pragma omp parallel for private(i) reduction(+: usage)
	for(i = 0; i < h->total_count; i+=1) {
		double current   = value_of_field(h->summaries_sorted[i], f);

		double wall_time;
		if(f->cummulative) {
			wall_time = 1;
		} else {
			wall_time = h->summaries_sorted[i]->wall_time;
		}

		usage += current * wall_time;
	}

	return usage;
}

void set_usage(struct rmsummary_set *s) {
	struct field *f;
	struct field_stats *h;

	int i;
	for(i = WALL_TIME; i < NUM_FIELDS; i++)
	{
		f = (fields + i);

		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));

		h->usage = total_usage(h, f);
	}
}

double throughput(struct field_stats *h, struct field *f, double first_alloc, struct allocation *alloc) {

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

	struct field_stats *all = itable_lookup(all_summaries->histograms, (uint64_t) ((uintptr_t) f));
	if(!all) {
		all = h;
	}
	double max_allocation  = all->max_value;


	int i;
#pragma omp parallel for private(i) reduction(+: wall_time_accum, tasks_accum)
	for(i = 0; i < h->total_count; i+=1) {
		double current_task;
		double current = value_of_field(h->summaries_sorted[i], f);

		if(current < 1) {
			continue;
		}

		/* for perfect throughput */
		if(first_alloc < 0) {
			current_task = max_allocation/current;
		} else {
			current_task = max_allocation/first_alloc;
		}

		double wall_time = h->summaries_sorted[i]->wall_time;

		wall_time_accum += wall_time;

		if(first_alloc > 0 && current > first_alloc) {
			tasks_accum     += 1;
			wall_time_accum += wall_time;
		} else {
			tasks_accum     += current_task;
		}
	}

	double th;
	if(wall_time_accum > 0) {
		th = tasks_accum/wall_time_accum;
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

int retries(struct field_stats *h, struct field *f, double first_alloc) {
	int tasks_retried     = 0;

	int i;
#pragma omp parallel for private(i) reduction(+: tasks_retried)
	for(i = 0; i < h->total_count; i+=1) {
		double current   = value_of_field(h->summaries_sorted[i], f);

		if(current > first_alloc) {
			tasks_retried += 1;
		}
	}

	return tasks_retried;
}


void set_category_maximum(struct rmsummary_set *s, struct hash_table *categories) {
	struct field *f;
	struct field_stats *h;

	struct category *c = category_lookup_or_create(categories, s->category);
	if(!c->max_allocation)
		c->max_allocation = rmsummary_create(-1);

	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));

		int64_t value;
		rmsummary_to_internal_unit(f->name, h->max_value, &value, f->units);

		if(strcmp(f->format, PRId64) != 0) {
			rmsummary_assign_double_field(c->max_allocation, f->name, value);
		} else {
			rmsummary_assign_int_field(c->max_allocation, f->name, value);
		}
	}
}

void set_fa_values(struct field_stats *h, struct field *f, struct allocation *alloc, double first_allocation) {
	alloc->first      = first_allocation;
	alloc->waste      = total_waste(h, f, alloc->first);
	alloc->throughput =  throughput(h, f, alloc->first, alloc);
	alloc->retries    =     retries(h, f, alloc->first);
}

void set_fa_min_waste_time_dependence(struct rmsummary_set *s, struct hash_table *categories) {
	struct category *c = category_lookup_or_create(categories, s->category);
	c->time_peak_independece = 0;
	c->allocation_mode       = CATEGORY_ALLOCATION_MODE_MIN_WASTE;

	category_update_first_allocation(c, NULL);

	if(!c->first_allocation)
		return;

	struct rmsummary *firsts = rmsummary_copy(c->first_allocation);

	struct field *f;
	struct field_stats *h;

	int i;
	for(i = WALL_TIME; i < NUM_FIELDS; i++)
	{
		f = (fields + i);

		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));
		set_fa_values(h, f, &(h->fa_min_waste_time_dependence), value_of_field(firsts, f));
	}

	free(firsts);
}

void set_fa_min_waste_time_independence(struct rmsummary_set *s, struct hash_table *categories) {
	struct category *c = category_lookup_or_create(categories, s->category);
	c->time_peak_independece = 1;
	c->allocation_mode       = CATEGORY_ALLOCATION_MODE_MIN_WASTE;

	category_update_first_allocation(c, NULL);

	if(!c->first_allocation)
		return;

	struct rmsummary *firsts = rmsummary_copy(c->first_allocation);

	struct field *f;
	struct field_stats *h;

	int i;
	for(i = WALL_TIME; i < NUM_FIELDS; i++)
	{
		f = (fields + i);

		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));
		set_fa_values(h, f, &(h->fa_min_waste_time_independence), value_of_field(firsts, f));
	}

	free(firsts);
}

void set_fa_max_throughput(struct rmsummary_set *s, struct hash_table *categories) {
	struct category *c = category_lookup_or_create(categories, s->category);
	c->time_peak_independece = 0;
	c->allocation_mode       = CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT;

	category_update_first_allocation(c, NULL);

	if(!c->first_allocation)
		return;

	struct rmsummary *firsts = rmsummary_copy(c->first_allocation);

	struct field *f;
	struct field_stats *h;

	int i;
	for(i = WALL_TIME; i < NUM_FIELDS; i++)
	{
		f = (fields + i);

		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));
		set_fa_values(h, f, &(h->fa_max_throughput), value_of_field(firsts, f));
	}

	free(firsts);
}

void set_fa_95(struct rmsummary_set *s, struct hash_table *categories) {
	struct field *f;
	struct field_stats *h;

	int i;
	for(i = WALL_TIME; i < NUM_FIELDS; i++)
	{
		f = (fields + i);

		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));
		set_fa_values(h, f, &(h->fa_95), value_of_p(h, 0.95));
	}
}

void set_fa_max(struct rmsummary_set *s, struct hash_table *categories) {
	struct field *f;
	struct field_stats *h;

	int i;
	for(i = WALL_TIME; i < NUM_FIELDS; i++)
	{
		f = (fields + i);

		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));
		set_fa_values(h, f, &(h->fa_max), h->max_value);
	}
}

void set_fa_perfect(struct rmsummary_set *s, struct hash_table *categories) {
	struct field *f;
	struct field_stats *h;

	int i;
	for(i = WALL_TIME; i < NUM_FIELDS; i++)
	{
		f = (fields + i);

		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));
		set_fa_values(h, f, &(h->fa_perfect), -1);

		h->fa_perfect.first      = -1;
		h->fa_perfect.waste      =  0;
		h->fa_perfect.retries    =  0;
	}
}

int64_t min_waste_brute_force_field(struct field_stats *h, struct field *f) {
	double   min_waste     = DBL_MAX;
	uint64_t min_candidate = h->max_value;

	uint64_t prev = 0;
	for(int i = 0; i < h->total_count; i++) {
		uint64_t candidate = value_of_field(h->summaries_sorted[i], f);

		if( i > 0 ) {
			if(candidate - prev < 1)
				continue;
		}

		double candidate_waste = total_waste(h, f, candidate);

		if(candidate_waste < min_waste) {
			min_candidate = candidate;
			min_waste     = candidate_waste;
		}

		prev = candidate;
	}

	debug(D_RMON, "first allocation '%s' brute force: %" PRId64, f->caption, min_candidate);

	return min_candidate;
}

void set_fa_min_waste_brute_force(struct rmsummary_set *s, struct hash_table *categories) {
	struct field *f;
	struct field_stats *h;

	int i;
	for(i = WALL_TIME; i < NUM_FIELDS; i++)
	{
		f = (fields + i);

		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));

		if(brute_force) {
			set_fa_values(h, f, &(h->fa_min_waste_brute_force), min_waste_brute_force_field(h, f));
		}
	}
}

int64_t max_throughput_brute_force_field(struct field_stats *h, struct field *f) {
	double   best_throughput = 0;
	uint64_t max_candidate  = h->max_value;

	uint64_t prev = 0;
	for(int i = 0; i < h->total_count; i++) {
		uint64_t candidate = value_of_field(h->summaries_sorted[i], f);

		if( i > 0 ) {
			if(candidate - prev < 1)
				continue;
		}

		double candidate_throughput = throughput(h, f, candidate, NULL);

		if(candidate_throughput > best_throughput) {
			max_candidate  = candidate;
			best_throughput = candidate_throughput;
		}

		prev = candidate;
	}

	debug(D_RMON, "first allocation '%s' throughput_max: %" PRId64, f->caption, max_candidate);

	return max_candidate;
}


void set_fa_max_throughput_brute_force(struct rmsummary_set *s, struct hash_table *categories) {
	struct field *f;
	struct field_stats *h;

	int i;
	for(i = WALL_TIME; i < NUM_FIELDS; i++)
	{
		f = (fields + i);

		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));

		if(brute_force) {
			set_fa_values(h, f, &(h->fa_max_throughput_brute_force), max_throughput_brute_force_field(h, f));
		}
	}
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

void write_stats_of_category(struct rmsummary_set *s)
{
	char *name_raw   = sanitize_path_name(s->category);
	char *filename      = string_format("%s/%s.stats", output_directory, name_raw);

	FILE *f_stats  = open_file(filename);

	free(name_raw);
	free(filename);

	debug(D_RMON, "Writing stats for %s", s->category);

	write_histogram_stats_header(f_stats);

	struct field_stats *h;
	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));
		write_histogram_stats(f_stats, h);
	}

	fclose(f_stats);
}

void write_limits_of_category(struct rmsummary_set *s)
{

	char *name_raw   = sanitize_path_name(s->category);
	char *filename      = string_format("%s/%s.limits", output_directory, name_raw);

	FILE *f_limits      = open_file(filename);

	free(filename);
	free(name_raw);

	debug(D_RMON, "Writing limits for %s", s->category);

	struct field *f;
	struct field_stats *h;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));

		char *value = field_str(f, h->fa_max_throughput.first);
		fprintf(f_limits, "%s: %s\n", f->name, value);
		free(value);
	}

	fclose(f_limits);
}

void write_scatters_of_category_aux(struct itable *hc, char *cat_name, char *field_name) {
	int n = itable_size(hc);
	if(n < 1)
		return;

	int64_t *keys         = malloc(n*sizeof(intptr_t));
	int64_t *counts_accum = malloc(n*sizeof(intptr_t));
	double  *times_mean   = malloc(n*sizeof(intptr_t));
	double  *times_accum  = malloc(n*sizeof(intptr_t));
	double tau_mean;

	category_first_allocation_accum_times(hc, &tau_mean, keys, counts_accum, times_mean, times_accum);

	char *name_raw   = sanitize_path_name(cat_name);
	char *filename   = string_format("%s/%s/scatter_%s_time.data", output_directory, name_raw, field_name);
	FILE *f_rt = open_file(filename);

	fprintf(f_rt, "mean_time = %lf\n", tau_mean);
	fprintf(f_rt, "# resource mean_time >mean_time\n");
	fprintf(f_rt, "$DATA << EOD\n");

	int i;
	for(i = 0; i < n; i++) {
		double Pa = 1 - ((double) counts_accum[i])/counts_accum[n-1];
		double mean_rest = Pa > 0 ? times_accum[i]/Pa : 0;

		fprintf(f_rt, "%" PRId64 " %lf %lf\n", keys[i], times_mean[i], mean_rest);
	}

	fprintf(f_rt, "EOD\n");
	fclose(f_rt);

	free(keys);
	free(counts_accum);
	free(times_mean);
	free(times_accum);
}

#define write_scatters_of_field(c, id, field) if(fields[id].active) { write_scatters_of_category_aux((c)->field##_histogram, c->name, #field); }

void write_scatters_of_category(struct rmsummary_set *s)
{

	struct category *c = category_lookup_or_create(categories, s->category);

	debug(D_RMON, "Writing scatters for %s", s->category);

	write_scatters_of_field(c, WALL_TIME,       wall_time)
	write_scatters_of_field(c, CPU_TIME,        cpu_time)
	write_scatters_of_field(c, MAX_PROCESSES,   max_concurrent_processes)
	write_scatters_of_field(c, TOTAL_PROCESSES, total_processes)
	write_scatters_of_field(c, VIRTUAL,         virtual_memory)
	write_scatters_of_field(c, RESIDENT,        memory)
	write_scatters_of_field(c, SWAP,            swap_memory)
	write_scatters_of_field(c, B_READ,          bytes_read)
	write_scatters_of_field(c, B_WRITTEN,       bytes_written)
	write_scatters_of_field(c, B_RX,            bytes_received)
	write_scatters_of_field(c, B_TX,            bytes_sent)
	write_scatters_of_field(c, BANDWIDTH,       bandwidth)
	write_scatters_of_field(c, FILES,           total_files)
	write_scatters_of_field(c, DISK,            disk)
	write_scatters_of_field(c, CORES_PEAK,      cores)
	write_scatters_of_field(c, CORES_AVG,       cores_avg)
}

void write_overheads_of_category(struct rmsummary_set *s)
{
	char *name_raw   = sanitize_path_name(s->category);
	char *filename   = string_format("%s/%s.overheads", output_directory, name_raw);

	FILE *f_ovhs  = open_file(filename);

	debug(D_RMON, "Writing overheads for %s", s->category);

	free(name_raw);
	free(filename);

	fprintf(f_ovhs, "task_count,");
	fprintf(f_ovhs, "input,");

	if(brute_force) {
		fprintf(f_ovhs, "min_waste_brute_force,");
		fprintf(f_ovhs, "max_throughput_brute_force,");
	}

	fprintf(f_ovhs, "time_independence,");
	fprintf(f_ovhs, "time_dependence,");
	fprintf(f_ovhs, "max_throughput\n");


	fprintf(f_ovhs, "%d,",  list_size(s->summaries));
	fprintf(f_ovhs, "%lf,", rmsummary_to_external_unit("wall_time", input_overhead));

	if(brute_force) {
		fprintf(f_ovhs, "%lf,", rmsummary_to_external_unit("wall_time", s->overhead_min_waste_brute_force));
		fprintf(f_ovhs, "%lf,", rmsummary_to_external_unit("wall_time", s->overhead_max_throughput_brute_force));
	}

	fprintf(f_ovhs, "%lf,", rmsummary_to_external_unit("wall_time", s->overhead_min_waste_time_independence));
	fprintf(f_ovhs, "%lf,", rmsummary_to_external_unit("wall_time", s->overhead_min_waste_time_dependence));
	fprintf(f_ovhs, "%lf\n", rmsummary_to_external_unit("wall_time", s->overhead_max_throughput));


	fclose(f_ovhs);
}


char *copy_outlier(struct rmsummary *s)
{
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


void write_outlier(FILE *stream, struct rmsummary *s, struct field *f, char *prefix)
{
	char *outlier_name;

	outlier_name = copy_outlier(s);

	if(!outlier_name)
		return;

	if(!prefix)
	{
		prefix = "";
	}

	char control_str[128];
	snprintf(control_str, sizeof(control_str) - 1, "%%%s\n", f->format); 

	fprintf(stream, "<td class=\"data\">\n");
	fprintf(stream, "<a href=%s%s/%s>(%s)</a>", prefix, OUTLIER_DIR, outlier_name, s->task_id);
	fprintf(stream, "<br><br>\n");

	char *value = field_str(f, value_of_field(s, f));
	fprintf(stream, "%s\n", value);
	free(value);

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
	fprintf(stream, "<td class=\"data\">%s", h->resource->caption);
	if(h->resource->units)
	{
		fprintf(stream, " (%s)", h->resource->units);
	}
	fprintf(stream, "</td>");

	fprintf(stream, "<td class=\"datahdr\" >mode <br> &#9653;</td>");
	fprintf(stream, "<td class=\"datahdr\" >&mu; <br> &#9643; </td>");
	fprintf(stream, "<td class=\"datahdr\" >1<sup>st</sup> alloc. max t.p<br> &#9663; </td>");
	fprintf(stream, "<td class=\"datahdr\" >1<sup>st</sup> alloc. min waste </td>");
	fprintf(stream, "<td class=\"datahdr\" >1<sup>st</sup> alloc. ind.</td>");
	fprintf(stream, "<td class=\"datahdr\" >1<sup>st</sup> alloc. 0.95</td>");

	if(brute_force) {
		fprintf(stream, "<td class=\"datahdr\" >1<sup>st</sup> alloc. b.f.</td>");
		fprintf(stream, "<td class=\"datahdr\" >1<sup>st</sup> alloc. m.t.</td>");
	}

	fprintf(stream, "<td class=\"datahdr\" >(&mu;+&sigma;)/&mu;</td>");
	fprintf(stream, "<td class=\"datahdr\" >p<sub>99</sub></td>");
	fprintf(stream, "<td class=\"datahdr\" >p<sub>95</sub></td>");
}

void write_webpage_stats(FILE *stream, struct field_stats *h, char *prefix, int include_thumbnail)
{
	struct field *f = h->resource;

	fprintf(stream, "<td>");
	if(include_thumbnail)
	{
		fprintf(stream, "<a href=\"../%s\">",  path_of_page(h, 0));
		fprintf(stream, "<img src=\"../%s\">", path_of_thumbnail_image(h, 0));
		fprintf(stream, "</a>");
	}
	fprintf(stream, "</td>");

	char *value;
	
	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, "%6.0lf\n", h->value_at_max_count);
	fprintf(stream, "</td>\n");

	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, "%6.0lf\n", h->mean);
	fprintf(stream, "</td>\n");

	value = field_str(f, h->fa_max_throughput.first);
	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, "%s\n", value);
	fprintf(stream, "</td>\n");
	free(value);

	value = field_str(f, h->fa_min_waste_time_dependence.first);
	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, "%s\n", value);
	fprintf(stream, "</td>\n");
	free(value);

	value = field_str(f, h->fa_min_waste_time_independence.first);
	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, "%s\n", value);
	fprintf(stream, "</td>\n");
	free(value);

	value = field_str(f, h->fa_95.first);
	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, "%s\n", value);
	fprintf(stream, "</td>\n");
	free(value);

	if(brute_force) {
		value = field_str(f, h->fa_min_waste_brute_force.first);
		fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
		fprintf(stream, "%s\n", value);
		fprintf(stream, "</td>\n");
		free(value);

		value = field_str(f, h->fa_max_throughput_brute_force.first);
		fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
		fprintf(stream, "%s\n", value);
		fprintf(stream, "</td>\n");
		free(value);
	}

	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, "%6.2lf\n", h->mean > 0 ? (h->mean + h->std_dev)/h->mean : -1);
	fprintf(stream, "</td>\n");

	struct rmsummary *s;

	s = h->summaries_sorted[index_of_p(h, 0.99)];
	write_outlier(stream, s, f, prefix);

	s = h->summaries_sorted[index_of_p(h, 0.95)];
	write_outlier(stream, s, f, prefix);
}

void write_individual_histogram_webpage(struct field_stats *h)
{

	char *fname = path_of_page(h, 0);
	FILE *fo    = open_file(fname);
	free(fname);

	struct field *f = h->resource;

	fprintf(fo, "<head>\n");
	fprintf(fo, "<title> %s : %s </title>\n", h->source->category, f->caption);
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
		write_outlier(fo, s, f, "../");

		fprintf(fo, "<td> </td>");

		s = h->summaries_sorted[i];
		write_outlier(fo, s, f, "../");

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

	int columns = brute_force ? 12 : 10;

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
		fprintf(fo, "<td class=\"datahdr\" colspan=\"%d\">%s: %d</td>", columns, s->category, list_size(s->summaries));
	}
	fprintf(fo, "</tr>\n");

	struct field *f;
	struct field_stats      *h;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(!f->active)
			continue;

		fprintf(fo, "<tr>\n");
		list_first_item(all_sets);
		while((s = list_next_item(all_sets)))
		{
			h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));
			write_webpage_stats_header(fo, h);
		}
		fprintf(fo, "</tr>\n");

		fprintf(fo, "<tr>\n");
		list_first_item(all_sets);
		while((s = list_next_item(all_sets)))
		{
			h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));
			write_webpage_stats(fo, h, NULL, 1);
		}
		fprintf(fo, "</tr>\n");

	}

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
		struct field_stats *h;
		struct field *f;
		for(f = &fields[WALL_TIME]; f->name != NULL; f++)
		{
			if(!f->active)
				continue;

			h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));
			write_individual_histogram_webpage(h);
		}
	}
}

static void show_usage(const char *cmd)
{
	fprintf(stdout, "\nUse: %s [options] output_directory [workflow_name]\n\n", cmd);
	fprintf(stdout, "\nIf no -D or -L are specified, read the summary file list from standard input.\n\n");
	fprintf(stdout, "%-20s Enable debugging for this subsystem.\n", "-d <subsystem>");
	fprintf(stdout, "%-20s Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal)\n", "-o <file>");
	fprintf(stdout, "%-20s Read summaries recursively from <dir> (filename of form '%s[0-9]+%s').\n", "-D <dir>", RULE_PREFIX, RULE_SUFFIX);
	fprintf(stdout, "%-20s Read summaries filenames from file <list>.\n", "-L <list>");
	fprintf(stdout, "%-20s Split on task categories.\n", "-s");
	fprintf(stdout, "%-20s Use brute force to compute proposed resource allocations. (slow)\n", "-b");
	fprintf(stdout, "%-20s Use smallest bucket sizes to compute proposed resource allocations. (slow)\n", "-m");
	fprintf(stdout, "%-20s Do not plot histograms.\n", "-n");
	fprintf(stdout, "%-20s Select these fields for the histograms.     (Default is: tcvmsrwhz).\n\n", "-f <fields>");
	fprintf(stdout, "<fields> is a string in which each character should be one of the following:\n");
	fprintf(stdout, "%s", make_field_names_str("\n"));
	fprintf(stdout, "%-20s Show this message.\n", "-h,--help");
}

int main(int argc, char **argv)
{
	char *input_directory = NULL;
	char *input_list      = NULL;
	char *workflow_name   = NULL;

	unique_strings = hash_table_create(0, 0);

	debug_config(argv[0]);

	signed char c;
	while( (c = getopt(argc, argv, "bD:d:f:hL:mno:s")) > -1 )
	{
		switch(c)
		{
			case 'D':
				input_directory = xxstrdup(optarg);
				break;
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
			case 's':
				split_categories = 1;
				break;
			case 'b':
				brute_force = 1;
				break;
			case 'm':
				/* brute force, small bucket size */
				category_tune_bucket_size("time",   10*USECOND);
				category_tune_bucket_size("memory", 1);
				category_tune_bucket_size("disk",   1);
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

	if(!input_directory && !input_list)
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

	if(input_directory)
	{
		parse_summary_recursive(all_summaries, input_directory, categories);
	}

	if(input_list)
	{
		parse_summary_from_filelist(all_summaries, input_list, categories);
	}

	input_overhead = timestamp_get() - input_overhead;

	list_push_head(all_sets, all_summaries);

	if(list_size(all_summaries->summaries) > 0)
	{
		if(split_categories)
		{
			/* partition summaries on category name */
			split_summaries_on_category(all_summaries);
		}

		/* construct field_statss across all categories/resources. */
		struct rmsummary_set *s;
		list_first_item(all_sets);
		while((s = list_next_item(all_sets)))
		{
			histograms_of_category(s);
			set_first_allocations_of_category(s, categories);

			set_usage(s);

			write_stats_of_category(s);
			write_limits_of_category(s);
			write_overheads_of_category(s);
			write_scatters_of_category(s);

			if(webpage_mode)
			{
				plots_of_category(s);
			}
		}
	}

	if(webpage_mode)
	{
		write_webpage(workflow_name);
	}

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
