/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <float.h>
#include <omp.h>

#include "rmon_tools.h"
#include "create_dir.h"
#include "category_internal.h"
#include "macros.h"
#include "copy_stream.h"

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

struct histogram {
	struct field *resource;
	char *units;

	struct rmDsummary_set *source;
	struct rmDsummary **summaries_sorted;
	int    total_count;

	double bin_size;

	double z_95;
	double z_99;

	double min_value;
	double max_value;
	uint64_t count_at_min_value;
	uint64_t count_at_max_value;

	uint64_t  max_count;             //i.e., how many times the mode occurs.
	uint64_t  min_count;
	double    value_at_max_count;    //i.e., mode
	double    value_at_min_count;

	double mean;
	double variance;
	double std_dev;

	double kurtosis;
	double skewdness;

	uint64_t first_allocation_time_dependence;
	uint64_t first_allocation_time_independence;
	uint64_t first_allocation_brute_force;
	uint64_t first_allocation_95;

	double waste_time_dependence;
	double waste_time_independence;
	double waste_brute_force;
	double waste_95;

	uint64_t overhead_time_dependence;
	uint64_t overhead_time_independence;
	uint64_t overhead_brute_force;

	double waste_max;

	struct itable *buckets;
	uint64_t  nbuckets;

	char *output_directory;
};

struct list *all_sets;

struct rmDsummary *max_values;
struct rmDsummary_set *all_summaries;

struct hash_table *unique_strings;

int split_categories  = 0;
struct hash_table *categories;

int brute_force = 0;

char *unique_string(char *str)
{
	char *tmp = hash_table_lookup(unique_strings, str);

	if(tmp)
		return tmp;
	else
	{
		tmp = xxstrdup(str);
		hash_table_insert(unique_strings, str, tmp);
	}

	return tmp;
}

void split_summaries_on_category(struct rmDsummary_set *source)
{
	struct itable *splits = itable_create(0);

	struct rmDsummary *s;
	struct rmDsummary_set *bucket;
	char *label;

	list_first_item(source->summaries);
	while((s = list_next_item(source->summaries)))
	{
		label = unique_string(s->category);
		bucket = itable_lookup(splits, (uint64_t) ((uintptr_t) label));

		if(!bucket)
		{
			bucket = make_new_set(label);
			itable_insert(splits, (uint64_t) ((uintptr_t) label), bucket);
			list_push_tail(all_sets, bucket);
		}

		list_push_tail(bucket->summaries, s);
	}

	itable_delete(splits);
}

static struct field *sort_field;
int less_than(const void *a, const void *b)
{
	struct rmDsummary *sa = * (struct rmDsummary * const *) a;
	struct rmDsummary *sb = * (struct rmDsummary * const *) b;

	double fa = value_of_field(sa, sort_field);
	double fb = value_of_field(sb, sort_field);

	return (fa > fb);
}

void sort_by_field(struct histogram *h, struct field *f)
{
	sort_field = f;

	qsort(h->summaries_sorted, h->total_count, sizeof(struct rmDsummary *), less_than);
}

int index_of_p(struct histogram *h, double p)
{
	return (int) ceil((h->total_count - 1) * p);
}

double value_of_p(struct histogram *h, double p)
{
	return value_at_index(h, index_of_p(h, p));
}

double set_bin_size_by_iqr(struct histogram *h)
{
	double v_25 = value_of_p(h, 0.25);
	double v_75 = value_of_p(h, 0.75);

	if(v_75 > v_25)
		return h->bin_size = 2*(v_75 - v_25)*pow((double) h->total_count, (-1.0/3.0));
	else
		return h->bin_size = 1.0;
}

uint64_t get_bucket_count(struct histogram *h, uint64_t bucket)
{
	return (uint64_t) ((uintptr_t) itable_lookup(h->buckets, bucket + 1));
}

double get_bucket_value(struct histogram *h, uint64_t bucket)
{
	return h->bin_size * (bucket);
}

uint64_t bucket_of(struct histogram *h, double value)
{
	return (uint64_t) floor(value/h->bin_size);
}

uint64_t increment_bucket(struct histogram *h, double value)
{
	uint64_t bucket = bucket_of(h, value);
	uint64_t count = get_bucket_count(h, bucket);
	count += 1;

	itable_insert(h->buckets, bucket + 1, (void *) ((uintptr_t) count));

	return count;
}

void set_min_max_value_of_field(struct histogram *h, struct field *f)
{
	h->min_value = value_of_field(h->summaries_sorted[0], f);
	h->max_value = value_of_field(h->summaries_sorted[h->total_count - 1], f);

	h->count_at_min_value = (uintptr_t) get_bucket_count(h, bucket_of(h, h->min_value));
	h->count_at_max_value = (uintptr_t) get_bucket_count(h, bucket_of(h, h->max_value));
}

double set_average_of_field(struct histogram *h, struct field *f)
{
	double accum = 0;

	int i;
	for(i = 0; i < h->total_count; i++)
		accum += value_of_field(h->summaries_sorted[i], f);

	h->mean = accum/h->total_count;

	return h->mean;
}

double set_variance_of_field(struct histogram *h, struct field *f)
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

double set_skewdness_of_field(struct histogram *h, struct field *f)
{
	double accum = 0;

	int i;
	for(i = 0; i < h->total_count; i++)
		accum += pow(value_of_field(h->summaries_sorted[i], f) - h->mean, 3);

	if(h->total_count > 1 && h->variance != 0)
		h->skewdness = (accum/(pow(h->std_dev, 3) * (h->total_count - 1)));
	else
		h->skewdness = 0;

	return h->skewdness;
}

double set_kurtosis_of_field(struct histogram *h, struct field *f)
{
	double accum = 0;

	int i;
	for(i = 0; i < h->total_count; i++)
		accum += pow(value_of_field(h->summaries_sorted[i], f) - h->mean, 4);

	if(h->total_count > 1 && h->variance != 0)
		h->kurtosis = (accum/(pow(h->variance, 2) * (h->total_count - 1))) - 3;
	else
		h->kurtosis = 0;

	return h->kurtosis;
}

void set_z_scores(struct histogram *h)
{
	//one tail
	h->z_95 = h->mean + h->std_dev * 1.645;
	h->z_99 = h->mean + h->std_dev * 2.33;
}

uint64_t set_min_max_count(struct histogram *h)
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

char *path_common(struct histogram *h, int only_base_name)
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

char *path_of_table(struct histogram *h, int only_base_name)
{
	char *common = path_common(h, only_base_name);
	char *path   = string_format("%s_table.data", common);

	free(common);

	return path;
}

char *path_of_variables_script(struct histogram *h, int only_base_name)
{
	char *common = path_common(h, only_base_name);
	char *path   = string_format("%s_vars.gnuplot", common);
	free(common);

	return path;
}

char *path_of_thumbnail_script(struct histogram *h, int only_base_name)
{
	char *common = path_common(h, only_base_name);
	char *path   = string_format("%s_%dx%d.gnuplot", common, width_thumb, height_thumb);
	free(common);

	return path;
}

char *path_of_thumbnail_image(struct histogram *h, int only_base_name)
{
	char *common = path_common(h, only_base_name);
	char *path   = string_format("%s_%dx%d.%s", common, width_thumb, height_thumb, format);
	free(common);

	return path;
}

char *path_of_image_script(struct histogram *h, int only_base_name)
{
	char *common = path_common(h, only_base_name);
	char *path   = string_format("%s_%dx%d.gnuplot", common, width, height);
	free(common);

	return path;
}

char *path_of_image(struct histogram *h, int only_base_name)
{
	char *common = path_common(h, only_base_name);
	char *path   = string_format("%s_%dx%d.%s", common, width, height, format);
	free(common);

	return path;
}

char *path_of_page(struct histogram *h, int only_base_name)
{
	char *common = path_common(h, only_base_name);
	char *path   = string_format("%s.html", common);
	free(common);

	return path;
}


void create_output_directory(struct histogram *h)
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

void write_histogram_table(struct histogram *h)
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

void write_variables_gnuplot(struct histogram *h, struct histogram *all)
{
	char *fname = path_of_variables_script(h, 0);
	FILE *f     = open_file(fname);
	free(fname);

	fprintf(f, "%s = %" PRId64"\n", "current_buckets",    h->nbuckets);
	fprintf(f, "%s = %lf\n",        "current_minimum",    h->min_value);
	fprintf(f, "%s = %lf\n",        "current_maximum",    h->max_value);
	fprintf(f, "%s = %lf\n",        "current_mode",       h->value_at_max_count);
	fprintf(f, "%s = %" PRId64"\n", "current_mode_count", h->max_count);
	fprintf(f, "%s = %" PRId64"\n", "current_min_count",  h->min_count);
	fprintf(f, "%s = %lf\n",        "current_mean",       h->mean);
	fprintf(f, "%s = %lf\n",        "current_percentile75", value_of_p(h, 0.75));
	fprintf(f, "%s = %lf\n",        "current_percentile25", value_of_p(h, 0.25));
	fprintf(f, "%s = %" PRId64"\n", "current_first_allocation", h->first_allocation_time_dependence);
	fprintf(f, "%s = %" PRId64"\n", "current_first_allocation_time_independence", h->first_allocation_time_independence);
	fprintf(f, "%s = %lf\n",        "current_bin_size",   h->bin_size);

	if(all) {
		fprintf(f, "%s = %lf\n",        "all_minimum",    all->min_value);
		fprintf(f, "%s = %lf\n",        "all_maximum",    all->max_value);
		fprintf(f, "%s = %lf\n",        "all_mode",       all->value_at_max_count);
		fprintf(f, "%s = %" PRId64"\n", "all_mode_count", all->max_count);
		fprintf(f, "%s = %lf\n",        "all_mean",       all->mean);
		fprintf(f, "%s = %lf\n",        "all_percentile75", value_of_p(all, 0.75));
		fprintf(f, "%s = %lf\n",        "all_percentile25", value_of_p(all, 0.25));
		fprintf(f, "%s = %" PRId64"\n", "all_first_allocation", all->first_allocation_time_dependence);
		fprintf(f, "%s = %" PRId64"\n", "all_first_allocation_time_independence", h->first_allocation_time_independence);
	}

	fclose(f);
}

void write_thumbnail_gnuplot(struct histogram *h, struct histogram *all)
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


void write_image_gnuplot(struct histogram *h, struct histogram *all)
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

void write_images(struct histogram *h)
{
	pid_t pid;

	pid = fork();
	if(pid < 0)
	{
		fatal("Could not fork when creating thumbnail: %s\n", path_of_thumbnail_image(h, 0));
	}

	if(pid == 0) {
		char *path = string_format("%s/%s", output_directory, sanitize_path_name(h->source->category));
		chdir(path);
		execlp(gnuplot_path, "gnuplot", path_of_thumbnail_script(h, 1), NULL);
		fatal("Could not exec when creating thumbnail: %s\n", path_of_thumbnail_image(h, 0));
	}

	pid = fork();
	if(pid < 0)
	{
		fatal("Could not fork when creating image: %s\n", path_of_image(h, 0));
	}

	if(pid == 0) {
		char *path = string_format("%s/%s", output_directory, sanitize_path_name(h->source->category));
		chdir(path);
		execlp(gnuplot_path, "gnuplot", path_of_image_script(h, 1), NULL);
		fatal("Could not exec when creating image: %s\n", path_of_image(h, 0));
	}

	gnuplots_running += 2;
}

struct histogram *histogram_of_field(struct rmDsummary_set *source, struct field *f, char *out_dir)
{
	struct histogram *h = malloc(sizeof(struct histogram));

	h->total_count = list_size(source->summaries);
	h->summaries_sorted = malloc(h->total_count * sizeof(struct rmDsummary *));

	struct rmDsummary *s;
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
	set_skewdness_of_field(h,f);
	set_kurtosis_of_field(h, f);
	set_z_scores(h);

	itable_insert(source->histograms, (uint64_t) ((uintptr_t) f), (void *) h);

	debug(D_RMON, "%s-%s:\n buckets: %" PRIu64 " bin_size: %lf max_count: %" PRIu64 " mode: %lf\n", h->source->category, h->resource->caption, h->nbuckets, h->bin_size, h->max_count, h->value_at_max_count);

	return h;
}

void write_histogram_stats_header(FILE *stream)
{
	fprintf(stream, "%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s\n",
			"resource",
			"count",
			"mean", "std_dev",
			"max", "min", "first_alloc_w_time", "first_alloc_wo_time", "first_alloc_bf",
			"waste_max", "waste_w_time", "waste_wo_time", "waste_brute_force",
			"p_25", "p_50", "p_75", "p_95", "p_99"
		   );
}

void write_histogram_stats(FILE *stream, struct histogram *h)
{
	char *resource_no_spaces = sanitize_path_name(h->resource->name);
	fprintf(stream, "%s %d %.3lf %.3lf %.3lf %.3lf %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %g %g %g %g %g %.3lf %.3lf %.3lf %.3lf %.3lf\n",
			resource_no_spaces,
			h->total_count,
			h->mean, h->std_dev,
			h->max_value, h->min_value,
			h->first_allocation_time_dependence,
			h->first_allocation_time_independence,
			h->first_allocation_95,
			h->first_allocation_brute_force,
			h->waste_max,
			h->waste_time_dependence,
			h->waste_time_independence,
			h->waste_95,
			h->waste_brute_force,
			value_of_p(h, 0.25),
			value_of_p(h, 0.50),
			value_of_p(h, 0.75),
			value_of_p(h, 0.95),
			value_of_p(h, 0.99));

	free(resource_no_spaces);
}

void histograms_of_category(struct rmDsummary_set *ss)
{
	/* construct histograms of category across all resources */
	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(!f->active)
			continue;

		histogram_of_field(ss, f, output_directory);
	}

}

void plots_of_category(struct rmDsummary_set *s)
{
	struct histogram     *h;

	/* construct histograms of category across all resources */
	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));

		struct histogram *all = itable_lookup(all_summaries->histograms, (uint64_t) ((uintptr_t) f));
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

double total_waste(struct histogram *h, struct field *f, double first_alloc) {
	double waste   = 0;
	double wall_time_accum = 0;
	double max_candidate = h->max_value;

	int i;
#pragma omp parallel for private(i) reduction(+: wall_time_accum, waste)
	for(i = 0; i < h->total_count; i+=1) {
		double current   = value_of_field(h->summaries_sorted[i], f);

		double wall_time;
		if(f->cummulative) {
			wall_time = 1;
		} else {
			wall_time = h->summaries_sorted[i]->wall_time;
		}

		wall_time_accum += wall_time;

		double current_waste;
		if(current > first_alloc) {
			current_waste = (max_candidate - current + first_alloc)*wall_time;
		} else {
			current_waste = (first_alloc - current)*wall_time;
		}

		waste += current_waste;
	}

	waste /= wall_time_accum;

	return waste;
}

void set_category_maximum(struct rmDsummary_set *s, struct hash_table *categories) {
	struct field *f;
	struct histogram *h;

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
		rmsummary_assign_int_field(c->max_allocation, f->name, value);
	}
}

void set_first_allocation_time_dependence(struct rmDsummary_set *s, struct hash_table *categories) {
	struct field *f;
	struct histogram *h;

	struct category *c = category_lookup_or_create(categories, s->category);
	c->time_peak_independece = 0;

	category_update_first_allocation(categories, s->category);

	int i;
	for(i = WALL_TIME; i < NUM_FIELDS; i++)
	{
		f = (fields + i);

		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));

		h->first_allocation_time_dependence = -1;
		if(c->first_allocation) {
			int64_t first = rmsummary_get_int_field(c->first_allocation, f->name);
			h->first_allocation_time_dependence = rmsummary_to_external_unit(f->name, first);
			h->waste_time_dependence = total_waste(h, f, h->first_allocation_time_dependence);
		}
	}
}

void set_first_allocation_time_independence(struct rmDsummary_set *s, struct hash_table *categories) {
	struct field *f;
	struct histogram *h;

	struct category *c = category_lookup_or_create(categories, s->category);
	c->time_peak_independece = 1;

	category_update_first_allocation(categories, s->category);

	int i;
	for(i = WALL_TIME; i < NUM_FIELDS; i++)
	{
		f = (fields + i);

		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));

		h->first_allocation_time_independence = -1;
		if(c->first_allocation) {
			int64_t first = rmsummary_get_int_field(c->first_allocation, f->name);
			h->first_allocation_time_independence = rmsummary_to_external_unit(f->name, first);
			h->waste_time_independence = total_waste(h, f, h->first_allocation_time_independence);
		}
	}
}

void set_first_allocation_brute_force_field(struct histogram *h, struct field *f) {
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

	h->first_allocation_brute_force = min_candidate;
	h->waste_brute_force            = total_waste(h, f, min_candidate);
}

void set_first_allocation_brute_force(struct rmDsummary_set *s, struct hash_table *categories) {
	struct field *f;
	struct histogram *h;

	int i;
	for(i = WALL_TIME; i < NUM_FIELDS; i++)
	{
		f = (fields + i);

		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));

		if(brute_force) {
			set_first_allocation_brute_force_field(h, f);
		} else {
			h->first_allocation_brute_force = -1;
			h->waste_brute_force            = -1;
		}
	}
}

void set_first_allocation_95(struct rmDsummary_set *s, struct hash_table *categories) {
	struct field *f;
	struct histogram *h;

	int i;
	for(i = WALL_TIME; i < NUM_FIELDS; i++)
	{
		f = (fields + i);

		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));

		h->first_allocation_95 = value_of_p(h, 0.95);
		h->waste_95            = total_waste(h, f, h->first_allocation_95);
	}
}


void set_max_waste(struct rmDsummary_set *s, struct hash_table *categories) {
	struct field *f;
	struct histogram *h;

	int i;
	for(i = WALL_TIME; i < NUM_FIELDS; i++)
	{
		f = (fields + i);

		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));

		h->waste_max = total_waste(h, f, h->max_value);
	}
}

void set_first_allocations_of_category(struct rmDsummary_set *s, struct hash_table *categories) {

	set_category_maximum(s, categories);
	set_first_allocation_time_dependence(s, categories);
	set_first_allocation_time_independence(s, categories);
	set_first_allocation_brute_force(s, categories);
	set_first_allocation_95(s, categories);

	set_max_waste(s, categories);
}

void write_stats_of_category(struct rmDsummary_set *s)
{
	char *f_stats_raw   = sanitize_path_name(s->category);
	char *filename      = string_format("%s/%s.stats", output_directory, f_stats_raw);

	FILE *f_stats  = open_file(filename);

	free(f_stats_raw);
	free(filename);

	write_histogram_stats_header(f_stats);

	struct histogram *h;
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

void write_limits_of_category(struct rmDsummary_set *s, double p_cut)
{

	char *f_stats_raw   = sanitize_path_name(s->category);
	char *filename      = string_format("%s/%s.limits", output_directory, f_stats_raw);

	FILE *f_limits      = open_file(filename);

	free(filename);
	free(f_stats_raw);

	struct field *f;
	struct histogram *h;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(!f->active)
			continue;

		h = itable_lookup(s->histograms, (uint64_t) ((uintptr_t) f));

		fprintf(f_limits, "%s: %" PRIu64 "\n", f->name, (int64_t) ceil(value_of_p(h, p_cut)));
	}

	fclose(f_limits);
}

char *copy_outlier(struct rmDsummary *s)
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
		rmDsummary_print(output, s);
		fclose(output);
	} else {
		debug(D_NOTICE, "Could not create outlier summary: %s\n", outlier);
		outlier = NULL;
	}

	free(outlier);

	return base;
}


void write_outlier(FILE *stream, struct rmDsummary *s, struct field *f, char *prefix)
{
	char *outlier_name;

	outlier_name = copy_outlier(s);

	if(!outlier_name)
		return;

	if(!prefix)
	{
		prefix = "";
	}

	fprintf(stream, "<td class=\"data\">\n");
	fprintf(stream, "<a href=%s%s/%s>(%s)</a>", prefix, OUTLIER_DIR, outlier_name, s->task_id);
	fprintf(stream, "<br><br>\n");
	fprintf(stream, "%d\n", (int) value_of_field(s, f));
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

void write_webpage_stats_header(FILE *stream, struct histogram *h)
{
	fprintf(stream, "<td class=\"data\">%s", h->resource->caption);
	if(h->resource->units)
	{
		fprintf(stream, " (%s)", h->resource->units);
	}
	fprintf(stream, "</td>");

	fprintf(stream, "<td class=\"datahdr\" >mode <br> &#9653;</td>");
	fprintf(stream, "<td class=\"datahdr\" >&mu; <br> &#9643; </td>");
	fprintf(stream, "<td class=\"datahdr\" >1<sup>st</sup> alloc.<br> &#9663; </td>");
	fprintf(stream, "<td class=\"datahdr\" >1<sup>st</sup> alloc. ind.<br> &#9663; </td>");
	fprintf(stream, "<td class=\"datahdr\" >1<sup>st</sup> alloc. 0.95<br> &#9663; </td>");

	if(brute_force) {
		fprintf(stream, "<td class=\"datahdr\" >1<sup>st</sup> alloc. b.f.</td>");
	}

	fprintf(stream, "<td class=\"datahdr\" >&sigma;/&mu;</td>");
	fprintf(stream, "<td class=\"datahdr\" >p<sub>99</sub></td>");
	fprintf(stream, "<td class=\"datahdr\" >p<sub>95</sub></td>");
}

void write_webpage_stats(FILE *stream, struct histogram *h, char *prefix, int include_thumbnail)
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

	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, "%6.0lf\n", h->value_at_max_count);
	fprintf(stream, "</td>\n");

	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, "%6.0lf\n", h->mean);
	fprintf(stream, "</td>\n");

	fprintf(stream, "<td class=\"data\"> (w: %lf) <br><br>\n", h->waste_time_dependence);
	fprintf(stream, "%" PRId64 "\n", h->first_allocation_time_dependence);
	fprintf(stream, "</td>\n");

	fprintf(stream, "<td class=\"data\"> (w: %3.2lf ) <br><br>\n", h->waste_time_independence);
	fprintf(stream, "%" PRId64 "\n", h->first_allocation_time_independence);
	fprintf(stream, "</td>\n");

	fprintf(stream, "<td class=\"data\"> (w: %3.2lf ) <br><br>\n", h->waste_95);
	fprintf(stream, "%" PRId64 "\n", h->first_allocation_95);
	fprintf(stream, "</td>\n");

	if(brute_force) {
		fprintf(stream, "<td class=\"data\"> (w: %3.2lf) <br><br>\n", h->waste_brute_force);
		fprintf(stream, "%" PRId64 "\n", h->first_allocation_brute_force);
		fprintf(stream, "</td>\n");
	}

	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, "%6.2lf\n", h->mean > 0 ? h->std_dev/h->mean : -1);
	fprintf(stream, "</td>\n");

	struct rmDsummary *s;

	s = h->summaries_sorted[index_of_p(h, 0.99)];
	write_outlier(stream, s, f, prefix);

	s = h->summaries_sorted[index_of_p(h, 0.95)];
	write_outlier(stream, s, f, prefix);

}

void write_individual_histogram_webpage(struct histogram *h)
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
	struct rmDsummary *s;
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

	int columns = brute_force ? 9 : 8;

	if(!fo)
		fatal("Could not open file %s for writing: %s\n", strerror(errno));

	fprintf(fo, "<head>\n");

	fprintf(fo, "<title> %s </title>\n", workflow_name);
	write_css_style(fo);
	fprintf(fo, "</head>\n");

	fprintf(fo, "<body>\n");

	fprintf(fo, "<table>\n");

	fprintf(fo, "<tr>\n");

	struct rmDsummary_set *s;

	list_first_item(all_sets);
	while((s = list_next_item(all_sets)))
	{
		fprintf(fo, "<td class=\"datahdr\" colspan=\"%d\">%s: %d</td>", columns, s->category, list_size(s->summaries));
	}
	fprintf(fo, "</tr>\n");

	struct field *f;
	struct histogram      *h;
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
	write_front_page(workflow_name);

	struct rmDsummary_set *s;
	list_first_item(all_sets);
	while((s = list_next_item(all_sets)))
	{
		struct histogram *h;
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
				break;
			case 'm':
				/* brute force, small bucket size */
				category_tune_bucket_size("time",   USECOND);
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

	if(input_directory)
	{
		parse_summary_recursive(all_summaries, input_directory, categories);
	}

	if(input_list)
	{
		parse_summary_from_filelist(all_summaries, input_list, categories);
	}

	list_push_head(all_sets, all_summaries);

	if(list_size(all_summaries->summaries) > 0)
	{
		if(split_categories)
		{
			/* partition summaries on category name */
			split_summaries_on_category(all_summaries);
		}

		/* construct histograms across all categories/resources. */
		struct rmDsummary_set *s;
		list_first_item(all_sets);
		while((s = list_next_item(all_sets)))
		{
			histograms_of_category(s);
			set_first_allocations_of_category(s, categories);

			write_stats_of_category(s);
			write_limits_of_category(s, 0.95);

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
