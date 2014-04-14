#include "rmon_tools.h"
#include "create_dir.h"

#include "copy_stream.h"

#define MAX_LINE 1024

#define OUTLIER_DIR "outliers"
#define OUTLIER_N    5

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

	struct itable *buckets;
	uint64_t  nbuckets;

	char *output_directory;
};

struct list *all_sets;

struct rmDsummary *max_values;
struct rmDsummary_set *all_summaries;

struct hash_table *unique_strings;

struct time_series_entry *head;

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
		return h->bin_size = 1;
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
	char *resource = sanitize_path_name(h->resource->name);
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

	char *path = string_format("%s%s_%s", prefix, category, resource);

	free(resource);
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

void write_thumbnail_gnuplot(struct histogram *h, struct histogram *all)
{
	char *fname = path_of_thumbnail_script(h, 0);
	FILE *f     = open_file(fname);
	free(fname);

	fprintf(f, "set terminal pngcairo truecolor rounded size %d,%d enhanced font \"times,10\"\n", 
			width_thumb, height_thumb);

	fname = path_of_thumbnail_image(h, 1);
	fprintf(f, "set output \"%s\"\n", fname);
	free(fname);

	fprintf(f, "unset key\n");
	fprintf(f, "unset border\n");
	fprintf(f, "set style line 1 lc 16\n");
	fprintf(f, "set style fill solid noborder 0.45\n");
	fprintf(f, "set boxwidth 1.0*%lf absolute\n", h->bin_size);
	fprintf(f, "set tmargin 2\n");
	fprintf(f, "set bmargin 2\n");
	fprintf(f, "unset tics\n");

	fprintf(f, "set arrow from %lf,graph -0.01 to %lf,graph -0.01 nohead lc 16\n", 
			h->min_value, value_of_p(h, 0.25));
	fprintf(f, "set arrow from %lf,graph -0.01 to %lf,graph -0.01 nohead lc 16\n", 
			value_of_p(h, 0.75), h->max_value);
	fprintf(f, "set label \"\" at %lf,graph -0.01 tc ls 1 center front point pt 5\n", 
			value_of_p(h, 0.5));

	fprintf(f, "set label \"%.0lf\" at %lf,graph -0.01 tc ls 1 center front point pt 27 offset 0,character -0.90\n", h->value_at_max_count, h->value_at_max_count);


	if(h == all)
	{
		fprintf(f, "set label \"%.0lf\" at %lf,graph -0.01 tc ls 1 right front nopoint offset character -1.0,character -0.25\n", all->min_value, all->min_value);

		fprintf(f, "set label \"%.0lf\" at %lf,graph -0.01 tc ls 1 left front nopoint offset character 1.0,character -0.25\n", all->max_value, all->max_value);
	}

	if( all->nbuckets == 1 )
	{
		fprintf(f, "set xrange [%lf:%lf]\n", all->min_value - 1, all->max_value + 2);
	}
	else
	{
		double gap = (all->max_value - all->min_value)/5.0;
		fprintf(f, "set xrange [%lf:%lf]\n", all->min_value - gap, all->max_value + gap);
	}

	char *table_name = path_of_table(h, 1);

	if(h->max_count > 100*h->min_count && 0) 
	{
		fprintf(f, "set yrange [0:(log10(%lf))]\n", 1.0*h->max_count);
		fprintf(f, "set label \"%" PRIu64 "\" at %lf,(log10(%lf)) tc ls 1 left front nopoint offset 0,character 0.5\n",
				h->max_count, h->value_at_max_count, (double) h->max_count);

		fprintf(f, "plot \"%s\" using 1:(log10($2)) w boxes\n", table_name);
	}
	else
	{
		fprintf(f, "set yrange [0:%lf]\n", 1.0*h->max_count);
		fprintf(f, "set label \"%" PRIu64 "\" at %lf,%lf tc ls 1 left front nopoint offset 0,character 0.5\n",
				h->max_count, h->value_at_max_count, (double) h->max_count);

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

	fprintf(f, "set terminal pngcairo truecolor rounded size %d,%d enhanced font \"times,12\"\n", 
			width, height);

	fname = path_of_image(h, 1);
	fprintf(f, "set output \"%s\"\n", fname);
	free(fname);

	fprintf(f, "unset key\n");
	fprintf(f, "unset border\n");
	fprintf(f, "set style line 1 lc 16\n");
	fprintf(f, "set style fill solid noborder 0.45\n");
	fprintf(f, "set boxwidth 1.0*%lf absolute\n", h->bin_size);
	fprintf(f, "set tmargin 2\n");
	fprintf(f, "set bmargin 2\n");
	fprintf(f, "unset tics\n");

	fprintf(f, "set arrow from %lf,graph -0.01 to %lf,graph -0.01 nohead lc 16\n", 
			h->min_value, value_of_p(h, 0.25));
	fprintf(f, "set arrow from %lf,graph -0.01 to %lf,graph -0.01 nohead lc 16\n", 
			value_of_p(h, 0.75), h->max_value);
	fprintf(f, "set label \"\" at %lf,graph -0.01 tc ls 1 center front point pt 5\n", 
			value_of_p(h, 0.5));

	fprintf(f, "set label \"%.0lf\" at %lf,graph -0.01 tc ls 1 center front point pt 27 offset 0,character -0.90\n", h->value_at_max_count, h->value_at_max_count);

	fprintf(f, "set label \"%.0lf\" at %lf,graph -0.01 tc ls 1 right front nopoint offset character -1.0,character -0.25\n", all->min_value, all->min_value);

	fprintf(f, "set label \"%.0lf\" at %lf,graph -0.01 tc ls 1 left front nopoint offset character 1.0,character -0.25\n", all->max_value, all->max_value);

	if( all->nbuckets == 1 )
	{
		fprintf(f, "set xrange [%lf:%lf]\n", all->min_value - 1, all->max_value + 2);
	}
	else
	{
		double gap = (all->max_value - all->min_value)/5.0;
		fprintf(f, "set xrange [%lf:%lf]\n", all->min_value - gap, all->max_value + gap);
	}

	char *table_name = path_of_table(h, 1);
	if(h->max_count > 100*h->min_count && 0)
	{
		fprintf(f, "set yrange [0:(log10(%lf))]\n", 1.0*h->max_count);
		fprintf(f, "set label \"%" PRIu64 "\" at %lf,(log10(%lf)) tc ls 1 left front nopoint offset 0,character 0.5\n",
				h->max_count, h->value_at_max_count, (double) h->max_count);
		fprintf(f, "plot \"%s\" using 1:(log10($2)) w boxes\n", table_name);
	}
	else
	{
		fprintf(f, "set yrange [0:%lf]\n", 1.0*h->max_count);
		fprintf(f, "set label \"%" PRIu64 "\" at %lf,%lf tc ls 1 left front nopoint offset 0,character 0.5\n",
				h->max_count, h->value_at_max_count, (double) h->max_count);
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

	debug(D_DEBUG, "%s-%s:\n buckets: %" PRIu64 " bin_size: %lf max_count: %" PRIu64 " mode: %lf\n", h->source->category, h->resource->name, h->nbuckets, h->bin_size, h->max_count, h->value_at_max_count);

	return h;
}

void write_histogram_stats_header(FILE *stream)
{
	fprintf(stream, "%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s\n", 
			"resource", 
			"n", 
			"mean", "std_dev", "skewd", "kurtos", 
			"max", "min", 
			"p_25", "p_50", "p_75", "p_95", "p_99",
			"z_95", "z_99"
		   );
}

void write_histogram_stats(FILE *stream, struct histogram *h)
{
	char *resource_no_spaces = sanitize_path_name(h->resource->name);
	fprintf(stream, "%s %d %.3lf %.3lf %.3lf %.3lf %.3lf %.3lf %.3lf %.3lf %.3lf %.3lf %.3lf %.3lf %.3lf\n", 
			resource_no_spaces,
			h->total_count,
			h->mean, h->std_dev, h->skewdness, h->kurtosis,
			h->max_value, h->min_value,
			value_of_p(h, 0.25), 
			value_of_p(h, 0.50), 
			value_of_p(h, 0.75), 
			value_of_p(h, 0.95), 
			value_of_p(h, 0.99), 
			h->z_95,
			h->z_99);

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

		char *resource_no_spaces = sanitize_path_name(f->name);
		fprintf(f_limits, "%s: %" PRIu64 "\n", resource_no_spaces, (int64_t) ceil(value_of_p(h, p_cut)));
		free(resource_no_spaces);
	}

	fclose(f_limits);
}

char *copy_outlier(struct rmDsummary *s)
{
	char *base = NULL;
	char *outlier;

	if(s->file)
	{
		base = sanitize_path_name(s->file);

		outlier = string_format("%s/%s/%s", output_directory, OUTLIER_DIR, base);
		copy_file_to_file(s->file, outlier);
		free(outlier);
	}

	return base;
}


void write_outlier(FILE *stream, struct rmDsummary *s, struct field *f, char *prefix)
{
	char *outlier_name;

	outlier_name = copy_outlier(s);

	if(!prefix)
	{
		prefix = "";
	}

	fprintf(stream, "<td class=\"data\">\n");
	fprintf(stream, "<a href=%s%s/%s>(%" PRId64 ")</a>", prefix, OUTLIER_DIR, outlier_name, s->task_id);
	fprintf(stream, "<br><br>\n");
	fprintf(stream, "%6.0lf\n", value_of_field(s, f));
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
	fprintf(stream, "<td class=\"data\">%s", h->resource->name);
	if(h->resource->units)
	{
		fprintf(stream, " (%s)", h->resource->units);
	}
	fprintf(stream, "</td>");

	fprintf(stream, "<td class=\"datahdr\" >max</td>");
	fprintf(stream, "<td class=\"datahdr\" >p_99</td>");
	fprintf(stream, "<td class=\"datahdr\" >p_95</td>");
	fprintf(stream, "<td class=\"datahdr\" >p_50</td>");
	fprintf(stream, "<td class=\"datahdr\" >min</td>");
	fprintf(stream, "<td class=\"datahdr\" >mode</td>");
	fprintf(stream, "<td class=\"datahdr\" >&mu;</td>");
	fprintf(stream, "<td class=\"datahdr\" >&sigma;</td>");
	fprintf(stream, "<td class=\"datahdr\" >z_99</td>");
	fprintf(stream, "<td class=\"datahdr\" >z_95</td>");
}

void write_webpage_stats(FILE *stream, struct histogram *h, int include_thumbnail)
{
	struct field *f = h->resource;

	fprintf(stream, "<td>");
	if(include_thumbnail)
	{
		fprintf(stream, "<a href=\"../../%s\">",  path_of_page(h, 0));
		fprintf(stream, "<img src=\"../../%s\">", path_of_thumbnail_image(h, 0));
		fprintf(stream, "</a>");
	}
	fprintf(stream, "</td>");

	struct rmDsummary *s;
	s = h->summaries_sorted[h->total_count - 1];
	write_outlier(stream, s, f, NULL);

	s = h->summaries_sorted[index_of_p(h, 0.99)];
	write_outlier(stream, s, f, NULL);

	s = h->summaries_sorted[index_of_p(h, 0.95)];
	write_outlier(stream, s, f, NULL);

	s = h->summaries_sorted[index_of_p(h, 0.50)];
	write_outlier(stream, s, f, NULL);

	s = h->summaries_sorted[0];
	write_outlier(stream, s, f, NULL);

	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, "%6.0lf\n", h->value_at_max_count);
	fprintf(stream, "</td>\n");

	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, "%6.0lf\n", h->mean);
	fprintf(stream, "</td>\n");

	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, "%6.0lf\n", h->std_dev);
	fprintf(stream, "</td>\n");

	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, "%6.0lf\n", h->z_99);
	fprintf(stream, "</td>\n");

	fprintf(stream, "<td class=\"data\"> -- <br><br>\n");
	fprintf(stream, "%6.0lf\n", h->z_95);
	fprintf(stream, "</td>\n");

}

void write_individual_histogram_webpage(struct histogram *h)
{

	char *fname = path_of_page(h, 0);
	FILE *fo    = open_file(fname);
	free(fname);

	struct field *f = h->resource;

	fprintf(fo, "<head>\n");
	fprintf(fo, "<title> %s : %s </title>\n", h->source->category, f->name);
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
	write_webpage_stats(fo, h, 0);
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
		fprintf(fo, "<td class=\"datahdr\" colspan=\"11\">%s</td>", s->category);
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
			write_webpage_stats(fo, h, 1);
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

	int split_categories  = 0;

	unique_strings = hash_table_create(0, 0);

	debug_config(argv[0]);

	signed char c;
	while( (c = getopt(argc, argv, "D:d:f:hL:o:s")) > -1 )
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

	if(argc - optind > 1)
	{
		workflow_name    = argv[optind + 1];
	}
	else
	{
		workflow_name = output_directory;
	}

	all_sets = list_create();

	/* read and parse all input summaries */
	all_summaries = make_new_set(ALL_SUMMARIES_CATEGORY);

	if(input_directory)
	{
		parse_summary_recursive(all_summaries, input_directory);
	}

	if(input_list)
	{
		parse_summary_from_filelist(all_summaries, input_list);
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


