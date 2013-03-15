#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <math.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fts.h>

#include "debug.h"
#include "itable.h"
#include "list.h"
#include "xxmalloc.h"
#include "stringtools.h"
#include "getopt_aux.h"

#define MAX_LINE 1024

#define DEFAULT_MAX_CLUSTERS 4

#define RULE_PREFIX "log-rule-"
#define RULE_SUFFIX "-summary"

enum fields { WALL_TIME = 1, PROCESSES = 2, CPU_TIME = 4, VIRTUAL = 8, RESIDENT = 16, B_READ = 32, B_WRITTEN = 64, WDIR_FILES = 128, WDIR_FOOTPRINT = 256, RULE = 512 };

struct summary
{
	int    rule;

	double start;
	double end;

	double wall_time;
	double max_concurrent_processes;
	double cpu_time;
	double virtual_memory; 
	double resident_memory; 
	double bytes_read;
	double bytes_written;
	double workdir_number_files_dirs;
	double workdir_footprint;
	double fs_nodes;
};

struct cluster
{
	struct summary *centroid;
	struct summary *centroid_raw;             // before dividing by count

	int             count;

	struct cluster *left;
	struct cluster *right;

	double          internal_conflict;        // distance between left and right
	
};


int fields_flags = 511;

struct summary *max_values;

#define assign_field_summ(s, field, key, value)\
	if(!strcmp(key, #field)){\
		double val = strtod(value, NULL);\
		(s)->field = val;\
		continue;\
	}

int get_rule_number(char *filename)
{
	char  name[MAX_LINE];
	const char *base =  string_basename(filename);

	sscanf(base, RULE_PREFIX "%6c" RULE_SUFFIX, name);
	return atoi(name);
}
													
struct summary *parse_summary_file(char *filename)
{
	FILE           *f;
	char            line[MAX_LINE], key[MAX_LINE], value[MAX_LINE];

	struct summary *s = calloc(1, sizeof(struct summary));

	debug(D_DEBUG, "parsing summary %s\n", filename);

	f = fopen(filename, "r");
	if(!f)
		fatal("cluster: could not open summary: %s\n", filename);

	s->rule = get_rule_number(filename);

	debug(D_DEBUG, "rule %d\n", s->rule);

	while(fgets(line, MAX_LINE, f))
	{
		sscanf(line, "%[^:]:%s[\n]", key, value);
		assign_field_summ(s, max_concurrent_processes,  key, value);
		assign_field_summ(s, cpu_time,                  key, value);
		assign_field_summ(s, wall_time,                 key, value);
		assign_field_summ(s, virtual_memory,            key, value);
		assign_field_summ(s, resident_memory,           key, value);
		assign_field_summ(s, bytes_read,                key, value);
		assign_field_summ(s, bytes_written,             key, value);
		assign_field_summ(s, workdir_number_files_dirs, key, value);
		assign_field_summ(s, workdir_footprint,         key, value);

		assign_field_summ(s, start, key, value);
		assign_field_summ(s, end,   key, value);
	}

	fclose(f);

	return s;
}

void print_summary_file(FILE *stream, struct summary *s, int include_field)
{
	if( RULE           & fields_flags ) fprintf(stream, "%d ", s->rule);
	if( WALL_TIME      & fields_flags ) fprintf(stream, "%s%lf ", include_field ? "t: " : "", s->wall_time);
	if( PROCESSES      & fields_flags ) fprintf(stream, "%s%lf ", include_field ? "p: " : "", s->max_concurrent_processes);
	if( CPU_TIME       & fields_flags ) fprintf(stream, "%s%lf ", include_field ? "c: " : "", s->cpu_time);
	if( VIRTUAL        & fields_flags ) fprintf(stream, "%s%lf ", include_field ? "v: " : "", s->virtual_memory);
	if( RESIDENT       & fields_flags ) fprintf(stream, "%s%lf ", include_field ? "m: " : "", s->resident_memory);
	if( B_READ         & fields_flags ) fprintf(stream, "%s%lf ", include_field ? "r: " : "", s->bytes_read);
	if( B_WRITTEN      & fields_flags ) fprintf(stream, "%s%lf ", include_field ? "w: " : "", s->bytes_written);
	if( WDIR_FILES     & fields_flags ) fprintf(stream, "%s%lf ", include_field ? "n: " : "", s->workdir_number_files_dirs);
	if( WDIR_FOOTPRINT & fields_flags ) fprintf(stream, "%s%lf ", include_field ? "z: " : "", s->workdir_footprint);
	fprintf(stream, "\n");
}

struct list *parse_summary_recursive(char *dirname)
{

	FTS *hierarchy;
	FTSENT *entry;
	char *argv[] = {dirname, NULL};

	struct list *summaries = list_create(0);

	hierarchy = fts_open(argv, FTS_PHYSICAL, NULL);

	if(!hierarchy)
		fatal("fts_open error: %s\n", strerror(errno));

	struct summary *s;
	while( (entry = fts_read(hierarchy)) )
		if( strstr(entry->fts_name, RULE_SUFFIX) )
		{
			s = parse_summary_file(entry->fts_accpath);
			list_push_head(summaries, (void *) s);
		}

	fts_close(hierarchy);

	return summaries;
}

#define assign_field_max(max, s, field)\
	if((max)->field < (s)->field){\
		(max)->field = (s)->field;\
	}

struct summary *find_max_summary(struct list *summaries)
{
	struct summary *s;
	struct summary *max = calloc(1, sizeof(struct summary));

	list_first_item(summaries);
	while( (s = list_next_item(summaries)) )
	{
		assign_field_max(max, s, wall_time);
		assign_field_max(max, s, max_concurrent_processes);
		assign_field_max(max, s, cpu_time);
		assign_field_max(max, s, virtual_memory);
		assign_field_max(max, s, resident_memory);
		assign_field_max(max, s, bytes_read);
		assign_field_max(max, s, bytes_written);
		assign_field_max(max, s, workdir_number_files_dirs);
		assign_field_max(max, s, workdir_footprint);
	}

	return max;
}

#define normalize_field(max, s, field)\
	if((max)->field > 0){\
		(s)->field /= (max)->field;\
	}

void normalize_summary(struct summary *s)
{
		normalize_field(max_values, s, wall_time);
		normalize_field(max_values, s, max_concurrent_processes);
		normalize_field(max_values, s, cpu_time);
		normalize_field(max_values, s, virtual_memory);
		normalize_field(max_values, s, resident_memory);
		normalize_field(max_values, s, bytes_read);
		normalize_field(max_values, s, bytes_written);
		normalize_field(max_values, s, workdir_number_files_dirs);
		normalize_field(max_values, s, workdir_footprint);
}

void normalize_summaries(struct list *summaries)
{
	struct summary *s;
	list_first_item(summaries);
	while( (s = list_next_item(summaries)) )
		normalize_summary(s);
}

#define denormalize_field(max, s, field)\
	if((max)->field > 0){\
		(s)->field *= (max)->field;\
	}

void denormalize_summary(struct summary *s)
{
		denormalize_field(max_values, s, wall_time);
		denormalize_field(max_values, s, max_concurrent_processes);
		denormalize_field(max_values, s, cpu_time);
		denormalize_field(max_values, s, virtual_memory);
		denormalize_field(max_values, s, resident_memory);
		denormalize_field(max_values, s, bytes_read);
		denormalize_field(max_values, s, bytes_written);
		denormalize_field(max_values, s, workdir_number_files_dirs);
		denormalize_field(max_values, s, workdir_footprint);
}

void denormalize_summaries(struct list *summaries)
{
	struct summary *s;
	list_first_item(summaries);
	while( (s = list_next_item(summaries)) )
		denormalize_summary(s);
}

#define assign_field_bin(s, a, b, op, field)\
	(s)->field = op((a)->field, (b)->field)

struct summary *summary_bin_op(struct summary *s, struct summary *a, struct summary *b, double (*op)(double, double))
{

	assign_field_bin(s, a, b, op, wall_time);
	assign_field_bin(s, a, b, op, max_concurrent_processes);
	assign_field_bin(s, a, b, op, cpu_time);
	assign_field_bin(s, a, b, op, virtual_memory);
	assign_field_bin(s, a, b, op, resident_memory);
	assign_field_bin(s, a, b, op, bytes_read);
	assign_field_bin(s, a, b, op, bytes_written);
	assign_field_bin(s, a, b, op, workdir_number_files_dirs);
	assign_field_bin(s, a, b, op, workdir_footprint);

	return s;
}

#define assign_field_unit(s, a, u, op, field)\
	(s)->field = op((a)->field, u);

struct summary *summary_unit_op(struct summary *s, struct summary *a, double u, double (*op)(double, double))
{

	assign_field_unit(s, a, u, op, wall_time);
	assign_field_unit(s, a, u, op, max_concurrent_processes);
	assign_field_unit(s, a, u, op, cpu_time);
	assign_field_unit(s, a, u, op, virtual_memory);
	assign_field_unit(s, a, u, op, resident_memory);
	assign_field_unit(s, a, u, op, bytes_read);
	assign_field_unit(s, a, u, op, bytes_written);
	assign_field_unit(s, a, u, op, workdir_number_files_dirs);
	assign_field_unit(s, a, u, op, workdir_footprint);

	return s;
}

double plus(double a, double b)
{
	return a + b;
}

double minus(double a, double b)
{
	return a - b;
}

double mult(double a, double b)
{
	return a * b;
}

double minus_squared(double a, double b)
{
	return pow(a - b, 2);
}

double divide(double a, double b)
{
	return a/b;
}

double summary_accumulate(struct summary *s)
{
	double acc = 0;

	if( WALL_TIME      & fields_flags ) acc += s->wall_time;
	if( PROCESSES      & fields_flags ) acc += s->max_concurrent_processes;
	if( CPU_TIME       & fields_flags ) acc += s->cpu_time;
	if( VIRTUAL        & fields_flags ) acc += s->virtual_memory;
	if( RESIDENT       & fields_flags ) acc += s->resident_memory;
	if( B_READ         & fields_flags ) acc += s->bytes_read;
	if( B_WRITTEN      & fields_flags ) acc += s->bytes_written;
	if( WDIR_FILES     & fields_flags ) acc += s->workdir_number_files_dirs;
	if( WDIR_FOOTPRINT & fields_flags ) acc += s->workdir_footprint;

	return acc;
}

double summary_euclidean(struct summary *a, struct summary *b)
{
	struct summary *s = calloc(1, sizeof(struct summary));

	summary_bin_op(s, a, b, minus_squared);

	double accum = summary_accumulate(s);

	free(s);

	return sqrt(accum);
}

double cluster_ward_distance(struct cluster *a, struct cluster *b)
{
	struct summary *s = calloc(1, sizeof(struct summary));
	
	summary_bin_op(s, a->centroid, b->centroid, minus_squared);

	double accum = summary_accumulate(s);

	accum /= (1.0/a->count + 1.0/b->count);

	free(s);

	return accum;
}

struct cluster *cluster_nearest_neighbor(struct itable *active_clusters, struct cluster *c)
{
	uint64_t        ptr;
	struct cluster *nearest = NULL;
	struct cluster *other;
	double          dmin, dtest;

	itable_firstkey(active_clusters);
	while( itable_nextkey( active_clusters, &ptr, (void *) &other ) )
	{
		dtest = cluster_ward_distance(c, other);

		if( !nearest || dtest < dmin )
		{
			dmin    = dtest;
			nearest = other;
		}
	}

	return nearest; 
}

struct summary *cluster_find_centroid(struct cluster *c)
{
	struct summary *s = calloc(1, sizeof(struct summary));
	struct summary *r = calloc(1, sizeof(struct summary));

	s->rule = -1;
	r->rule = -1;

	summary_bin_op(r, c->left->centroid_raw, c->right->centroid_raw, plus);
	summary_unit_op(s, r, (double) c->count, divide);

	c->centroid_raw = r;
	c->centroid     = s;

	return s;
}

struct cluster *cluster_create(struct summary *s)
{
	struct cluster *c = calloc(1, sizeof(struct cluster));

	c->centroid     = s;
	c->centroid_raw = s;
	c->count        = 1;

	return c;
}

/* We keep track of cluster merges in a tree structure. The
 * centroids of leaves are the actual data clustered. */
struct cluster *cluster_merge(struct cluster *left, struct cluster *right)
{
	struct cluster *c = calloc(1, sizeof(struct cluster));

	c->count = left->count + right->count;
	c->left  = left;
	c->right = right;

	c->internal_conflict = cluster_ward_distance(left, right);

    cluster_find_centroid(c);

	return c;
}

static int summary_cmp_rule(const void *a, const void *b)
{
	const struct summary **sa = (void *) a;
	const struct summary **sb = (void *) b;

	return ((*sa)->rule - (*sb)->rule); 
}

void cluster_collect_summaries_recursive(struct cluster *c, struct list *accum)
{
	if( !c->right && !c->left)
		list_push_head(accum, c->centroid);

	if( c->left )
		cluster_collect_summaries_recursive(c->left, accum);

	if( c->right )
		cluster_collect_summaries_recursive(c->right, accum);
}

struct list *cluster_collect_summaries(struct cluster *c)
{
	struct list *summaries = list_create(0);

	cluster_collect_summaries_recursive(c, summaries);

			//BUG: rules incorrectly sorted
	summaries = list_sort(summaries, summary_cmp_rule);

	return summaries;
}


struct cluster *nearest_neighbor_clustering(struct list *initial_clusters)
{
	struct cluster *top, *closest, *subtop;
	struct list   *stack;
	struct itable *active_clusters;
	double dclosest, dsubtop;

	int merge = 0;

	list_first_item(initial_clusters);
	top = list_next_item(initial_clusters);

	/* Return immediately if top is NULL, or there is a unique
	 * initial cluster */
	if(list_size(initial_clusters) < 2)
		return top;

	stack = list_create(0);
	list_push_head(stack, top);

	/* Add all of the initial clusters as active clusters. */
	active_clusters = itable_create(0);
	while( (top = list_next_item(initial_clusters)) ) 
		itable_insert(active_clusters, (uintptr_t) top, (void *) top);

	do
	{
		/* closest might be NULL if all of the clusters are in
		 * the stack now. subtop might be NULL if top was the
		 * only cluster in the stack */
		top     = list_pop_head( stack );
		closest = cluster_nearest_neighbor(active_clusters, top);
		subtop  = list_peek_head( stack );

		dclosest = -1;
		dsubtop  = -1;

		if(closest)
			dclosest = cluster_ward_distance(top, closest);

		if(subtop)
			dsubtop = cluster_ward_distance(top, subtop);

		/* The nearest neighbor of top is either one of the
		 * remaining active clusters, or the second topmost
		 * cluster in the stack */
		if( closest && subtop )
		{
			/* Use pointer address to systematically break ties. */
			if(dclosest < dsubtop || ((dclosest == dsubtop) && (uintptr_t)closest < (uintptr_t)subtop)) 
				merge = 0;
			else 
				merge = 1;
		}
		else if( subtop )
			merge = 1;
		else if( closest )
			merge = 0;
		else
			fatal("Zero clusters?\n"); //We should never reach here.

		if(merge)
		{
			/* If the two topmost clusters in the stack are
			 * mutual nearest neighbors, merge them into a single
			 * cluster */
			subtop = list_pop_head( stack );
			list_push_head(stack, cluster_merge(top, subtop));
		}
		else
		{
			/* Otherwise, push the nearest neighbor of top to the
			 * stack */
			itable_remove(active_clusters, (uintptr_t) closest);
			list_push_head(stack, top);
			list_push_head(stack, closest);
		}

		debug(D_DEBUG, "stack: %d  active: %d  closest: %lf subtop: %lf\n", 
				list_size(stack), itable_size(active_clusters), dclosest, dsubtop);

		/* If there are no more active_clusters, but there is not
		 * a single cluster in the stack, we try again,
		 * converting the clusters in the stack into new active
		 * clusters. */
		if(itable_size(active_clusters) == 0 && list_size(stack) > 3)
		{
			itable_delete(active_clusters);
			return nearest_neighbor_clustering(stack);
		}

	}while( !(itable_size(active_clusters) == 0 && list_size(stack) == 1) );

	/* top is now the root of a cluster hierarchy, of
	 * cluster->right, cluster->left. */
	top = list_pop_head(stack);

	list_delete(stack);
	itable_delete(active_clusters);

	return top;
}

struct list *collect_final_clusters(struct cluster *final, int max_clusters)
{
	int count;
	struct list *clusters, *clusters_next;
	struct cluster *c, *cmax;
	double dmax;

	clusters = list_create(0);
	list_push_head(clusters, final);

	/* At each count, we split the cluster with the maximal
	 * distance between left and right. The iteration stops when
	 * the maximum number of clusters is reached, or when no more
	 * clusters can be split. */
	for(count = 1; count < max_clusters && count == list_size(clusters); count++)
	{
		clusters_next = list_create(0);
		list_first_item(clusters);

		cmax = NULL;
		dmax = 0;
				
		list_first_item(clusters);
		while( (c = list_next_item(clusters)) )
		{
			/* Find out if we need to split this cluster for next
			 * round */
			if(!cmax || c->internal_conflict > dmax)
			{
				dmax = c->internal_conflict;
				cmax = c;
			}
		}

		/* Iterate through the clusters again. If the cluster has
		 * the maximal internal conflict, add its left and right
		 * to the next iteration, otherwise add the cluster to
		 * the next iteration with no change. */
		list_first_item(clusters);
		while( (c = list_next_item(clusters)) )
		{
			if(c == cmax)
			{
				if( c->right ) 
					list_push_tail(clusters_next, c->right);

				if( c->left ) 
					list_push_tail(clusters_next, c->left);

				if( !c->left || !c->right ) 
					list_push_tail(clusters_next, c);
			}
			else
					list_push_tail(clusters_next, c);
		}

		list_delete(clusters);
		clusters = clusters_next;
	}

	return clusters;
}

void report_clusters_centroids(FILE *freport, struct list *clusters)
{
	struct cluster *c;

	list_first_item(clusters);
	while( (c = list_next_item(clusters)) )
	{
		fprintf(freport, "%d ", c->count);
		print_summary_file(freport, c->centroid, 0);
	}

}

#define add_plot_column(stream, column, flag, title)\
	if( flag & fields_flags )\
	{\
		if(column == 2)\
			fprintf(stream, " using %d:xticlabels(1) title '%s'", column, title);\
		else\
			fprintf(stream, ", '' using %d title '%s'", column, title);\
		column++;\
	}


/* It would be nice to have a function flag to title */
void report_clusters_histograms(char *plot_cmd_file, char *clusters_file)
{
	FILE *fplot;

	fplot = fopen(plot_cmd_file, "w");
	if(!fplot)
		fatal("cannot open file for plot command.\n");

	int column = 2;

	fprintf(fplot, "plot '%s' ", clusters_file);

	add_plot_column(fplot, column, WALL_TIME,      "wall time");      
	add_plot_column(fplot, column, PROCESSES,      "concurrent processes");
	add_plot_column(fplot, column, CPU_TIME,       "cpu time");      
	add_plot_column(fplot, column, VIRTUAL,        "virtual memory");       
	add_plot_column(fplot, column, RESIDENT,       "resident memory");
	add_plot_column(fplot, column, B_READ,         "bytes read");        
	add_plot_column(fplot, column, B_WRITTEN,      "bytes written");     
	add_plot_column(fplot, column, WDIR_FILES,     "inodes");    
	add_plot_column(fplot, column, WDIR_FOOTPRINT, "disk footprint");
}

void report_clusters_rules(FILE *freport, struct list *clusters)
{
	struct cluster *c;
	int i = 1;

	fprintf(freport, "# %d clusters ------\n", list_size(clusters));

	list_first_item(clusters);
	
	while( (c = list_next_item(clusters)) )
	{
		/* Centroids are denormalized just for show, so that the
		 * report shows the actual units. */
		denormalize_summary(c->centroid);

		/* Print cluster header and centroid */
		fprintf(freport, "cluster %d count %d \ncenter ", i++, c->count);
		print_summary_file(freport, c->centroid, 1);

		/* Print rule numbers in this cluster */
		fprintf(freport, "rules ");
		struct list *summaries = cluster_collect_summaries(c);
		struct summary *s;
		list_first_item(summaries);
		while( (s = list_next_item(summaries)) )
			fprintf(freport, "%d ", s->rule);
		list_delete(summaries);
		fprintf(freport, "\n\n");

		/* Normalize again. Is the precision we are losing significant? */
		normalize_summary(c->centroid);
	}

	list_delete(clusters);
}

struct list *create_initial_clusters(struct list *summaries)
{
	struct summary *s;
	struct list *initial_clusters = list_create(0);

	list_first_item(summaries);
	while( (s = list_next_item(summaries)) )
		list_push_head(initial_clusters, cluster_create(s));

	return initial_clusters;
}

int parse_fields_options(char *field_str)
{
	char *c =  field_str;
	int flags = 0;

	while( *c != '\0' )
	{
		switch(*c)
		{
			case 't':
				flags |= WALL_TIME;
				break;
			case 'p':
				flags |= PROCESSES;
				break;
			case 'c':
				flags |= CPU_TIME;
				break;
			case 'v':
				flags |= VIRTUAL;
				break;
			case 'm':
				flags |= RESIDENT;
				break;
			case 'r':
				flags |= B_READ;
				break;
			case 'w':
				flags |= B_WRITTEN;
				break;
			case 'n':
				flags |= WDIR_FILES;
				break;
			case 'z':
				flags |= WDIR_FOOTPRINT;
				break;
			default:
				fprintf(stderr, "%c is not an option\n", *c);
				break;
		}
		c++;
	}

	return flags;
}

int main(int argc, char **argv)
{
	char           *report_filename = NULL;
	char           *input_directory;
	FILE           *freport;
	int             max_clusters = DEFAULT_MAX_CLUSTERS; 
	struct list    *summaries, *initial_clusters, *final_clusters;
	struct cluster *final;

	if(argc < 2)
	{
				/* BUG: show usage here */
		fatal("Need an input directory.\n");
	}

	debug_config(argv[0]);

	char c;
	while( (c = getopt(argc, argv, "d:f:m:o:")) >= 0 )
	{
		switch(c)
		{
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'f':
				fields_flags = parse_fields_options(optarg);
				break;
			case 'm':
				max_clusters = atoi(optarg);
				if(max_clusters < 1)
					fatal("The number of clusters cannot be less than one.\n");
				break;
			case 'o':
				report_filename = xxstrdup(optarg);
				break;
			default:
				/* BUG: show usage here */
				fatal("%c is not a valid option.\n", c);
				break;
		}
	}

	if(optind >= argc)
	{
		/* BUG: show usage here */
		fatal("Need an input directory.\n");
	}
	else
		input_directory = argv[optind];

	if(!report_filename)
		report_filename = xxstrdup("clusters.txt");

	freport = fopen(report_filename, "w");
	if(!freport)
		fatal("%s: %s\n", report_filename, strerror(errno));


	summaries = parse_summary_recursive(input_directory);

	max_values = find_max_summary(summaries);

	normalize_summaries(summaries);

	initial_clusters = create_initial_clusters(summaries);

	final = nearest_neighbor_clustering(initial_clusters);

	final_clusters = collect_final_clusters(final, max_clusters);

	report_clusters_centroids(freport, final_clusters);

	report_clusters_histograms("clusters_plot_cmd", report_filename);

	denormalize_summaries(summaries);

	fclose(freport);

	return 0;
}


