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
#include "path.h"
#include "getopt_aux.h"

#define MAX_LINE 1024

#define DEFAULT_MAX_CLUSTERS 4

#define RULE_PREFIX "log-rule-"
#define RULE_SUFFIX "-summary"

#define NUM_FIELDS 10

#define field_flag(k) (((unsigned int) 1) << (k - 1))

enum fields      { WALL_TIME = 1, PROCESSES, CPU_TIME, VIRTUAL, RESIDENT, B_READ, B_WRITTEN, WDIR_FILES, WDIR_FOOTPRINT, RULE};

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

	double covariance[NUM_FIELDS][NUM_FIELDS];

	int             count;

	struct cluster *left;
	struct cluster *right;

	double          internal_conflict;        // distance between left and right
	
};

int fields_flags = 511; /* all but rule number */

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
	const char *base =  path_basename(filename);

	sscanf(base, RULE_PREFIX "%6c" RULE_SUFFIX, name);
	return atoi(name);
}
													
struct summary *parse_summary_file(char *filename)
{
	FILE           *f;
	char            line[MAX_LINE], key[MAX_LINE], value[MAX_LINE];

	struct summary *s = calloc(1, sizeof(struct summary));

	debug(D_RMON, "parsing summary %s\n", filename);

	f = fopen(filename, "r");
	if(!f)
		fatal("cluster: could not open summary: %s\n", filename);

	s->rule = get_rule_number(filename);

	debug(D_RMON, "rule %d\n", s->rule);

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
	if( field_flag(RULE)           & fields_flags ) fprintf(stream, "%d ", s->rule);
	if( field_flag(WALL_TIME)      & fields_flags ) fprintf(stream, "%s%10lf ", include_field ? "t: " : "", s->wall_time);
	if( field_flag(PROCESSES)      & fields_flags ) fprintf(stream, "%s%10lf ", include_field ? "p: " : "", s->max_concurrent_processes);
	if( field_flag(CPU_TIME)       & fields_flags ) fprintf(stream, "%s%10lf ", include_field ? "c: " : "", s->cpu_time);
	if( field_flag(VIRTUAL)        & fields_flags ) fprintf(stream, "%s%10lf ", include_field ? "v: " : "", s->virtual_memory);
	if( field_flag(RESIDENT)       & fields_flags ) fprintf(stream, "%s%10lf ", include_field ? "m: " : "", s->resident_memory);
	if( field_flag(B_READ)         & fields_flags ) fprintf(stream, "%s%10lf ", include_field ? "r: " : "", s->bytes_read);
	if( field_flag(B_WRITTEN)      & fields_flags ) fprintf(stream, "%s%10lf ", include_field ? "w: " : "", s->bytes_written);
	if( field_flag(WDIR_FILES)     & fields_flags ) fprintf(stream, "%s%10lf ", include_field ? "n: " : "", s->workdir_number_files_dirs);
	if( field_flag(WDIR_FOOTPRINT) & fields_flags ) fprintf(stream, "%s%10lf ", include_field ? "z: " : "", s->workdir_footprint);
	fprintf(stream, "\n");
}

void print_covariance_matrix(FILE *stream, double covariance[NUM_FIELDS][NUM_FIELDS])
{
	enum fields row, col;

	fprintf(stream, "#   ");

	if( field_flag(WALL_TIME)      & fields_flags ) fprintf(stream, "%10s ", "t ");
	if( field_flag(PROCESSES)      & fields_flags ) fprintf(stream, "%10s ", "p ");
	if( field_flag(CPU_TIME)       & fields_flags ) fprintf(stream, "%10s ", "c ");
	if( field_flag(VIRTUAL)        & fields_flags ) fprintf(stream, "%10s ", "v ");
	if( field_flag(RESIDENT)       & fields_flags ) fprintf(stream, "%10s ", "m ");
	if( field_flag(B_READ)         & fields_flags ) fprintf(stream, "%10s ", "r ");
	if( field_flag(B_WRITTEN)      & fields_flags ) fprintf(stream, "%10s ", "w ");
	if( field_flag(WDIR_FILES)     & fields_flags ) fprintf(stream, "%10s ", "n ");
	if( field_flag(WDIR_FOOTPRINT) & fields_flags ) fprintf(stream, "%10s ", "z ");

	fprintf(stream, "\n");

	/* To NUM_FIELDS - 1 because we do not want RULE */
	for(row = 1; row < NUM_FIELDS; row++)
	{
		if(!(field_flag(row) & fields_flags))
			continue;
	
		switch(row)
		{
			case WALL_TIME:      fprintf(stream, "%s ", "# t "); break; 
			case PROCESSES:      fprintf(stream, "%s ", "# p "); break;
			case CPU_TIME:       fprintf(stream, "%s ", "# c "); break;
			case VIRTUAL:        fprintf(stream, "%s ", "# v "); break;
			case RESIDENT:       fprintf(stream, "%s ", "# m "); break;
			case B_READ:         fprintf(stream, "%s ", "# r "); break;
			case B_WRITTEN:      fprintf(stream, "%s ", "# w "); break;
			case WDIR_FILES:     fprintf(stream, "%s ", "# n "); break;
			case WDIR_FOOTPRINT: fprintf(stream, "%s ", "# z "); break;
			case RULE:                                           break;
		}

		for(col = 1; col < NUM_FIELDS; col++)
		{
			if(!(field_flag(col) & fields_flags))
				continue;
			fprintf(stream, "%10lf ", covariance[row][col]);
		}

		fprintf(stream, "\n");
	}

	fprintf(stream, "# \n");
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

	if( field_flag(WALL_TIME)      & fields_flags ) acc += s->wall_time;
	if( field_flag(PROCESSES)      & fields_flags ) acc += s->max_concurrent_processes;
	if( field_flag(CPU_TIME)       & fields_flags ) acc += s->cpu_time;
	if( field_flag(VIRTUAL)        & fields_flags ) acc += s->virtual_memory;
	if( field_flag(RESIDENT)       & fields_flags ) acc += s->resident_memory;
	if( field_flag(B_READ)         & fields_flags ) acc += s->bytes_read;
	if( field_flag(B_WRITTEN)      & fields_flags ) acc += s->bytes_written;
	if( field_flag(WDIR_FILES)     & fields_flags ) acc += s->workdir_number_files_dirs;
	if( field_flag(WDIR_FOOTPRINT) & fields_flags ) acc += s->workdir_footprint;

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

	free(s);

	return accum;
}

struct cluster *cluster_nearest_neighbor(struct itable *active_clusters, struct cluster *c, double (*cmp)(struct cluster *, struct cluster *))
{
	uint64_t        ptr;
	struct cluster *nearest = NULL;
	struct cluster *other;
	double          dmin, dtest;

	itable_firstkey(active_clusters);
	while( itable_nextkey( active_clusters, &ptr, (void *) &other ) )
	{
		dtest = cmp(c, other);

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

	bzero(c->covariance, sizeof(double) * NUM_FIELDS * NUM_FIELDS);

	return c;
}

/* Merge the covariance of fields x and y, from clusters A and B.
 * Prefixes: u -> median, s -> covariance */
double covariance_scalar_merge(double uxA, double uyA, double sA, int nA, double uxB, double uyB, double sB, int nB)
{
	/* X = A \cup B */
	double sX;

	sX = sA + sB + (uxA - uxB)*(uyA - uyB)*(((double) nA)*((double) nB) / ((double) nA + (double) nB));

	return sX;
}

/* This code is ugly. Need better translation between fields and
 * row/column numbers. */
double n_to_field(struct summary *s, enum fields n)
{
	switch(n)
	{
		case WALL_TIME:
			return s->wall_time;
			break;
		case PROCESSES:
			return s->max_concurrent_processes;
			break;
		case CPU_TIME:
			return s->cpu_time;
			break;
		case VIRTUAL:
			return s->virtual_memory;
			break;
		case RESIDENT:
			return s->resident_memory;
			break;
		case B_READ:
			return s->bytes_read;
			break;
		case B_WRITTEN:
			return s->bytes_written;
			break;
		case WDIR_FILES:
			return s->workdir_number_files_dirs;
			break;
		case WDIR_FOOTPRINT:
			return s->workdir_footprint;
			break;
		case RULE:
			return s->rule;
			break;
	}

	return 0;
}

void covariance_matrix_merge(struct cluster *c, struct cluster *left, struct cluster *right)
{
	enum fields row, col;
	struct summary *uA, *uB;

	double uxA, uxB;
	double uyA, uyB;
	double sA,  sB;
	int    nA,  nB;

	uA = left->centroid;
	uB = right->centroid;

	nA = left->count;
	nB = right->count;

	/* To NUM_FIELDS - 1 because we do not want RULE */
	for(row = 1; row < NUM_FIELDS; row++)
	{
		if(!(field_flag(row) & fields_flags))
			continue;

		uxA = n_to_field(uA, row);
		uxB = n_to_field(uB, row);

		for(col = row; col < NUM_FIELDS; col++)
		{
			if(!(field_flag(col) & fields_flags))
				continue;

			uyA = n_to_field(uA, col);
			uyB = n_to_field(uB, col);

			sA  = left->covariance[row][col];
			sB  = right->covariance[row][col];

			c->covariance[row][col] = covariance_scalar_merge(uxA, uyA, sA, nA, uxB, uyB, sB, nB);

			c->covariance[col][row] = c->covariance[row][col];
		}
	}
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

	covariance_matrix_merge(c, left, right);

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


struct cluster *nearest_neighbor_clustering(struct list *initial_clusters, double (*cmp)(struct cluster *, struct cluster *))
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
		closest = cluster_nearest_neighbor(active_clusters, top, cmp);
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
			return nearest_neighbor_clustering(stack, cmp);
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

	/* Print the covariance matrices as comments */
	list_first_item(clusters);
	while( (c = list_next_item(clusters)) )
	{
		print_covariance_matrix(freport, c->covariance);
	}

	/* Print the centroids as actual data */
	list_first_item(clusters);
	while( (c = list_next_item(clusters)) )
	{
		fprintf(freport, "%-4d ", c->count);
		print_summary_file(freport, c->centroid, 0);
	}

	/* We print two blank lines to signal the end of the data set
	 * in gnuplot */
	fprintf(freport, "\n\n\n");
}

#define add_plot_column(stream, column, flag, title)\
	if( flag & fields_flags )\
	{\
		if(column == 2)\
			fprintf(stream, " index clusters_index using %d:xticlabels(1) title '%s'", column, title);\
		else\
			fprintf(stream, ", '' index clusters_index using %d title '%s'", column, title);\
		column++;\
	}


/* It would be nice to have a function flag to title */
void report_clusters_histograms(char *plot_cmd_file, char *clusters_file, int max_clusters)
{
	FILE *fplot;
	int   column;

	fplot = fopen(plot_cmd_file, "w");
	if(!fplot)
		fatal("cannot open file for plot command.\n");

		column = 2;

		fprintf(fplot, "foutput = sprintf(\"clusters_%%03d.jpg\", 1 + clusters_index)\n");
		fprintf(fplot, "set terminal push\n");
		fprintf(fplot, "set terminal jpeg size 1024,768\n");
		fprintf(fplot, "set output foutput\n");
		fprintf(fplot, "set multiplot\n");
		fprintf(fplot, "plot '%s' ", clusters_file);
		add_plot_column(fplot, column, field_flag(WALL_TIME),      "wall time");      
		add_plot_column(fplot, column, field_flag(PROCESSES),      "concurrent processes");
		add_plot_column(fplot, column, field_flag(CPU_TIME),       "cpu time");      
		add_plot_column(fplot, column, field_flag(VIRTUAL),        "virtual memory");       
		add_plot_column(fplot, column, field_flag(RESIDENT),       "resident memory");
		add_plot_column(fplot, column, field_flag(B_READ),         "bytes read");        
		add_plot_column(fplot, column, field_flag(B_WRITTEN),      "bytes written");     
		add_plot_column(fplot, column, field_flag(WDIR_FILES),     "inodes");    
		add_plot_column(fplot, column, field_flag(WDIR_FOOTPRINT), "disk footprint");

		fprintf(fplot, "\n");
		fprintf(fplot, "unset multiplot\n");
		fprintf(fplot, "clusters_index = clusters_index + 1\n");
		fprintf(fplot, "if (clusters_index < %d) reread\n", max_clusters);

		fprintf(fplot, "\n");
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
				flags |= field_flag(WALL_TIME);
				debug(D_DEBUG, "adding clustering field: wall time: %d\n", field_flag(WALL_TIME));
				break;
			case 'p':
				flags |= field_flag(PROCESSES);
				debug(D_DEBUG, "adding clustering field: concurrent processes: %d\n", field_flag(PROCESSES));
				break;
			case 'c':
				flags |= field_flag(CPU_TIME);
				debug(D_DEBUG, "adding clustering field: cpu time: %d\n", field_flag(CPU_TIME));
				break;
			case 'v':
				flags |= field_flag(VIRTUAL);
				debug(D_DEBUG, "adding clustering field: virtual memory: %d\n", field_flag(VIRTUAL));
				break;
			case 'm':
				flags |= field_flag(RESIDENT);
				debug(D_DEBUG, "adding clustering field: resident memory: %d\n", field_flag(RESIDENT));
				break;
			case 'r':
				flags |= field_flag(B_READ);
				debug(D_DEBUG, "adding clustering field: bytes read: %d\n", field_flag(B_READ));
				break;
			case 'w':
				flags |= field_flag(B_WRITTEN);
				debug(D_DEBUG, "adding clustering field: bytes written: %d\n", field_flag(B_WRITTEN));
				break;
			case 'n':
				flags |= field_flag(WDIR_FILES);
				debug(D_DEBUG, "adding clustering field: number of files: %d\n", field_flag(WDIR_FILES));
				break;
			case 'z':
				flags |= field_flag(WDIR_FOOTPRINT);
				debug(D_DEBUG, "adding clustering field: footprint %d\n", field_flag(WDIR_FOOTPRINT));
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

	signed char c;
	while( (c = getopt(argc, argv, "d:f:m:o:")) > -1 )
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

	final = nearest_neighbor_clustering(initial_clusters, cluster_ward_distance);

	int i;
	for(i = 1; i <= max_clusters; i++)
	{
		final_clusters = collect_final_clusters(final, i);
		report_clusters_centroids(freport, final_clusters);
		list_delete(final_clusters);
	}

	report_clusters_histograms("gnuplot-plot-cmd", report_filename, max_clusters);

	denormalize_summaries(summaries);

	fclose(freport);

	return 0;
}


