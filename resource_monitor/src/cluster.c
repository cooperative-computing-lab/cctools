#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
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
#include "buffer.h"
#include "path.h"
#include "getopt_aux.h"

#include "rmsummary.h"

#include "rmon_tools.h"


#define DEFAULT_MAX_CLUSTERS 4

static char *report_filename = "clusters.txt";
struct rmDsummary *max_values;
int fields_flags;

struct cluster
{
	struct rmDsummary *centroid;
	struct rmDsummary *centroid_raw;             // before dividing by count

	double covariance[NUM_FIELDS][NUM_FIELDS];

	int             count;

	struct cluster *left;
	struct cluster *right;

	double          internal_conflict;        // distance between left and right

};

void print_summary_file(FILE *stream, struct rmDsummary *s, int include_abbrev)
{

	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(f->active)
		{
			fprintf(stream, "%s%6.3lf ", (include_abbrev) ? f->abbrev : "", value_of_field(s, f));
		}
	}

	fprintf(stream, "\n");
}

void print_covariance_matrix(FILE *stream, double covariance[NUM_FIELDS][NUM_FIELDS])
{
	enum fields row, col;

	fprintf(stream, "#   ");

	for(col = WALL_TIME; col < NUM_FIELDS; col++)
		if(fields[col].active)
			fprintf(stream, "%6s ", fields[col].abbrev);

	fprintf(stream, "\n");

	for(row = WALL_TIME; row < NUM_FIELDS; row++)
	{
		if(!fields[row].active)
			continue;

		fprintf(stream, "# %s ", fields[row].abbrev);

		for(col = WALL_TIME; col < NUM_FIELDS; col++)
		{
			if(!fields[col].active)
				continue;

			fprintf(stream, "%6.3lf ", covariance[row][col]);
		}

		fprintf(stream, "\n");
	}

	fprintf(stream, "# \n");
}

struct rmDsummary *find_max_summary(struct list *summaries)
{
	struct rmDsummary *s;
	struct rmDsummary *max = calloc(1, sizeof(struct rmDsummary));

	list_first_item(summaries);
	while( (s = list_next_item(summaries)) )
	{
		struct field *f;
		for(f = &fields[WALL_TIME]; f->name != NULL; f++)
		{
			if(f->active)
			{
				if(value_of_field(max, f) < value_of_field(s, f))
					assign_to_field(max, f, value_of_field(s, f));
			}
		}
	}

	return max;
}

void normalize_summary(struct rmDsummary *s)
{
	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(f->active)
		{
			if(value_of_field(max_values, f) > 0)
				assign_to_field(s, f, value_of_field(s, f) / value_of_field(max_values, f));
		}
	}
}

void normalize_summaries(struct list *summaries)
{
	struct rmDsummary *s;
	list_first_item(summaries);
	while( (s = list_next_item(summaries)) )
		normalize_summary(s);
}

void denormalize_summary(struct rmDsummary *s)
{
	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(f->active)
		{
			if(value_of_field(max_values, f) > 0)
				assign_to_field(s, f, value_of_field(s, f) * value_of_field(max_values, f));
		}
	}
}

void denormalize_summaries(struct list *summaries)
{
	struct rmDsummary *s;
	list_first_item(summaries);
	while( (s = list_next_item(summaries)) )
		denormalize_summary(s);
}

double summary_accumulate(struct rmDsummary *s)
{
	double acc = 0;

	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(f->active)
		{
			acc += value_of_field(s, f);
		}
	}

	return acc;
}

double summary_euclidean(struct rmDsummary *a, struct rmDsummary *b)
{
	struct rmDsummary *s = calloc(1, sizeof(struct rmDsummary));

	summary_bin_op(s, a, b, minus_squared);

	double accum = summary_accumulate(s);

	free(s);

	return sqrt(accum);
}

double cluster_ward_distance(struct cluster *a, struct cluster *b)
{
	struct rmDsummary *s = calloc(1, sizeof(struct rmDsummary));

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
	double         dmin, dtest;

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

struct rmDsummary *cluster_find_centroid(struct cluster *c)
{
	struct rmDsummary *s = calloc(1, sizeof(struct rmDsummary));
	struct rmDsummary *r = calloc(1, sizeof(struct rmDsummary));

	s->task_id = -1;
	r->task_id = -1;

	summary_bin_op(r, c->left->centroid_raw, c->right->centroid_raw, plus);
	summary_unit_op(s, r, (double) c->count, divide);

	c->centroid_raw = r;
	c->centroid     = s;

	return s;
}

struct cluster *cluster_create(struct rmDsummary *s)
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

void covariance_matrix_merge(struct cluster *c, struct cluster *left, struct cluster *right)
{
	enum fields row, col;
	struct rmDsummary *uA, *uB;

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
		if(!(fields[row].active))
			continue;

		uxA = value_of_field(uA, &fields[row]);
		uxB = value_of_field(uB, &fields[row]);

		for(col = row; col < NUM_FIELDS; col++)
		{
			if(!(fields[col].active))
				continue;

			uyA = value_of_field(uA, &fields[col]);
			uyB = value_of_field(uB, &fields[col]);

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
	const struct rmDsummary **sa = (void *) a;
	const struct rmDsummary **sb = (void *) b;

	return ((*sa)->task_id - (*sb)->task_id); 
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


void report_clusters_histograms(char *clusters_file, int max_clusters)
{
	FILE *fplot;
	int   column;

	char *plot_cmd_file = string_format("%s.gnuplot", clusters_file);
	fplot = fopen(plot_cmd_file, "w");
	free(plot_cmd_file);

	if(!fplot)
		fatal("cannot open file for plot command.\n");

	column = 2;

	fprintf(fplot, "div=1.1; bw = 0.9; h=1.0; BW=0.9; wd=10; LIMIT=255-wd; white = 0;\n");
	fprintf(fplot, "red = \"#080000\"; green = \"#000800\"; blue = \"#000008\";\n");
	fprintf(fplot, "set auto x;\n");
	fprintf(fplot, "set auto y;\n");
	fprintf(fplot, "set style data histogram;\n");
	fprintf(fplot, "set style histogram rowstacked;\n");
	fprintf(fplot, "set style fill solid;\n");
	fprintf(fplot, "set boxwidth bw;\n");
	fprintf(fplot, "set key invert box opaque;\n"); 
	fprintf(fplot, "set xtics nomirror; set ytics nomirror; set border front;\n");
	fprintf(fplot, "unset border; set noytics; set xlabel \"number of tasks\"; set ylabel \" resource proportion to max used\";\n");
	fprintf(fplot, "do for [clusters_index=0:%d] {\n", max_clusters - 1);

	fprintf(fplot, "foutput = sprintf(\"%s.%%03d.png\", 1 + clusters_index)\n", report_filename);
	fprintf(fplot, "set terminal push\n");
	fprintf(fplot, "set terminal png size 1024,768\n");
	fprintf(fplot, "set output foutput\n");
	fprintf(fplot, "set multiplot\n");
	fprintf(fplot, "plot '%s' ", clusters_file);

	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(f->active)
		{
			if(column == 2)
				fprintf(fplot, " index clusters_index using %d:xticlabels(1) title '%s'", 
						column,
						f->name);
			else
				fprintf(fplot, ", '' index clusters_index using %d title '%s'", 
						column,
						f->name);
			column++;
		}
	}

	fprintf(fplot, ";\n");
	fprintf(fplot, "unset multiplot;\n");
	fprintf(fplot, "}\n");
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
		struct rmDsummary *s;
		list_first_item(summaries);
		while( (s = list_next_item(summaries)) )
			fprintf(freport, "%" PRId64 " ", s->task_id);
		list_delete(summaries);
		fprintf(freport, "\n\n");

		/* Normalize again. Is the precision we are losing significant? */
		normalize_summary(c->centroid);
	}

	list_delete(clusters);
}

struct list *create_initial_clusters(struct list *summaries)
{
	struct rmDsummary *s;
	struct list *initial_clusters = list_create(0);

	list_first_item(summaries);
	while( (s = list_next_item(summaries)) )
		list_push_head(initial_clusters, cluster_create(s));

	return initial_clusters;
}

static void show_usage(const char *cmd)
{
	fprintf(stdout, "\nUse: %s [options]\n\n", cmd);
	fprintf(stdout, "\nIf no -D or -L are specified, read the summary file list from standard input.\n\n");
	fprintf(stdout, "%-20s Enable debugging for this subsystem.\n", "-d <subsystem>");
	fprintf(stdout, "%-20s Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal)\n", "-o <file>");
	fprintf(stdout, "%-20s Read summaries recursively from <dir> (filename of form '%s[0-9]+%s').\n", "-D <dir>", RULE_PREFIX, RULE_SUFFIX);
	fprintf(stdout, "%-20s Read summaries filenames from file <list>.\n", "-L <list>");
	fprintf(stdout, "%-20s Find at most <number> clusters.         (Default %d)\n", "-n <number>", DEFAULT_MAX_CLUSTERS);
	fprintf(stdout, "%-20s Write cluster information to this file. (Default %s)\n", "-O <file>", report_filename);
	fprintf(stdout, "%-20s Select these fields for clustering.     (Default is: tcvmsrwhz).\n\n", "-f <fields>");
	fprintf(stdout, "<fields> is a string in which each character should be one of the following:\n");
	fprintf(stdout, "%s", make_field_names_str("\n"));
	fprintf(stdout, "%-20s Show this message.\n", "-h,--help");
}

int main(int argc, char **argv)
{
	char           *input_directory = NULL;
	char           *input_list      = NULL;
	FILE           *freport;
	int             max_clusters = DEFAULT_MAX_CLUSTERS; 
	struct list    *initial_clusters, *final_clusters;
	struct cluster *final;

	debug_config(argv[0]);

	signed char c;
	while( (c = getopt(argc, argv, "D:d:f:hL:n:O:o:")) > -1 )
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
			case 'n':
				max_clusters = atoi(optarg);
				if(max_clusters < 1)
					fatal("The number of clusters cannot be less than one.\n");
				break;
			case 'O':
				report_filename = xxstrdup(optarg);
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

	if(!input_directory && !input_list)
	{
		input_list = "-";
	}

	if(!report_filename)
		report_filename = xxstrdup("clusters.txt");

	freport = fopen(report_filename, "w");
	if(!freport)
		fatal("%s: %s\n", report_filename, strerror(errno));


	struct rmDsummary_set *set = make_new_set("all");

	if(input_directory)
	{
		parse_summary_recursive(set, input_directory);
	}

	if(input_list)
	{
		parse_summary_from_filelist(set, input_list);
	}

	max_values = find_max_summary(set->summaries);

	normalize_summaries(set->summaries);

	initial_clusters = create_initial_clusters(set->summaries);

	final = nearest_neighbor_clustering(initial_clusters, cluster_ward_distance);

	int i;
	for(i = 1; i <= max_clusters; i++)
	{
		final_clusters = collect_final_clusters(final, i);
		report_clusters_centroids(freport, final_clusters);
		list_delete(final_clusters);
	}

	report_clusters_histograms(report_filename, max_clusters);

	denormalize_summaries(set->summaries);

	fclose(freport);

	return 0;
}



/* vim: set noexpandtab tabstop=4: */
