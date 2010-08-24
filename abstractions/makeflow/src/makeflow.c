/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <signal.h>
#include <fcntl.h>
#include <unistd.h>

#include "catalog_query.h"
#include "catalog_server.h"
#include "datagram.h"
#include "domain_name_cache.h"
#include "link.h"
#include "macros.h"
#include "hash_table.h"
#include "itable.h"
#include "debug.h"
#include "batch_job.h"
#include "work_queue.h"
#include "stringtools.h"
#include "load_average.h"
#include "get_line.h"
#include "int_sizes.h"
#include "list.h"

#define DAG_LINE_MAX 1048576
#define SHOW_INPUT_FILES 2
#define SHOW_OUTPUT_FILES 3
#define MASTER_CATALOG_LINE_MAX 1024
#define RANDOM_PORT_RETRY_TIME 300

#define MAKEFLOW_AUTO_WIDTH 1
#define MAKEFLOW_AUTO_GROUP 2

static int dag_abort_flag = 0;
static int dag_failed_flag = 0;
static int dag_submit_timeout = 3600;
static int dag_retry_flag = 0;
static int dag_retry_max = 100;

static batch_queue_type_t batch_queue_type = BATCH_QUEUE_TYPE_UNIX;
static struct batch_queue *local_queue = 0;
static struct batch_queue *remote_queue = 0;

static char *project = NULL;
static int priority = 0;
static int port = 0;

typedef enum {
	DAG_NODE_STATE_WAITING=0,
	DAG_NODE_STATE_RUNNING=1,
	DAG_NODE_STATE_COMPLETE=2,
	DAG_NODE_STATE_FAILED=3,
	DAG_NODE_STATE_ABORTED=4,
	DAG_NODE_STATE_MAX=5
} dag_node_state_t;

struct dag {
	const char *filename;
	struct dag_node   *nodes;
	struct itable     *node_table;
	struct itable     *local_job_table;
	struct itable     *remote_job_table;
	struct hash_table *file_table;
	struct hash_table *completed_files;
	struct hash_table *filename_translation_rev;
	struct hash_table *filename_translation_fwd;
	FILE * logfile;
	int node_states[DAG_NODE_STATE_MAX];
	int linenum;
	int local_jobs_running;
	int local_jobs_max;
	int remote_jobs_running;
	int remote_jobs_max;
	int nodeid_counter;
};

struct dag_file {
	const char *filename;
	struct dag_file *next;
};

struct dag_node {
	int only_my_children;
	time_t previous_completion;
	int linenum;
	int nodeid;
	int local_job;
	int failure_count;
	dag_node_state_t state;
	const char *command;
	struct dag_file *source_files;
	struct dag_file *target_files;
	int source_file_names_size;
	int target_file_names_size;
	batch_job_id_t jobid;
	struct dag_node *next;
	int children;
	int children_left;
	int level;
};

int dag_width( struct dag *d );

int dag_estimate_nodes_needed(struct dag *d, int actual_max)
{
	struct dag_node *n,*m,*tmp;
	struct dag_file *f;
	int nodeid;
	int depends_on_single_node = 1;
	int max=0;

	for(n=d->nodes;n;n=n->next) {
		nodeid = -1;
		m = 0;
		// for each source file, see if it is a target file of another node
		for(f=n->source_files;f;f=f->next) {
			// d->file_table contains all target files
			// get the node (tmp) that outputs current source file
			tmp = hash_table_lookup(d->file_table,f->filename);
			// if a source file is also a target file 
			if(tmp) {
				debug(D_DEBUG, "%d depends on %d\n", n->nodeid, tmp->nodeid);
				if(nodeid == -1) {
					m = tmp;  // m holds the parent node
					nodeid = m->nodeid;
					continue;
				} 
			
				// if current node depends on multiple nodes, continue to process next node
				if(nodeid != tmp->nodeid) {
					depends_on_single_node = 0;
					break;
				}
			}
		}

		// m != 0 : current node depends on at least one exsisting node
		if(m && depends_on_single_node && nodeid != -1) {
				m->only_my_children++;
				// return if maximum number of possible nodes is reached
				if(m->only_my_children == actual_max) return actual_max;
		}
	}
	
	// find out the maximum number of direct children that a single parent node has
	for(n=d->nodes;n;n=n->next) {
		max = max < n->only_my_children ? n->only_my_children : max;
	}

	return max;
}

void dag_show_input_files(struct dag *d)
{
	struct dag_node *n,*m,*tmp;
	struct dag_file *f;
	int nodeid;
	struct hash_table *ih;
	char *key;
	void *value;

	ih = hash_table_create(0,0);
	for(n=d->nodes;n;n=n->next) {
		nodeid = -1;
		m = 0;
		// for each source file, see if it is a target file of another node
		for(f=n->source_files;f;f=f->next) {
			// d->file_table contains all target files
			// get the node (tmp) that outputs current source file
			tmp = hash_table_lookup(d->file_table,f->filename);
			// if a source file is also a target file 
			if(!tmp) {
				debug(D_DEBUG, "Found independent input file: %s\n", f->filename);
				hash_table_insert(ih, f->filename, (void *)NULL);
			} 
		}
	}

	hash_table_firstkey(ih);
	while(hash_table_nextkey(ih,&key,&value)) {
		printf("%s\n",key);
	}

	hash_table_delete(ih);
}

void dag_show_output_files(struct dag *d)
{
	char *key;
	void *value;

	hash_table_firstkey(d->file_table);

	while(hash_table_nextkey(d->file_table, &key,&value)) {
		printf("%s\n", key);
	}
}

static int handle_auto_workers(struct dag *d, int auto_workers)
{
	char start_worker_line[1024];
	char hostname[DOMAIN_NAME_MAX];
	int num_of_workers;

	domain_name_cache_guess(hostname);

	if (auto_workers == MAKEFLOW_AUTO_GROUP) {
		num_of_workers = dag_estimate_nodes_needed(d, d->remote_jobs_max); 
	} else if (auto_workers == MAKEFLOW_AUTO_WIDTH) {
		num_of_workers = dag_width(d);
		if (num_of_workers > d->remote_jobs_max)
			num_of_workers = d->remote_jobs_max;
	}

	sprintf(start_worker_line, "condor_submit_workers %s %d %d", hostname, port, num_of_workers);
	printf("makeflow: starting %d workers: `%s`\n",num_of_workers,start_worker_line);
	if (system(start_worker_line)) {
		fprintf(stderr, "makeflow: unable to start workers.\n");
		return 0;
	}

	return 1;
}

/* Code added by kparting to compute the width of the graph.
   Original algorithm by pbui, with improvements by kparting */

int dag_width( struct dag *d )
{
	struct dag_node *n, *tmp;
	struct dag_file *f;
	
	/* 1. Find the number of immediate children for all nodes; also,
	determine leaves by adding nodes with children==0 to list. */

	for (n = d->nodes; n != NULL; n = n->next)
	{
		for (f = n->source_files; f != NULL; f = f->next)
		{
			tmp = (struct dag_node *)hash_table_lookup(d->file_table, f->filename);
			if (!tmp) continue;
			++tmp->children;
		}
	}

	struct list *list = list_create();

	for (n = d->nodes; n != NULL; n = n->next)
	{
		n->children_left = n->children;
		if (n->children == 0) list_push_tail(list, n);
	}

	/* 2. Assign every node a "reverse depth" level. Normally by depth,
	   I mean topologically sort and assign depth=0 to nodes with no
	   parents. However, I'm thinking I need to reverse this, with depth=0
	   corresponding to leaves. Also, we want to make sure that no node is
	   added to the queue without all its children "looking at it" first
	   (to determine its proper "depth level"). */

	int max_level = 0;

	while (list_size(list) > 0)
	{
		struct dag_node *n = (struct dag_node *)list_pop_head(list);
		
		for (f = n->source_files; f != NULL; f = f->next)
		{
			tmp = (struct dag_node *)hash_table_lookup(d->file_table, f->filename);
			if (!tmp) continue;

			if (tmp->level < n->level + 1)
				tmp->level = n->level + 1;

			if (tmp->level > max_level) max_level = tmp->level;
			
			--tmp->children_left;
			if (tmp->children_left == 0) list_push_tail(list, tmp);
		}
	}

	/* 3. Now that every node has a level, simply create an array and then
	   go through the list once more to count the number of nodes in each
	   level. */

	int *level_count = malloc((max_level + 1) * sizeof(*level_count));
	
	int i;
	for (i = 0; i <= max_level; ++i) /* yes, should be <=, no joke */
	{
		level_count[i] = 0;
	}

	for (n = d->nodes; n != NULL; n = n->next)
	{
		++level_count[n->level];
	}

	int max = 0;
	for (i = 0; i <= max_level; ++i) /* yes, should still be <=, srsly */
	{
		if (max < level_count[i]) max = level_count[i];
	}

	return max;

}

void dag_print( struct dag *d )
{
	char name[DAG_LINE_MAX];

	struct dag_node *n;
	struct dag_file *f;

	printf("digraph {\n");

	printf("node [shape=ellipse];\n");

	for(n=d->nodes;n;n=n->next) {
		strncpy(name, n->command, DAG_LINE_MAX);
		char * label = strtok(name," \t\n");
		printf("N%d [label=\"%s\"];\n",n->nodeid,label);
	}

	printf("node [shape=box];\n");

	for(n=d->nodes;n;n=n->next) {
		for(f=n->source_files;f;f=f->next) {
			printf("\"%s\" -> N%d;\n",f->filename,n->nodeid);
		}
		for(f=n->target_files;f;f=f->next) {
			printf("N%d -> \"%s\";\n",n->nodeid,f->filename);
		}
	}

	printf("}\n");
}

const char * dag_node_state_name( dag_node_state_t state )
{
	switch(state) {
		case DAG_NODE_STATE_WAITING:	return "waiting";
		case DAG_NODE_STATE_RUNNING:	return "running";
		case DAG_NODE_STATE_COMPLETE:	return "complete";
		case DAG_NODE_STATE_FAILED:	return "failed";
		case DAG_NODE_STATE_ABORTED:	return "aborted";
		default:			return "unknown";
	}
}

struct dag_file * dag_file_create( const char *filename, struct dag_file *next )
{
	struct dag_file *f = malloc(sizeof(*f));
	f->filename = strdup(filename);
	f->next = next;
	return f;
}

void dag_node_add_source_file( struct dag_node *n, const char *filename )
{
	n->source_files = dag_file_create(filename,n->source_files);
}

void dag_node_add_target_file( struct dag_node *n, const char *filename )
{
	n->target_files = dag_file_create(filename,n->target_files);
}

void dag_count_states( struct dag *d )
{
	struct dag_node *n;
	int i;

	for(i=0;i<DAG_NODE_STATE_MAX;i++) {
		d->node_states[i] = 0;
	}
    
	for(n=d->nodes;n;n=n->next) {
		d->node_states[n->state]++;
	}     
}

void dag_node_state_change( struct dag *d, struct dag_node *n, int newstate )
{
	debug(D_DEBUG,"node %d %s -> %s\n",n->nodeid,dag_node_state_name(n->state),dag_node_state_name(newstate));

	if (d->node_states[n->state] > 0) {
		d->node_states[n->state]--;
	}
	n->state = newstate;
	d->node_states[n->state]++;

	fprintf(d->logfile,"%u %d %d %d %d %d %d %d %d %d\n",(unsigned)time(0),n->nodeid,newstate,n->jobid,d->node_states[0],d->node_states[1],d->node_states[2],d->node_states[3],d->node_states[4],d->nodeid_counter);
	fflush(d->logfile);
	fsync(fileno(d->logfile));
}

void dag_abort_all( struct dag *d )
{
	UINT64_T jobid;
	struct dag_node *n;

	printf("makeflow: got abort signal...\n");

	itable_firstkey(d->local_job_table);
	while(itable_nextkey(d->local_job_table,&jobid,(void**)&n)) {
		printf("makeflow: aborting local job %llu\n",jobid);
		batch_job_remove(local_queue,jobid);
		dag_node_state_change(d,n,DAG_NODE_STATE_ABORTED);
	}

	itable_firstkey(d->remote_job_table);
	while(itable_nextkey(d->remote_job_table,&jobid,(void**)&n)) {
		printf("makeflow: aborting remote job %llu\n",jobid);
		batch_job_remove(remote_queue,jobid);
		dag_node_state_change(d,n,DAG_NODE_STATE_ABORTED);
	}
}

void file_clean( const char *filename, int silent )
{
	if (!filename) return;

	if(unlink(filename)==0) {
		if (!silent) printf("makeflow: deleted %s\n",filename);
	} else {
		if(errno==ENOENT) {
			// nothing
		} else {
			if (!silent) printf("makeflow: couldn't delete %s: %s\n",filename,strerror(errno));
		}
	}
}

void dag_node_clean( struct dag *d, struct dag_node *n )
{
	struct dag_file *f;
	for(f=n->target_files;f;f=f->next) {
		
		file_clean(f->filename, 0);

		/* Make sure to clobber the original file too if it exists */
		char *name = (char *) hash_table_lookup(d->filename_translation_rev, f->filename);

		if (name) file_clean(name, 0);
		
		hash_table_remove(d->completed_files,f->filename);
	}
}

void clean_symlinks( struct dag *d, int silent )
{
	char *key;
	void *value;
	
	if (batch_queue_type != BATCH_QUEUE_TYPE_CONDOR) return;

	hash_table_firstkey(d->filename_translation_rev);
	while (hash_table_nextkey(d->filename_translation_rev, &key, &value))
	{
		file_clean(key, silent);
	}
}

void dag_clean( struct dag *d )
{
	struct dag_node *n;
	for(n=d->nodes;n;n=n->next) dag_node_clean(d,n);

	/* Since we are in clean mode, remove symlinks verbosely */
	clean_symlinks(d, 0);
}

void dag_node_force_rerun(struct itable *rerun_table, struct dag *d, struct dag_node *n);

// Decide whether to rerun a node based on file system status
void dag_node_decide_rerun(struct itable *rerun_table, struct dag *d, struct dag_node *n) {
	struct stat filestat;
	struct dag_file *f;
	
	if(itable_lookup(rerun_table, n->nodeid)) return;

	// Below are a bunch of situations when a node has to be rerun.

	// Input file has been updated since last execution
	for(f=n->source_files;f;f=f->next) {
		if(stat(f->filename, &filestat)>=0) {
			if(difftime(filestat.st_mtime, n->previous_completion) > 0) {
				goto rerun; // rerun this node
			}
		} else {
			if(!hash_table_lookup(d->file_table, f->filename)) {
				fprintf(stderr, "makeflow: input file %s does not exist and is not created by any rule.\n",f->filename);
				exit(1);
			} else {
				goto rerun;
			}
		}
	}

	// Output file has been updated/deleted since last execution
	for(f=n->target_files;f;f=f->next) {
		if(stat(f->filename, &filestat)>=0) {
			if(difftime(filestat.st_mtime, n->previous_completion) > 0) {
				goto rerun;
			}
		} else {
			goto rerun;
		}
	}

	// Do not rerun this node
	return; 

rerun:
	dag_node_force_rerun(rerun_table, d, n);
}

void dag_node_force_rerun(struct itable *rerun_table, struct dag *d, struct dag_node *n) {
	struct dag_node *p;
	struct dag_file *f1;
	struct dag_file *f2;
	int child_node_found;

	// Remove running batch jobs
	if(n->state==DAG_NODE_STATE_RUNNING) {
		if(n->local_job) {
			batch_job_remove(local_queue, n->jobid);
			if(itable_remove(d->local_job_table, n->jobid)) {
				d->local_jobs_running--;
			}
		} else {
			batch_job_remove(remote_queue, n->jobid);
			if(itable_remove(d->remote_job_table, n->jobid)) {
				d->remote_jobs_running--;
			}
		}
	}

	// Clean up things associated with this node
	dag_node_clean(d,n);
	dag_node_state_change(d,n,DAG_NODE_STATE_WAITING);

	// For each child node, rerun it
	for(f1=n->target_files;f1;f1=f1->next) {
		for(p=d->nodes;p;p=p->next) {
			child_node_found = 0;
			for(f2=p->source_files;f2;f2=f2->next) {
				if(!strcmp(f1->filename, f2->filename)) {
					child_node_found = 1;
					break;
				}
			}
			if(child_node_found) {
				dag_node_force_rerun(rerun_table, d, p);
			}
		}
	}

	// Mark this node as having been rerun already
	itable_insert(rerun_table,n->nodeid,n);
}


void dag_log_recover( struct dag *d, const char *filename )
{
	int linenum = 0;
	char *line;
	int nodeid, state, jobid;
	int first_run = 1;
	struct dag_node *n;
	unsigned int previous_completion_time;

	d->logfile = fopen(filename,"r");
	if(d->logfile) {
		first_run = 0;

		while ((line = get_line(d->logfile))) {
			linenum++;

			if(line[0] == '#') continue;
			if(sscanf(line,"%u %d %d %d", &previous_completion_time, &nodeid,&state,&jobid)==4) {
				n = itable_lookup(d->node_table,nodeid);
				if(n) {
					n->state = state;
					n->jobid = jobid;
					n->previous_completion = (time_t)previous_completion_time;
					continue;
				}
			}

			fprintf(stderr,"makeflow: %s appears to be corrupted on line %d\n",filename,linenum);	
			clean_symlinks(d, 1);
			exit(1);
		}
		fclose(d->logfile);
	}

	d->logfile = fopen(filename,"a");
	if(!d->logfile) {
		fprintf(stderr,"makeflow: couldn't open logfile %s: %s\n",filename,strerror(errno));
		clean_symlinks(d, 1);
		exit(1);
	}

	if(first_run) {
		struct dag_file *f;
		struct dag_node *p;
		for(n=d->nodes;n;n=n->next) {
			fprintf(d->logfile, "# %d\t%s",n->nodeid,n->command);
			for(f=n->source_files;f;f=f->next) {
				p = hash_table_lookup(d->file_table,f->filename);
				if (p)
					fprintf(d->logfile, "\t%d",p->nodeid);
			}
			fputc('\n', d->logfile);
		}
	}

	dag_count_states(d);

	for(n=d->nodes;n;n=n->next) {
		if(n->state==DAG_NODE_STATE_RUNNING && !n->local_job && batch_queue_type==BATCH_QUEUE_TYPE_CONDOR) {
			printf("makeflow: rule still running: %s\n",n->command);
			itable_insert(d->remote_job_table,n->jobid,n);
			d->remote_jobs_running++;
		} else if(n->state==DAG_NODE_STATE_RUNNING || n->state==DAG_NODE_STATE_FAILED || n->state==DAG_NODE_STATE_ABORTED) {
			printf("makeflow: will retry failed rule: %s\n",n->command);
			dag_node_clean(d,n);
			dag_node_state_change(d,n,DAG_NODE_STATE_WAITING);
		}
	}



	// Check file system status to decide rerun tasks 
	if(!first_run) {
		struct itable *rerun_table = itable_create(0);
		for(n=d->nodes;n;n=n->next) {
			dag_node_decide_rerun(rerun_table, d, n);
		}
	}
}

static char * lookupenv( const char *name, void *arg )
{
	const char *env = getenv(name);
	
	if (env) return strdup(env);

	return NULL;
}

char * dag_readline( struct dag *d, FILE *file )
{
	char *rawline = get_line(file);

	if (rawline) {
		d->linenum++;
		if (d->linenum % 1000 == 0)
		{
			debug(D_DEBUG, "read line %d\n", d->linenum);
			if (d->linenum % 100000 == 0)
				printf("makeflow: reading line %d\n", d->linenum);
		}

		string_chomp(rawline);

		char *hash = strchr(rawline,'#');
		if(hash) *hash = 0;

		char *substline = strdup(rawline);
		substline = string_subst(substline,lookupenv,0);

		char * cookedline = strdup(substline);

		string_replace_backslash_codes(substline,cookedline);
		free(substline);

		return cookedline;
	}

	return 0;
}

static int translate_filename( struct dag *d, const char *filename, char **newname_ptr )
{
	/* The purpose of this function is to translate an absolute path
	   filename into a unique slash-less name to allow for the sending
	   of any file to remote systems. Function returns 1 on success, 0 if
	   filename has already been translated. */

	if (!newname_ptr) return 0;
	if (!strncmp(filename, "./", 2))
	{
		/* Assume this is a current working directory path */
		*newname_ptr = NULL;
		return 0;
	}

	/* First check for if the filename has already been translated-- if so,
	   use that translation */

	char *newname;
	newname = (char *)hash_table_lookup(d->filename_translation_fwd, filename);

	if (newname) /* Filename has been translated before */
	{
		char *temp = newname;
		newname = strdup(temp);
		*newname_ptr = newname;
		return 0;
	}

	newname = strdup(filename);
	char *c;

	for (c = newname; *c; ++c)
	{
		if (*c == '/') *c = '_';
	}
	
	for (c = newname; *c == '.'; ++c)
	{
		*c = '_';
	}

	while (!hash_table_insert(d->filename_translation_rev, newname, strdup(filename)))
	{
		/* It's not 100% collision-proof, technically, but the odds of
		   an unresolvable collision are unbelievably slim. */

		c = strchr(newname, '_');
		if (c)
		{
			*c = '~';
		}
		else
		{
			c = strchr(newname, '~');
			if (c)
			{
				*c = '-';
			}
			else
			{
				*newname_ptr = NULL;
				return 0;
			}
		}
	}

	hash_table_insert(d->filename_translation_fwd, filename, strdup(newname));

	*newname_ptr = newname;
	return 1;
}

static char *translate_command( struct dag *d, char *old_command, int is_local )
{
	char *new_command;
	char *sp;
	char *token;
	int first = 1;
	int wait = 0; /* Wait for next token before prepending "./"? */
	int padding = 3;
	char prefix;
	
	UPTRINT_T current_length = (UPTRINT_T)0;

	for (sp = old_command; *sp; sp++)
	    if (isspace((int)*sp)) padding += 2;

	new_command = malloc((strlen(old_command)+padding) * sizeof(char));
	new_command[0] = '\0';
	
	token = strtok(old_command, " \t\n");

	while (token)
	{
		/* Remove (and store) the shell metacharacter prefix, if
		   there is one. */
		switch (token[0])
		{
			case '<':
			case '>':
				prefix = token[0];
				++token;
				break;
			default:
				prefix = '\0';
		}
		
		if (prefix && !token)
		{
			/* Indicates "< input" or "> output", i.e., with
			   space after the shell metacharacter */
			wait = 1;
		}

		char *val = NULL;
		int len;
		
		if (!is_local)
			val = (char *)hash_table_lookup(d->filename_translation_fwd, token);

		if (!first)
		{
			strncat(new_command + current_length, " ", 1);
			++current_length;
		}
		else
		{
			first = 0;
		}

		/* Append the shell metacharacter prefix, if there is one. */
		if (prefix)
		{
			strncat(new_command + current_length, &prefix, 1);
			++current_length;
		}

		if (val)
		{
			/* If the executable has a hashtable entry, then we
			   need to prepend "./" to the symlink name */
			if (wait)
			{
				wait = 0;
			}
			else
			{
				strncat(new_command + current_length, "./", 2);
				current_length += 2;
			}
			
			len = strlen(val);
			strncat(new_command + current_length, val, len);
			current_length += len;
		}
		else
		{
			len = strlen(token);
			strncat(new_command + current_length, token, len);
			current_length += len;
		}

		token = strtok(NULL, " \t\n");
	}

	return new_command;
}

void dag_parse_assignment( struct dag *d, char *line )
{
	char *name = line;
	char *eq = strchr(line,'=');
	char *value=eq+1;

	// advance value to the first non-whitespace
	while(*value && isspace((int)*value)) value++;

	// set = and any preceding whitespace to null
	do {
		*eq=0;
		if (eq > line) eq--;
	} while(eq>line && isspace((int)*eq));

	if(eq==name) {
		fprintf(stderr,"makeflow: error at %s:%d: variable assignment has no name!\n",d->filename,d->linenum);
		exit(1);
	}

	setenv(name,value,1);
}

void dag_node_parse_filelist( struct dag *d, struct dag_node *n, char *filelist, int source, int clean_mode )
{
	char *filename;
	char *newname;
	int rv;

	filename = strtok(filelist," \t\n");
	while(filename) {
		if(batch_queue_type == BATCH_QUEUE_TYPE_CONDOR && strchr(filename, '/')) {
			newname = NULL;
			rv = translate_filename(d, filename, &newname);
			if (rv && !clean_mode) {
				printf("makeflow: creating symlink \"./%s\" for file \"%s\"\n", newname, filename);
				rv = symlink(filename, newname);
				if (rv < 0 && errno != EEXIST) {
					//TODO: Check for if symlink points to right place
					fprintf(stderr,"makeflow: could not create symbolic link (%s)\n", strerror(errno));
					exit(1);
				}

				if (access(filename, R_OK) < 0) {
					int fd;

					fd = open(filename, O_WRONLY|O_CREAT|O_TRUNC, 0700);
					if (fd < 0) {
						fprintf(stderr, "makeflow: could not create symbolic link target (%s)\n", strerror(errno));
						exit(1);
					}
					close(fd);
				}
			}

			if (!newname) newname = filename;

			if (source) {
				dag_node_add_source_file(n, newname);
				n->source_file_names_size += strlen(filename)+1;
			} else {
				dag_node_add_target_file(n, newname);
				n->target_file_names_size += strlen(filename)+1;
			}

			if (newname != filename) free(newname);
		} else if(filename[0] == '/' && batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
			// Translate only explicit absolute paths for work queue tasks.
			newname = NULL;
			rv = translate_filename(d, filename, &newname);

			if (source) {
				dag_node_add_source_file(n, filename);
				n->source_file_names_size += strlen(filename)+strlen(newname)+2;
			} else {
				dag_node_add_target_file(n, filename);
				n->target_file_names_size += strlen(filename)+strlen(newname)+2;
			}

			if (newname != filename) free(newname);
		} else {
			if (source) {
				dag_node_add_source_file(n,filename);
				n->source_file_names_size += strlen(filename)+1;
			} else {
				dag_node_add_target_file(n,filename);
				n->target_file_names_size += strlen(filename)+1;
			}
		}

		filename = strtok(0," \t\n");
	}
}

struct dag_node * dag_node_parse( struct dag *d, FILE *file, int clean_mode )
{
	char *line;
	char *eq;
	char *colon;
	char *targetfiles;
	char *sourcefiles;

	while(1) {
		line = dag_readline(d,file);
		if(!line) return 0;

		if(string_isspace(line)) {
			free(line);
			continue;
		}

		eq = strchr(line,'=');
		colon = strchr(line,':');

		if(eq) {
			if(!colon || colon>eq) {
				dag_parse_assignment(d,line);
				continue;
			}
		}

		break;
	}

	if(!colon) {
		fprintf(stderr,"makeflow: error at %s:%d: %s\n",d->filename,d->linenum,line);
		exit(1);
	}

	struct dag_node *n = malloc(sizeof(*n));
	memset(n,0,sizeof(*n));
	n->only_my_children = 0;
	n->children = 0;
	n->children_left = 0;
	n->level = 0;
	n->linenum = d->linenum;
	n->state = DAG_NODE_STATE_WAITING;
	n->nodeid = d->nodeid_counter++;
	n->local_job = 0;
	n->failure_count = 0;

	*colon = 0;
	targetfiles = line;
	sourcefiles = colon+1;

	dag_node_parse_filelist(d, n, targetfiles, 0, clean_mode);
	dag_node_parse_filelist(d, n, sourcefiles, 1, clean_mode);

	free(line);

	line = dag_readline(d,file);
	if(!line) {
		fprintf(stderr,"makeflow: error at %s:%d: expected a command\n",d->filename,d->linenum);
		exit(1);
	}

	char *c=line;
	while(*c && isspace((int)*c)) c++;
	if(!strncmp(c,"LOCAL ",6)) {
		n->local_job = 1;
		c+=6;
	}

	n->command = translate_command(d, c, n->local_job);

	free(line);
	return n;
}

struct dag * dag_create( const char *filename, int clean_mode )
{
	FILE *file = fopen(filename,"r");
	if(!file) return 0;

	struct dag *d = malloc(sizeof(*d));
	memset(d,0,sizeof(*d));
	d->nodes = 0;
	d->linenum = 0;
	d->filename = strdup(filename);
	d->node_table = itable_create(0);
	d->local_job_table = itable_create(0);
	d->remote_job_table = itable_create(0);
	d->file_table = hash_table_create(0,0);
	d->completed_files = hash_table_create(0,0);
	d->local_jobs_running = 0;
	d->local_jobs_max = 1;
	d->remote_jobs_running = 0;
	d->remote_jobs_max = 100;
	d->nodeid_counter = 0;
	d->filename_translation_rev = hash_table_create(0,0);
	d->filename_translation_fwd = hash_table_create(0,0);

	memset(d->node_states, 0, sizeof(int) * DAG_NODE_STATE_MAX);

	struct dag_node *n,*m;
	struct dag_file *f;

	while((n=dag_node_parse(d,file,clean_mode))) {
		n->next = d->nodes;
		d->nodes = n;
		itable_insert(d->node_table,n->nodeid,n);
	}

	if (!clean_mode)
	{
		debug(D_DEBUG, "checking for duplicate targets...\n");
	}
	
	for(n=d->nodes;n;n=n->next) {
		for(f=n->target_files;f;f=f->next) {
			m = hash_table_lookup(d->file_table,f->filename);
			if(m) {
				fprintf(stderr,"makeflow: %s is defined multiple times at %s:%d and %s:%d\n",f->filename,d->filename,n->linenum,d->filename,m->linenum);
				exit(1);
			} else {
				hash_table_insert(d->file_table,f->filename,n);
			}
		}
	}

	return d;
}

void dag_node_complete( struct dag *d, struct dag_node *n, struct batch_job_info *info );

void dag_node_submit( struct dag *d, struct dag_node *n )
{
	char *input_files = NULL;
	char *output_files = NULL;
	char *filename = NULL;
	struct dag_file *f;

	struct batch_queue *thequeue;

	if(n->local_job) {
		thequeue = local_queue;
	} else {
		thequeue = remote_queue;
	}

	printf("makeflow: %s\n",n->command);

	input_files = malloc((n->source_file_names_size + 1) * sizeof(char));
	input_files[0] = '\0';
	for(f=n->source_files;f;f=f->next) {
		strcat(input_files,f->filename);
		if (batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
			filename = hash_table_lookup(d->filename_translation_fwd, f->filename);
			if (filename) {
				strcat(input_files,"=");
				strcat(input_files,filename);
			}
		}
		strcat(input_files,",");
	}

	output_files = malloc((n->target_file_names_size + 1) * sizeof(char));
	output_files[0] = '\0';
	for(f=n->target_files;f;f=f->next) {
		if (batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
			filename = hash_table_lookup(d->filename_translation_fwd, f->filename);
			if (filename) {
				strcat(output_files,filename);
				strcat(output_files,"=");
			}
		}
		strcat(output_files,f->filename);
		strcat(output_files,",");
	}

	batch_queue_set_options(thequeue,getenv("BATCH_OPTIONS"));

	time_t stoptime = time(0) + dag_submit_timeout;
	int waittime = 1;

	while(1) {
		n->jobid = batch_job_submit_simple(thequeue,n->command,input_files,output_files);
		if(n->jobid>=0) break;

		fprintf(stderr,"makeflow: couldn't submit batch job, still trying...\n");

		if(time(0)>stoptime) {
			fprintf(stderr,"makeflow: unable to submit job after %d seconds!\n",dag_submit_timeout);
			break;
		}

		sleep(waittime);
		waittime *= 2;
		if(waittime>60) waittime=60;
	}

	if(n->jobid>=0) {
		dag_node_state_change(d,n,DAG_NODE_STATE_RUNNING);
		if(n->local_job) {
			itable_insert(d->local_job_table,n->jobid,n);
			d->local_jobs_running++;
		} else {
			itable_insert(d->remote_job_table,n->jobid,n);
			d->remote_jobs_running++;
		}
	} else {
		dag_node_state_change(d,n,DAG_NODE_STATE_FAILED);
		dag_failed_flag = 1;
	}
}

int dag_node_ready( struct dag *d, struct dag_node *n )
{
	struct dag_file *f;

	if(n->state!=DAG_NODE_STATE_WAITING) return 0;

	if(n->local_job) {
		if(d->local_jobs_running>=d->local_jobs_max) return 0;
	} else {
		if(d->remote_jobs_running>=d->remote_jobs_max) return 0;
	}

	for(f=n->source_files;f;f=f->next) {
		if(hash_table_lookup(d->completed_files,f->filename)) {
			continue;
		} else {
			return 0;
		}
	}

	return 1;
}

void dag_dispatch_ready_jobs( struct dag *d )
{
	struct dag_node *n;

	for(n=d->nodes;n;n=n->next) {

		if( d->remote_jobs_running >= d->remote_jobs_max && d->local_jobs_running  >= d->local_jobs_max) break;

		if(dag_node_ready(d,n)) {
			dag_node_submit(d,n);
		}
	}
}

void dag_node_complete( struct dag *d, struct dag_node *n, struct batch_job_info *info )
{
	struct dag_file *f;
	int job_failed = 0;

	if(n->state!=DAG_NODE_STATE_RUNNING) return;

	if(n->local_job) {
		d->local_jobs_running--;
	} else {
		d->remote_jobs_running--;
	}

	if(info->exited_normally && info->exit_code==0) {
		for(f=n->target_files;f;f=f->next) {
			if(access(f->filename,R_OK)!=0) {
				fprintf(stderr,"makeflow: %s did not create file %s\n",n->command,f->filename);
				job_failed=1;
			}
		}
	} else {
		if(info->exited_normally) {
			fprintf(stderr,"makeflow: %s failed with exit code %d\n",n->command,info->exit_code);
		} else {
			fprintf(stderr,"makeflow: %s crashed with signal %d (%s)\n",n->command,info->exit_signal,strsignal(info->exit_signal));
		}
		job_failed = 1;
	}

	if(job_failed) {
		dag_node_state_change(d,n,DAG_NODE_STATE_FAILED);
		if(dag_retry_flag || info->exit_code==101) {
			n->failure_count++;
			if(n->failure_count>dag_retry_max) {
				fprintf(stderr,"makeflow: job %s failed too many times.\n",n->command);
				dag_failed_flag = 1;
			} else {
				fprintf(stderr,"makeflow: will retry failed job %s\n",n->command);
				dag_node_state_change(d,n,DAG_NODE_STATE_WAITING);
			}
		} else {
			dag_failed_flag = 1;
		}
	} else {

		for(f=n->target_files;f;f=f->next) {
			hash_table_insert(d->completed_files,f->filename,f->filename);
		}

		dag_node_state_change(d,n,DAG_NODE_STATE_COMPLETE);
	}
}

int dag_check( struct dag *d )
{
	struct dag_node *n;
	struct dag_file *f;

	printf("makeflow: checking rules for consistency...\n");

	for(n=d->nodes;n;n=n->next) {
		for(f=n->source_files;f;f=f->next) {
			if(hash_table_lookup(d->completed_files,f->filename)) {
				continue;
			}

			if(access(f->filename,R_OK)==0) {
				hash_table_insert(d->completed_files,f->filename,f->filename);
				continue;
			}

			if(hash_table_lookup(d->file_table,f->filename)) {
				continue;
			}

			fprintf(stderr,"makeflow: error: %s does not exist, and is not created by any rule.\n",f->filename);
			clean_symlinks(d, 1);
			return 0;
		}
	}

	return 1;			
}

void dag_run( struct dag *d )
{
	struct dag_node *n;
	batch_job_id_t jobid;
	struct batch_job_info info;

	while(!dag_abort_flag) {

		dag_dispatch_ready_jobs(d);

		if(d->local_jobs_running==0 && d->remote_jobs_running==0) break;

		if(d->remote_jobs_running) {
			jobid = batch_job_wait_timeout(remote_queue,&info,time(0)+5);
			if(jobid>0) {
				n = itable_remove(d->remote_job_table,jobid);
				if(n) dag_node_complete(d,n,&info);
			}
		}

		if(d->local_jobs_running) {
			time_t stoptime;

			if(d->remote_jobs_running) {
				stoptime = time(0);
			} else {
				stoptime = time(0)+5;
			}

			jobid = batch_job_wait_timeout(local_queue,&info,stoptime);
			if(jobid>0) {
				n = itable_remove(d->local_job_table,jobid);
				if(n) dag_node_complete(d,n,&info);
			}
		}
	}

	if(dag_abort_flag) {
		dag_abort_all(d);
	}
}

static void handle_abort( int sig )
{
	dag_abort_flag = 1;
}

int parse_catalog_server_description(char* server_string) {
	char *host;
	int port;
	char *colon;
	char line[1024];

	colon = strchr(server_string, ':');

	if(!colon) return 0;

	*colon = '\0';

	host = strdup(server_string);
	port = atoi(colon+1);

	if(!port) return 0;
		
	sprintf(line,"CATALOG_HOST=%s",host);
	putenv(strdup(line));
	sprintf(line,"CATALOG_PORT=%d",port);
	putenv(strdup(line));
	
	return 1;
}
	
static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	printf("Use: %s [options] <dagfile>\n", cmd);
	printf("where options are:\n");
	printf(" -c             Clean up: remove logfile and all targets.\n");
	printf(" -T <type>      Batch system type: %s. (default is unix)\n",batch_queue_type_string());
	printf(" -j <#>         Max number of local jobs to run at once.    (default is # of cores)\n");
	printf(" -J <#>         Max number of remote jobs to run at once.   (default is 100)\n");
	printf(" -p <port>      Port number to use with work queue.         (default is %d, -1=random)\n",WORK_QUEUE_DEFAULT_PORT);
	printf(" -C             Syntax check.\n");
	printf(" -N <project>   Report the master information to a catalog server with the project name - <project>\n");
	printf(" -E <integer>   Priority. Higher the value, higher the priority.\n");
	printf(" -I             Show input files.\n");
	printf(" -O             Show output files.\n");
	printf(" -D             Display the Makefile as a Dot graph.\n");
	printf(" -B <options>   Add these options to all batch submit files.\n");
	printf(" -S <timeout>   Time to retry failed batch job submission.  (default is %ds)\n",dag_submit_timeout);
	printf(" -r <n>         Automatically retry failed batch jobs up to n times.\n");
	printf(" -l <logfile>   Use this file for the makeflow log.         (default is X.makeflowlog)\n");
	printf(" -L <logfile>   Use this file for the batch system log.     (default is X.condorlog)\n");
	printf(" -A             Disable the check for AFS.                  (experts only.)\n");;
	printf(" -F <#>         Work Queue fast abort multiplier.           (default is deactivated)\n");
	printf(" -W <mode>      Work Queue scheduling algorithm.            (time|files|fcfs)\n");
	printf(" -a <mode>      Auto Work Queue mode. Mode is either 'width' or 'group' (DAG [width] or largest [group] of tasks).\n");
	printf(" -d <subsystem> Enable debugging for this subsystem\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -P             Preserve (i.e., do not clean) intermediate symbolic links\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
	
}

int main( int argc, char *argv[] )
{
	char c;
	char *logfilename = NULL;
	char *batchlogfilename = NULL;
	int clean_mode = 0;
	int catalog_mode = 0;
	int display_mode = 0;
	int syntax_check = 0;
	int explicit_remote_jobs_max = 0;
	int explicit_local_jobs_max = 0;
	int skip_afs_check = 0;
	int preserve_symlinks = 0;
	const char *batch_submit_options = 0;
	int auto_workers = 0;
	char line[1024];

	debug_config(argv[0]);

	while((c = getopt(argc, argv, "a:Ap:cCd:E:DCT:iIB:s:S:Rr:l:L:j:J:N:Oo:vhF:W:P")) != (char) -1) {
		switch (c) {
		case 'A':
			skip_afs_check = 1;
			break;
		case 'p':
			port = atoi(optarg);
			break;
		case 'c':
			clean_mode = 1;
			break;
		case 'N':
			if (project) free(project);
			project = strdup(optarg);
			sprintf(line,"WORK_QUEUE_NAME=%s",project);
			putenv(strdup(line));
			catalog_mode = 1;
			break;
		case 'E':
			priority = atoi(optarg);
			sprintf(line,"WORK_QUEUE_PRIORITY=%d",priority);
			putenv(strdup(line));
			break;
		case 'I':
			display_mode = SHOW_INPUT_FILES;
			break;
		case 'O':
			display_mode = SHOW_OUTPUT_FILES;
			break;
		case 'l':
			logfilename = strdup(optarg);
			break;
		case 'L':
			batchlogfilename = malloc((strlen(optarg)+1) * sizeof(char));
			batchlogfilename = strdup(optarg);
			break;
		case 'D':
			display_mode = 1;
			break;
		case 'C':
			syntax_check = 1;
			break;
		case 's':
			if(!parse_catalog_server_description(optarg)) {
				fprintf(stderr,"makeflow: catalog server should be given as HOSTNAME:PORT'.\n");
				exit(1);
			}
			break;
		case 'S':
			dag_submit_timeout = atoi(optarg);
			break;
		case 'R':
			dag_retry_flag = 1;
			break;
		case 'r':
			dag_retry_flag = 1;
			dag_retry_max = atoi(optarg);
			break;
		case 'j':
			explicit_local_jobs_max = atoi(optarg);
			break;
		case 'J':
			explicit_remote_jobs_max = atoi(optarg);
			break;
		case 'B':
			batch_submit_options = optarg;
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'v':
			show_version(argv[0]);
			return 0;
		case 'h':
			show_help(argv[0]);
			return 0;
		case 'T':
			batch_queue_type = batch_queue_type_from_string(optarg);
			if(batch_queue_type==BATCH_QUEUE_TYPE_UNKNOWN) {
				fprintf(stderr,"makeflow: unknown batch queue type: %s\n",optarg);
				return 1;
			}
			break;
		case 'a':
			if (!strcmp(optarg, "width"))
			{
				auto_workers = MAKEFLOW_AUTO_WIDTH;
			}
			else if (!strcmp(optarg, "group"))
			{
				auto_workers = MAKEFLOW_AUTO_GROUP;
			}
			else
			{
				show_help(argv[0]);
			}
			break;
		case 'F':
			wq_option_fast_abort_multiplier = atof(optarg);
			break;
		case 'W':
			if(!strcmp(optarg,"files")) {
				wq_option_scheduler = WORK_QUEUE_SCHEDULE_FILES;
			} else if(!strcmp(optarg,"time")) {
				wq_option_scheduler = WORK_QUEUE_SCHEDULE_TIME;
			} else if(!strcmp(optarg,"fcfs")) {
				wq_option_scheduler = WORK_QUEUE_SCHEDULE_FCFS;
			} else {
				fprintf(stderr,"makeflow: unknown scheduling mode %s\n",optarg);
				return 1;
			}
			break;
		case 'P':
			preserve_symlinks = 1;
			break;
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	const char *dagfile;
	
	if((argc-optind)!=1) {
		int rv = access("./Makeflow",R_OK);
		if (rv < 0)
		{
			fprintf(stderr, "makeflow: No makeflow specified and file \"./Makeflow\" could not be found.\n");
			fprintf(stderr, "makeflow: Run \"%s -h\" for help with options.\n", argv[0]);
			return 1;
		}
		
		dagfile = "./Makeflow";
	}
	else
	{
		dagfile = argv[optind];
	}

	if(port!=0) {
		char line[1024];
		sprintf(line,"WORK_QUEUE_PORT=%d",port);
		putenv(strdup(line));
	}

	int dagfile_namesize = strlen(dagfile);

	if(!logfilename)
	{
		logfilename = malloc((dagfile_namesize+13)*sizeof(char));
		sprintf(logfilename,"%s.makeflowlog",dagfile);
	}
	if(!batchlogfilename)
	{
		batchlogfilename = malloc((dagfile_namesize+11)*sizeof(char));
		sprintf(batchlogfilename,"%s.condorlog",dagfile);
	}

	int no_symlinks = (clean_mode || syntax_check || display_mode);
	struct dag *d = dag_create(dagfile, no_symlinks);
	if(!d) {
		fprintf(stderr,"makeflow: couldn't load %s: %s\n",dagfile,strerror(errno));
		free(logfilename);
		free(batchlogfilename);
		return 1;
	}

	if (syntax_check)
	{
		printf("makeflow: %s: Syntax OK.\n", dagfile);
		return 0;
	}

	if(display_mode == SHOW_INPUT_FILES) {
		dag_show_input_files(d);
		exit(0);
	}
	if(display_mode == SHOW_OUTPUT_FILES) {
		dag_show_output_files(d);
		exit(0);
	}

	if(explicit_local_jobs_max) {
		d->local_jobs_max = explicit_local_jobs_max;
	} else {
		d->local_jobs_max = load_average_get_cpus();
	}

	if(explicit_remote_jobs_max) {
		d->remote_jobs_max = explicit_remote_jobs_max;
	} else {
		if(batch_queue_type==BATCH_QUEUE_TYPE_UNIX) {
			d->remote_jobs_max = load_average_get_cpus();
		} else if(batch_queue_type==BATCH_QUEUE_TYPE_WORK_QUEUE) {
			d->remote_jobs_max = 1000;
		} else {
			d->remote_jobs_max = 100;
		}
	}

	char *s = getenv("MAKEFLOW_MAX_REMOTE_JOBS");
	if(s) {
		d->remote_jobs_max = MIN(d->remote_jobs_max,atoi(s));
	}

	s = getenv("MAKEFLOW_MAX_LOCAL_JOBS");
	if(s) {
		int n = atoi(s);
		d->local_jobs_max = MIN(d->local_jobs_max,n);
		if(batch_queue_type==BATCH_QUEUE_TYPE_UNIX) {
			d->remote_jobs_max = MIN(d->local_jobs_max,n);
		}
	}

	if(display_mode) {
		free(logfilename);
		free(batchlogfilename);
		dag_print(d);
		return 0;
	}

	if(clean_mode) {
		dag_clean(d);
		file_clean(logfilename, 0);
		file_clean(batchlogfilename, 0);
		free(logfilename);
		free(batchlogfilename);
		return 0;
	}

	if(!dag_check(d))
	{
		free(logfilename);
		free(batchlogfilename);
		return 1;
	}
	
	if(batch_queue_type==BATCH_QUEUE_TYPE_CONDOR && !skip_afs_check) {
		char cwd[DAG_LINE_MAX];
		if(getcwd(cwd,sizeof(cwd))>=0) {
			if(!strncmp(cwd,"/afs",4)) {
				fprintf(stderr,"makeflow: This won't work because Condor is not able to write to files in AFS.\n");
				fprintf(stderr,"makeflow: Instead, run makeflow from a local disk like /tmp.\n");
				fprintf(stderr,"makeflow: Or, use the work queue with -T wq and condor_submit_workers.\n");
				
				free(logfilename);
				free(batchlogfilename);

				exit(1);
			}
		}
	}

	setlinebuf(stdout);
	setlinebuf(stderr);

	local_queue = batch_queue_create(BATCH_QUEUE_TYPE_UNIX);
	if(!local_queue) {
		fprintf(stderr,"makeflow: couldn't create local job queue.\n");
		exit(1);
	}

	remote_queue = batch_queue_create(batch_queue_type);
	if(!remote_queue) {
		fprintf(stderr,"makeflow: couldn't create batch queue.\n");
		if(port!=0) fprintf(stderr,"makeflow: perhaps port %d is already in use?\n",port);
		exit(1);
	}

	if(batch_submit_options) {
		batch_queue_set_options(remote_queue,batch_submit_options);
	}

	if(batchlogfilename) {
		batch_queue_set_logfile(remote_queue,batchlogfilename);
	}

	port = batch_queue_port(remote_queue);
	if(port>0) printf("makeflow: listening on port %d.\n",port);

	if(auto_workers>0) {
		if(!handle_auto_workers(d, auto_workers)) {
			exit(1);
		}
	}

	dag_log_recover(d,logfilename);

	signal(SIGINT,handle_abort);
	signal(SIGQUIT,handle_abort);
	signal(SIGTERM,handle_abort);

	dag_run(d);

	batch_queue_delete(local_queue);
	batch_queue_delete(remote_queue);

	if (!preserve_symlinks && batch_queue_type == BATCH_QUEUE_TYPE_CONDOR) {
		clean_symlinks(d,0);
	}

	if (logfilename) free(logfilename);
	if (batchlogfilename) free(batchlogfilename);

	if(dag_abort_flag) {
		fprintf(stderr,"makeflow: workflow was aborted.\n");
		return 1;
	} else if(dag_failed_flag) {
		fprintf(stderr,"makeflow: workflow failed.\n");
		return 1;
	} else {
		printf("makeflow: nothing left to do.\n");
		return 0;
	}
}


