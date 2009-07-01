
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <sys/stat.h>
#include <signal.h>

#include "macros.h"
#include "hash_table.h"
#include "itable.h"
#include "debug.h"
#include "batch_job.h"
#include "stringtools.h"

static int dag_abort_flag = 0;

#define DAG_LINE_MAX 1024

typedef enum {
	DAG_NODE_STATE_WAITING=0,
	DAG_NODE_STATE_RUNNING=1,
	DAG_NODE_STATE_COMPLETE=2,
	DAG_NODE_STATE_FAILED=3,
	DAG_NODE_STATE_ABORTED=4,
} dag_node_state_t;

struct dag {
	const char *filename;
	struct dag_node   *nodes;
	struct itable     *node_table;
	struct itable     *job_table;
	struct hash_table *file_table;
	FILE * logfile;
	int linenum;
	int jobs_running;
	int jobs_max;
};

struct dag_file {
	const char *filename;
	struct dag_file *next;
};

struct dag_node {
	int linenum;
	int nodeid;
	dag_node_state_t state;
	const char *command;
	struct dag_file *source_files;
	struct dag_file *target_files;
	batch_job_id_t jobid;
	struct dag_node *next;
};

void dag_print( struct dag *d )
{
	char name[DAG_LINE_MAX];

	struct dag_node *n;
	struct dag_file *f;

	printf("digraph {\n");

	printf("node [shape=ellipse,style=filled,fillcolor=green];\n");

	for(n=d->nodes;n;n=n->next) {
		strcpy(name,n->command);
		char * label = strtok(name," \t\n");
		printf("N%d [label=\"%s\"];\n",n->nodeid,label);
	}

	printf("node [shape=box,style=filled,fillcolor=skyblue];\n");

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

void dag_node_state_change( struct dag *d, struct dag_node *n, int newstate )
{
	debug(D_DEBUG,"node %d %s -> %s\n",n->nodeid,dag_node_state_name(n->state),dag_node_state_name(newstate));
	fprintf(d->logfile,"%u %d %d %d\n",(unsigned)time(0),n->nodeid,newstate,n->jobid);
	fflush(d->logfile);
	fsync(fileno(d->logfile));
	n->state = newstate;
}

void dag_abort_all( struct dag *d, struct batch_queue *q )
{
	int jobid;
	struct dag_node *n;

	printf("makeflow: got abort signal...\n");

	itable_firstkey(d->job_table);
	while(itable_nextkey(d->job_table,&jobid,(void**)&n)) {
		printf("makeflow: aborting job %d\n",jobid);
		batch_job_remove(q,jobid);
	}
}

void remove_target( const char *filename )
{
	if(unlink(filename)==0) {
		printf("makeflow: deleted %s\n",filename);
	} else {
		if(errno==ENOENT) {
			// nothing
		} else {
			printf("makeflow: couldn't delete %s: %s\n",filename,strerror(errno));
		}
	}
}

void dag_clean( struct dag *d )
{
	struct dag_node *n;
	struct dag_file *f;

	for(n=d->nodes;n;n=n->next) {
		for(f=n->target_files;f;f=f->next) {
			remove_target(f->filename);
		}
	}
}

void dag_log_recover( struct dag *d, const char *filename )
{
	int linenum = 0;
	char line[DAG_LINE_MAX];
	int nodeid, state, jobid;
	struct dag_node *n;

	d->logfile = fopen(filename,"r");
	if(d->logfile) {
		while(fgets(line,sizeof(line),d->logfile)) {
			linenum++;
			if(sscanf(line,"%*u %d %d %d",&nodeid,&state,&jobid)==3) {
				n = itable_lookup(d->node_table,nodeid);
				if(n) {
					n->state = state;
					n->jobid = jobid;
					if(state==DAG_NODE_STATE_RUNNING && jobid>0) itable_insert(d->job_table,jobid,n);
					continue;
				}
			}

			fprintf(stderr,"makeflow: %s appears to be corrupted on line %d\n",filename,linenum);	
			exit(1);
		}
		fclose(d->logfile);
	}
}

void dag_log_init( struct dag *d, const char *logfilename )
{

	d->logfile = fopen(logfilename,"a");
	if(!d->logfile) {
		fprintf(stderr,"makeflow: couldn't open logfile %s: %s\n",logfilename,strerror(errno));
		exit(1);
	}
}

static char * lookupenv( const char *name, void *arg )
{
	return strdup(getenv(name));
}

char * dag_readline( struct dag *d, FILE *file )
{
	char rawline[DAG_LINE_MAX];

	if(fgets(rawline,sizeof(rawline),file)) {
		d->linenum++;

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

struct dag_node * dag_node_parse( struct dag *d, FILE *file )
{
	static int nodeid_counter = 0;

	char *line;
	char *eq;
	char *colon;
	char *targetfiles;
	char *sourcefiles;
	char *filename;

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
				putenv(line);
				continue;
			}
		}

		break;
	}

	if(!colon) {
		printf("makeflow: error at %s:%d: %s\n",d->filename,d->linenum,line);
		exit(1);
	}

	struct dag_node *n = malloc(sizeof(*n));
	memset(n,0,sizeof(*n));
	n->linenum = d->linenum;
	n->state = DAG_NODE_STATE_WAITING;
	n->nodeid = nodeid_counter++;

	*colon = 0;
	targetfiles = line;
	sourcefiles = colon+1;

	filename = strtok(targetfiles," \t\n");
	while(filename) {
		dag_node_add_target_file(n,filename);
		filename = strtok(0," \t\n");
	}

	filename = strtok(sourcefiles," \t\n");
	while(filename) {
		dag_node_add_source_file(n,filename);
		filename = strtok(0," \t\n");
	}

	free(line);

	line = dag_readline(d,file);
	if(!line) {
		printf("makeflow: error at %s:%d: expected a command\n",d->filename,d->linenum);
		exit(1);
	}

	char *c=line;
	while(*c && isspace(*c)) c++;
	n->command = strdup(c);
	free(line);
	return n;
}

struct dag * dag_create( const char *filename )
{
	FILE *file = fopen(filename,"r");
	if(!file) return 0;

	struct dag *d = malloc(sizeof(*d));
	d->nodes = 0;
	d->linenum = 0;
	d->filename = strdup(filename);
	d->node_table = itable_create(0);
	d->job_table = itable_create(0);
	d->file_table = hash_table_create(0,0);
	d->jobs_running = 0;
	d->jobs_max = 1000;

	struct dag_node *n,*m;
	struct dag_file *f;

	while((n=dag_node_parse(d,file))) {
		n->next = d->nodes;
		d->nodes = n;
		itable_insert(d->node_table,n->nodeid,n);
	}
	
	for(n=d->nodes;n;n=n->next) {
		for(f=n->target_files;f;f=f->next) {
			m = hash_table_lookup(d->file_table,f->filename);
			if(m) {
				fatal("%s is defined multiple times at %s:%d and %s:%d\n",f->filename,d->filename,n->linenum,d->filename,m->linenum);
			} else {
				hash_table_insert(d->file_table,f->filename,n);
			}
		}
	}
	
	return d;
}

void dag_node_complete( struct batch_queue *q, struct dag *d, struct dag_node *n, struct batch_job_info *info );

void dag_node_submit( struct batch_queue *q, struct dag *d, struct dag_node *n )
{
	char input_files[DAG_LINE_MAX];
	char output_files[DAG_LINE_MAX];
	struct dag_file *f;

	printf("makeflow: %s\n",n->command);

	if(!strncmp(n->command,"LOCAL ",6)) {
		struct batch_job_info info;
		memset(&info,0,sizeof(info));
		dag_node_state_change(d,n,DAG_NODE_STATE_RUNNING);
		int result = system(&n->command[6]);
		info.exited_normally = 1;
		info.exit_code = result;
		d->jobs_running++;
		dag_node_complete(q,d,n,&info);
		return;
	}

	input_files[0] = 0;
	output_files[0] = 0;

	for(f=n->source_files;f;f=f->next) {
		strcat(input_files,f->filename);
		strcat(input_files,",");
	}

	for(f=n->target_files;f;f=f->next) {
		strcat(output_files,f->filename);
		strcat(output_files,",");
	}

	batch_queue_set_options(q,getenv("BATCH_OPTIONS"));

	n->jobid = batch_job_submit_simple(q,n->command,input_files,output_files);
	if(n->jobid>=0) {
		dag_node_state_change(d,n,DAG_NODE_STATE_RUNNING);
		itable_insert(d->job_table,n->jobid,n);
		d->jobs_running++;
	} else {
		dag_node_state_change(d,n,DAG_NODE_STATE_FAILED);
	}
}

int dag_node_ready( struct dag *d, struct dag_node *n )
{
	struct stat info;
	struct dag_file *f;
	time_t age = 0;

	if(n->state!=DAG_NODE_STATE_WAITING) return 0;

	for(f=n->source_files;f;f=f->next) {
		if(stat(f->filename,&info)==0) {
			age = MAX(age,info.st_mtime);
		} else {
			return 0;
		}
	}

	for(f=n->target_files;f;f=f->next) {
		if(stat(f->filename,&info)==0) {
			if(info.st_mtime<age) return 1;
		} else {
			return 1;
		}
	}

	return 0;
}

void dag_node_complete( struct batch_queue *q, struct dag *d, struct dag_node *n, struct batch_job_info *info )
{
	struct dag_node *m;
	struct dag_file *f;
	int job_failed = 0;

	if(n->state!=DAG_NODE_STATE_RUNNING) return;

	d->jobs_running--;

	if(info->exited_normally && info->exit_code==0) {
		for(f=n->target_files;f;f=f->next) {
			if(access(f->filename,R_OK)!=0) {
				printf("makeflow: %s did not create file %s\n",n->command,f->filename);
				job_failed=1;
			}
		}
	} else {
		if(info->exited_normally) {
			printf("makeflow: %s failed with exit code %d\n",n->command,info->exit_code);
		} else {
			printf("makeflow: %s crashed with signal %d (%s)\n",n->command,info->exit_signal,strsignal(info->exit_signal));
		}
		job_failed = 1;
	}

	if(job_failed) {
		dag_node_state_change(d,n,DAG_NODE_STATE_FAILED);
		return;
	} else {
		dag_node_state_change(d,n,DAG_NODE_STATE_COMPLETE);
	}

	for(m=d->nodes;m;m=m->next) {

		if(d->jobs_running>=d->jobs_max) break;

		if(dag_node_ready(d,m)) {
			dag_node_submit(q,d,m);
		}
	}
}

void dag_run( struct dag *d, struct batch_queue *q )
{
	struct dag_node *n;
	batch_job_id_t jobid;
	struct batch_job_info info;

	for(n=d->nodes;n;n=n->next) {
		if(dag_node_ready(d,n)) {
			dag_node_submit(q,d,n);
		}
	}

	while(!dag_abort_flag && d->jobs_running>0 && (jobid=batch_job_wait_timeout(q,&info,time(0)+5))!=0) {
		if(jobid<0) continue;
		n = itable_remove(d->job_table,jobid);
		if(n) dag_node_complete(q,d,n,&info);
	}

	if(dag_abort_flag) {
		dag_abort_all(d,q);
	}
	printf("makeflow: nothing left to do.\n");
}

static void handle_abort( int sig )
{
	dag_abort_flag = 1;
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
	printf(" -T <type>      Batch system type: condor, sge, unix, wq.  (default is unix)\n");
	printf(" -D             Display the Makefile as a Dot graph.\n");
	printf(" -B <options>   Add these options to all batch submit files.\n");
	printf(" -p <port>      Port number to use with work queue.        (default=9123)\n");
	printf(" -l <logfile>   Use this file for the makeflow log.        (default=<dagfile>.makeflowlog)\n");
	printf(" -L <logfile>   Use this file for the batch system log.    (default=<dagfile>.condorlog)\n");
	printf(" -A             Disable the check for AFS. (experts only.)\n");;
	printf(" -d <subsystem> Enable debugging for this subsystem\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

int main( int argc, char *argv[] )
{
	int port = 9123;
	char c;
	batch_queue_type_t batchtype = BATCH_QUEUE_TYPE_UNIX;
	char logfilename[DAG_LINE_MAX];
	char batchlogfilename[DAG_LINE_MAX];
	int clean_mode = 0;
	int display_mode = 0;
	int explicit_jobs_max = 0;
	int skip_afs_check = 0;
	const char *batch_submit_options = 0;

	logfilename[0] = 0;
	batchlogfilename[0] = 0;

	debug_config(argv[0]);

	while((c = getopt(argc, argv, "Ap:cd:DT:iB:l:L:j:o:v")) != (char) -1) {
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
		case 'l':
			strcpy(logfilename,optarg);
			break;
		case 'L':
			strcpy(batchlogfilename,optarg);
			break;
		case 'D':
			display_mode = 1;
			break;
		case 'j':
			explicit_jobs_max = atoi(optarg);
			break;
		case 'B':
			batch_submit_options = optarg;
			break;
		case 'd':
			debug_flags_set(optarg);			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'v':
			show_version(argv[0]);
			return 0;
		case 'T':
			batchtype = batch_queue_type_from_string(optarg);
			if(batchtype==BATCH_QUEUE_TYPE_UNKNOWN) {
				fprintf(stderr,"dag: unknown batch queue type: %s\n",optarg);
				return 1;
			}
			break;
		case 'h':
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	if((argc-optind)!=1) {
		show_help(argv[0]);
		return 1;
	}

	const char *dagfile = argv[optind];

	if(!logfilename[0]) sprintf(logfilename,"%s.makeflowlog",dagfile);
	if(!batchlogfilename[0]) sprintf(batchlogfilename,"%s.condorlog",dagfile);

	struct dag *d = dag_create(dagfile);
	if(!d) {
		fprintf(stderr,"makeflow: couldn't load %s: %s\n",dagfile,strerror(errno));
		return 1;
	}

	if(explicit_jobs_max) {
		d->jobs_max = explicit_jobs_max;
	} else {
		if(batchtype==BATCH_QUEUE_TYPE_UNIX) {
			d->jobs_max = 2;
		} else {
			d->jobs_max = 1000;
		}
	}

	if(display_mode) {
		dag_print(d);
		return 0;
	}

	if(clean_mode) {
		dag_clean(d);
		remove_target(logfilename);
		remove_target(batchlogfilename);
		return 0;
	}

	if(batchtype==BATCH_QUEUE_TYPE_CONDOR && !skip_afs_check) {
		char cwd[DAG_LINE_MAX];
		if(getcwd(cwd,sizeof(cwd))>=0) {
			if(!strncmp(cwd,"/afs",4)) {
				fprintf(stderr,"makeflow: This won't work because Condor is not able to write to files in AFS.\n");
				fprintf(stderr,"makeflow: Instead, run makeflow from a local disk like /tmp.\n");
				fprintf(stderr,"makeflow: Or, use the work queue with -T wq and condor_submit_workers.\n");
				exit(1);
			}
		}
	}

	setlinebuf(stdout);
	setlinebuf(stderr);

	struct batch_queue *q = batch_queue_create(batchtype);
	if(batch_submit_options)   batch_queue_set_options(q,batch_submit_options);
	if(batchlogfilename) batch_queue_set_logfile(q,batchlogfilename);
	if(batchtype==BATCH_QUEUE_TYPE_CONDOR) dag_log_recover(d,logfilename);
	dag_log_init(d,logfilename);

	signal(SIGINT,handle_abort);
	signal(SIGQUIT,handle_abort);
	signal(SIGTERM,handle_abort);

	dag_run(d,q);

	return 0;
}


