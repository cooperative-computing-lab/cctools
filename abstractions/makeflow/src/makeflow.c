
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
#include "work_queue.h"
#include "stringtools.h"
#include "load_average.h"

static int dag_abort_flag = 0;
static int dag_failed_flag = 0;
static int dag_submit_timeout = 3600;
static int dag_retry_flag = 0;

static batch_queue_type_t batch_queue_type = BATCH_QUEUE_TYPE_UNIX;
static struct batch_queue *local_queue = 0;
static struct batch_queue *remote_queue = 0;

// A line length this long is not ideal, but it will have to do
// until we can parse lines of arbitrary length.
#define DAG_LINE_MAX 1048576

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
	FILE * logfile;
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
	int linenum;
	int nodeid;
	int local_job;
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

	printf("node [shape=ellipse];\n");

	for(n=d->nodes;n;n=n->next) {
		strcpy(name,n->command);
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

void dag_count_states( struct dag *d, int states[DAG_NODE_STATE_MAX] )
{
	struct dag_node *n;
	int i;

	for(i=0;i<DAG_NODE_STATE_MAX;i++) {
		states[i] = 0;
	}

	for(n=d->nodes;n;n=n->next) {
		states[n->state]++;
	}	
}

void dag_node_state_change( struct dag *d, struct dag_node *n, int newstate )
{
	int states[DAG_NODE_STATE_MAX];

	debug(D_DEBUG,"node %d %s -> %s\n",n->nodeid,dag_node_state_name(n->state),dag_node_state_name(newstate));

	n->state = newstate;
	dag_count_states(d,states);

	fprintf(d->logfile,"%u %d %d %d %d %d %d %d %d %d\n",(unsigned)time(0),n->nodeid,newstate,n->jobid,states[0],states[1],states[2],states[3],states[4],d->nodeid_counter);
	fflush(d->logfile);
	fsync(fileno(d->logfile));
}

void dag_abort_all( struct dag *d )
{
	int jobid;
	struct dag_node *n;

	printf("makeflow: got abort signal...\n");

	itable_firstkey(d->local_job_table);
	while(itable_nextkey(d->local_job_table,&jobid,(void**)&n)) {
		printf("makeflow: aborting local job %d\n",jobid);
		batch_job_remove(local_queue,jobid);
	}

	itable_firstkey(d->remote_job_table);
	while(itable_nextkey(d->remote_job_table,&jobid,(void**)&n)) {
		printf("makeflow: aborting remote job %d\n",jobid);
		batch_job_remove(remote_queue,jobid);
	}
}

void file_clean( const char *filename )
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

void dag_node_clean( struct dag *d, struct dag_node *n )
{
	struct dag_file *f;
	for(f=n->target_files;f;f=f->next) {
		file_clean(f->filename);
		hash_table_remove(d->completed_files,f->filename);
	}
}

void dag_clean( struct dag *d )
{
	struct dag_node *n;
	for(n=d->nodes;n;n=n->next) dag_node_clean(d,n);
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
					continue;
				}
			}

			fprintf(stderr,"makeflow: %s appears to be corrupted on line %d\n",filename,linenum);	
			exit(1);
		}
		fclose(d->logfile);
	}

	d->logfile = fopen(filename,"a");
	if(!d->logfile) {
		fprintf(stderr,"makeflow: couldn't open logfile %s: %s\n",filename,strerror(errno));
		exit(1);
	}

	for(n=d->nodes;n;n=n->next) {
		if(n->state==DAG_NODE_STATE_RUNNING && !n->local_job && batch_queue_type==BATCH_QUEUE_TYPE_CONDOR) {
			printf("makeflow: rule still running: %s\n",n->command);
			itable_insert(d->remote_job_table,n->jobid,n);
			d->remote_jobs_running++;
		} else if(n->state==DAG_NODE_STATE_RUNNING || n->state==DAG_NODE_STATE_FAILED || n->state==DAG_NODE_STATE_FAILED) {
			printf("makeflow: will retry failed rule: %s\n",n->command);
			dag_node_clean(d,n);
			dag_node_state_change(d,n,DAG_NODE_STATE_WAITING);
		}
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

static void filename_error( struct dag *d, const char *filename )
{
	fprintf(stderr,"makeflow: Error at %s:%d: %s contains a slash.\n",d->filename,d->linenum,filename);
	fprintf(stderr,"makeflow: Rules can only refer to files in the current directory.\n");
	exit(1);
}

void dag_parse_assignment( struct dag *d, char *line )
{
	char *name = line;
	char *eq = strchr(line,'=');
	char *value=eq+1;

	// advance value to the first non-whitespace
	while(*value && isspace(*value)) value++;

	// set = and any preceding whitespace to null
	do {
		*eq=0;
		eq--;
	} while(eq>line && isspace(*eq));

	if(eq==name) {
		fprintf(stderr,"makeflow: error at %s:%d: variable assignment has no name!\n",d->filename,d->linenum);
		exit(1);
	}

	setenv(name,value,1);
}

struct dag_node * dag_node_parse( struct dag *d, FILE *file )
{
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
	n->linenum = d->linenum;
	n->state = DAG_NODE_STATE_WAITING;
	n->nodeid = d->nodeid_counter++;
	n->local_job = 0;

	*colon = 0;
	targetfiles = line;
	sourcefiles = colon+1;

	filename = strtok(targetfiles," \t\n");
	while(filename) {
		if(strchr(filename,'/')) filename_error(d,filename);
		dag_node_add_target_file(n,filename);
		filename = strtok(0," \t\n");
	}

	filename = strtok(sourcefiles," \t\n");
	while(filename) {
		if(strchr(filename,'/')) filename_error(d,filename);
		dag_node_add_source_file(n,filename);
		filename = strtok(0," \t\n");
	}

	free(line);

	line = dag_readline(d,file);
	if(!line) {
		fprintf(stderr,"makeflow: error at %s:%d: expected a command\n",d->filename,d->linenum);
		exit(1);
	}

	char *c=line;
	while(*c && isspace(*c)) c++;
	if(!strncmp(c,"LOCAL ",6)) {
		n->local_job = 1;
		c+=6;
	}		
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
	d->local_job_table = itable_create(0);
	d->remote_job_table = itable_create(0);
	d->file_table = hash_table_create(0,0);
	d->completed_files = hash_table_create(0,0);
	d->local_jobs_running = 0;
	d->local_jobs_max = 1;
	d->remote_jobs_running = 0;
	d->remote_jobs_max = 100;
	d->nodeid_counter = 0;

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
	char input_files[DAG_LINE_MAX];
	char output_files[DAG_LINE_MAX];
	struct dag_file *f;

	struct batch_queue *thequeue;

	if(n->local_job) {
		thequeue = local_queue;
	} else {
		thequeue = remote_queue;
	}

	printf("makeflow: %s\n",n->command);

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
			fprintf(stderr,"makeflow: will retry failed job %s\n",n->command);
			dag_node_state_change(d,n,DAG_NODE_STATE_WAITING);
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

static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	printf("Use: %s [options] <dagfile>\n", cmd);
	printf("where options are:\n");
	printf(" -c             Clean up: remove logfile and all targets.\n");
	printf(" -T <type>      Batch system type: condor, sge, unix, wq.   (default is unix)\n");
	printf(" -j <#>         Max number of local jobs to run at once.    (default is # of cores)\n");
	printf(" -J <#>         Max number of remote jobs to run at once.   (default is 100)\n");
	printf(" -p <port>      Port number to use with work queue.         (default is %d)\n",WORK_QUEUE_DEFAULT_PORT);
	printf(" -D             Display the Makefile as a Dot graph.\n");
	printf(" -B <options>   Add these options to all batch submit files.\n");
	printf(" -S <timeout>   Time to retry failed batch job submission.  (default is %ds)\n",dag_submit_timeout);
	printf(" -R             Automatically retry failed batch jobs.\n");
	printf(" -l <logfile>   Use this file for the makeflow log.         (default is X.makeflowlog)\n");
	printf(" -L <logfile>   Use this file for the batch system log.     (default is X.condorlog)\n");
	printf(" -A             Disable the check for AFS. (experts only.)\n");;
	printf(" -d <subsystem> Enable debugging for this subsystem\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

int main( int argc, char *argv[] )
{
	int port = 0;
	char c;
	char logfilename[DAG_LINE_MAX];
	char batchlogfilename[DAG_LINE_MAX];
	int clean_mode = 0;
	int display_mode = 0;
	int explicit_remote_jobs_max = 0;
	int explicit_local_jobs_max = 0;
	int skip_afs_check = 0;
	const char *batch_submit_options = 0;

	logfilename[0] = 0;
	batchlogfilename[0] = 0;

	debug_config(argv[0]);

	while((c = getopt(argc, argv, "Ap:cd:DT:iB:S:Rl:L:j:J:o:v")) != (char) -1) {
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
		case 'S':
			dag_submit_timeout = atoi(optarg);
			break;
		case 'R':
			dag_retry_flag = 1;
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
		case 'T':
			batch_queue_type = batch_queue_type_from_string(optarg);
			if(batch_queue_type==BATCH_QUEUE_TYPE_UNKNOWN) {
				fprintf(stderr,"makeflow: unknown batch queue type: %s\n",optarg);
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

	if(port!=0) {
		char line[1024];
		sprintf(line,"WORK_QUEUE_PORT=%d",port);
		putenv(strdup(line));
	}

	const char *dagfile = argv[optind];

	if(!logfilename[0]) sprintf(logfilename,"%s.makeflowlog",dagfile);
	if(!batchlogfilename[0]) sprintf(batchlogfilename,"%s.condorlog",dagfile);

	struct dag *d = dag_create(dagfile);
	if(!d) {
		fprintf(stderr,"makeflow: couldn't load %s: %s\n",dagfile,strerror(errno));
		return 1;
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
		} else {
			d->remote_jobs_max = 100;
		}
	}

	if(display_mode) {
		dag_print(d);
		return 0;
	}

	if(clean_mode) {
		dag_clean(d);
		file_clean(logfilename);
		file_clean(batchlogfilename);
		return 0;
	}

	if(!dag_check(d)) return 1;

	if(batch_queue_type==BATCH_QUEUE_TYPE_CONDOR && !skip_afs_check) {
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

	local_queue = batch_queue_create(BATCH_QUEUE_TYPE_UNIX);
	remote_queue = batch_queue_create(batch_queue_type);

	if(batch_submit_options) {
		batch_queue_set_options(remote_queue,batch_submit_options);
	}

	if(batchlogfilename) {
		batch_queue_set_logfile(remote_queue,batchlogfilename);
	}

	dag_log_recover(d,logfilename);

	signal(SIGINT,handle_abort);
	signal(SIGQUIT,handle_abort);
	signal(SIGTERM,handle_abort);

	dag_run(d);

	batch_queue_delete(local_queue);
	batch_queue_delete(remote_queue);

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


