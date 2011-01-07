
/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <ctype.h>
#include <sys/stat.h>

#include "debug.h"
#include "work_queue.h"
#include "hash_table.h"
#include "stringtools.h"
#include "macros.h"
#include "envtools.h"

#include "sequence.h"
#include "compressed_sequence.h"

static struct work_queue *queue = 0;
static struct hash_table *sequence_table = 0;
static int port = WORK_QUEUE_DEFAULT_PORT;
static char align_prog[1024];
static const char *align_prog_args = "";
static const char *candidate_file_name;
static const char *sequence_file_name;
static const char *output_file_name;

static FILE *sequence_file;
static FILE *candidate_file;
static FILE *output_file;

static time_t start_time = 0;
static time_t last_display_time = 0;

static int more_candidates = 1;
static int tasks_submitted = 0;
static int tasks_done = 0;
static timestamp_t tasks_runtime = 0;
static timestamp_t tasks_filetime = 0;
static int candidates_loaded = 0;
static int sequences_loaded = 0;

static int max_pairs_per_task = 10000;

#define CANDIDATE_SUCCESS 0
#define CANDIDATE_EOF 1
#define CANDIDATE_WAIT 2

#define CAND_FILE_LINE_MAX 4096

static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	printf("Use: %s [options] <sand_align_kernel> <candidates.cand> <sequences.cfa> <overlaps.ovl>\n", cmd);
	printf("where options are:\n");
	printf(" -p <port>      Port number for work queue master to listen on. (default: %d)\n",port);
	printf(" -n <number>    Maximum number of candidates per task. (default is %d)\n",max_pairs_per_task);
	printf(" -e <args>      Extra arguments to pass to the alignment program.\n");
	printf(" -d <subsystem> Enable debugging for this subsystem.  (Try -d all to start.)\n");
	printf(" -F <#>         Work Queue fast abort multiplier.     (default is 10.)\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

static void display_progress(struct work_queue *q)
{
	struct work_queue_stats info;
	static int row_count = 0;
	int row_limit = 25;

	work_queue_get_stats(q, &info);

	if(row_count==0) {
		printf(" Total | Workers   | Tasks                      Avg | K-Cand K-Seqs | Total\n");
		printf("  Time | Idle Busy | Submit Idle  Run   Done   Time | Loaded Loaded | Speedup\n");
		row_count = row_limit;
	}

	printf("%6d | %4d %4d | %6d %4d %4d %6d %6.2lf | %6d %6d | %5.2lf\n",
		(int) (time(0) - start_time),
		info.workers_init + info.workers_ready,
		info.workers_busy,
		tasks_submitted,
		info.tasks_waiting,
		info.tasks_running,
		tasks_done,
		tasks_done ? tasks_runtime / (double) tasks_done / 1000000.0 : 0,
		candidates_loaded / 1000,
		sequences_loaded / 1000,
		(time(0)>start_time) ? (tasks_runtime/1000000.0) / (time(0)-start_time) : 0);

	row_count--;

	last_display_time = time(0);
	fflush(stdout);
}

/*
Check to see that the output consists of an envelope [ ... ]
around some OVL records.  If there are no good matches in the
output, we should see an envelope with nothing in it.
If the file is completely empty, then there is a problem and
we reject the output.
*/

static char * confirm_output( char *output )
{
	char *s = output;
	char *result = 0;

	while(isspace(*s)) s++;

	if(*s!='[') {
		debug(D_NOTICE,"aligment output did not begin with [:\n%s\n",output);
		return 0;
	}

	s++;

	while(isspace(*s)) s++;

	result = s;

	while(*s) s++;

	s--;

	while(isspace(*s)) s--;

	if(*s!=']') {
		debug(D_NOTICE,"aligment output did not end with ]:\n%s\n",output);
		return 0;
	}

	*s = 0;

	return result;
}

static void task_complete( struct work_queue_task *t )
{
	if(t->return_status!=0) {
		debug(D_NOTICE,"task failed with status %d on host %s\n",t->return_status,t->host);
		work_queue_submit(queue,t);
		return;
	}

	char *clean_output = confirm_output(t->output);
	if(!clean_output) {
		work_queue_submit(queue,t);
		return;
	}

	fprintf(output_file,"%s",clean_output);
	fflush(output_file);

	tasks_done++;
	tasks_runtime += (t->finish_time - t->start_time);
	tasks_filetime += t->total_transfer_time;

	work_queue_task_delete(t);
}

static int candidate_read(FILE * fp, char *name1, char *name2, char *extra_data)
{
	char line[CAND_FILE_LINE_MAX];

	clearerr(fp);

	long start_of_line = ftell(fp);

	if(!fgets(line, CAND_FILE_LINE_MAX, fp)) return CANDIDATE_WAIT;

	if(line[strlen(line)-1]!='\n') {
		fseek(fp,start_of_line,SEEK_SET);
		return CANDIDATE_WAIT;
	}

	if(!strcmp(line,"EOF\n")) {
		more_candidates = 0;
		return CANDIDATE_EOF;
	}

	int n = sscanf(line, "%s %s %[^\n]", name1, name2, extra_data);
	if(n!=3) fatal("candidate file is corrupted: %s\n",line);

	candidates_loaded++;

	return CANDIDATE_SUCCESS;
}

struct cseq * sequence_lookup( struct hash_table *h, const char *name )
{
	struct cseq *c = hash_table_lookup(h,name);
	if(c) return c;

	while(1) {
		c = cseq_read(sequence_file);
		if(!c) break;

		sequences_loaded++;

		hash_table_insert(h,c->name,c);
		if(!strcmp(name,c->name)) return c;

		int size = hash_table_size(h);
		if(size%100000 ==0 )debug(D_DEBUG,"loaded %d sequences",size);
	}

	fatal("candidate file contains invalid sequence name: %s\n",name);
	return 0;
}

static void buffer_ensure( char **buffer, int *buffer_size, int buffer_used, int buffer_delta )
{
	int buffer_needed = buffer_used + buffer_delta;

	if(buffer_needed>*buffer_size) {
		do {
			*buffer_size *=2 ;
		} while( buffer_needed > *buffer_size );

		*buffer = realloc(*buffer,*buffer_size);
	}
}

static struct work_queue_task * task_create( struct hash_table *sequence_table )
{
	char aname1[CAND_FILE_LINE_MAX];
	char aname2[CAND_FILE_LINE_MAX];
	char aextra[CAND_FILE_LINE_MAX];

	char bname1[CAND_FILE_LINE_MAX];
	char bname2[CAND_FILE_LINE_MAX];
	char bextra[CAND_FILE_LINE_MAX];

	struct cseq *s1, *s2;

	int result = candidate_read(candidate_file,aname1,aname2,aextra);
	if(result!=CANDIDATE_SUCCESS) return 0;

	s1 = sequence_lookup(sequence_table,aname1);
	s2 = sequence_lookup(sequence_table,aname2);

	static int buffer_size = 1024;
	char *buffer = malloc(buffer_size);
	int buffer_pos = 0;

	buffer_ensure(&buffer,&buffer_size,buffer_pos,cseq_size(s1)+cseq_size(s2)+10);

	buffer_pos += cseq_sprint(&buffer[buffer_pos],s1,"");
	buffer_pos += cseq_sprint(&buffer[buffer_pos],s2,aextra);

	int npairs = 1;
	int nseqs = 2;

	do {
		result = candidate_read(candidate_file,bname1,bname2,bextra);
		if(result!=CANDIDATE_SUCCESS) break;

		s1 = sequence_lookup(sequence_table,bname1);
		s2 = sequence_lookup(sequence_table,bname2);

		if(strcmp(aname1,bname1)!=0) {
			buffer_ensure(&buffer,&buffer_size,buffer_pos,cseq_size(s1)+cseq_size(s2)+10);
			buffer_pos += cseq_sprint(&buffer[buffer_pos],0,"");
			buffer_pos += cseq_sprint(&buffer[buffer_pos],s1,"");
			strcpy(aname1,bname1);
			strcpy(aname2,bname2);
			strcpy(aextra,bextra);
			nseqs++;
		}

		buffer_ensure(&buffer,&buffer_size,buffer_pos,cseq_size(s2)+10);
		buffer_pos += cseq_sprint(&buffer[buffer_pos],s2,bextra);

		nseqs++;
		npairs++;

	} while( npairs < max_pairs_per_task );

	debug(D_DEBUG,"created task of %d sequences and %d comparisons\n",nseqs,npairs);

	char cmd[strlen(align_prog)+strlen(align_prog_args)+100];

	sprintf(cmd, "./%s %s aligndata", "align", align_prog_args);

	struct work_queue_task *t = work_queue_task_create(cmd);
	work_queue_task_specify_input_file(t, align_prog, "align");
	work_queue_task_specify_input_buf(t, buffer, buffer_pos, "aligndata");

	free(buffer);

	return t;
}

int main(int argc, char *argv[])
{
	char c;

	const char *progname = "sand_align_master";

	debug_config(progname);

	// By default, turn on fast abort option since we know each job is of very similar size (in terms of runtime).
	// One can also set the fast_abort_multiplier by the '-f' option.
	wq_option_fast_abort_multiplier = 10;

	while((c = getopt(argc, argv, "e:F:p:n:d:o:vh")) != (char) -1) {
		switch (c) {
		case 'p':
			port = atoi(optarg);
			break;
		case 'n':
			max_pairs_per_task = atoi(optarg);
			break;
		case 'e':
			align_prog_args = strdup(optarg);
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'F':
			wq_option_fast_abort_multiplier = atof(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'v':
			show_version(progname);
			exit(0);
			break;
		case 'h':
			show_help(progname);
			exit(0);
			break;
		}
	}


	if((argc - optind) != 4) {
		show_help(progname);
		exit(1);
	}

	if(!find_executable(argv[optind],"PATH",align_prog,sizeof(align_prog))) {
		fprintf(stderr, "%s: couldn't find alignment program %s: is it in your path?\n",progname,argv[optind]);
		return 1;
	}
			

	candidate_file_name = argv[optind + 1];
	sequence_file_name = argv[optind + 2];
	output_file_name = argv[optind + 3];

	sequence_file = fopen(sequence_file_name,"r");
	if(!sequence_file) {
		fprintf(stderr, "%s: couldn't open sequence file %s: %s\n", progname, sequence_file_name, strerror(errno));
		return 1;
	}

	candidate_file = fopen(candidate_file_name,"r");
	if(!candidate_file) {
		fprintf(stderr, "%s: couldn't open candidate file %s: %s\n", progname,candidate_file_name, strerror(errno));
		return 1;
	}

	output_file = fopen(output_file_name, "a");
	if(!output_file) {
		fprintf(stderr, "%s: couldn't open output file %s: %s\n", progname,output_file_name, strerror(errno));
		return 1;
	}

	queue = work_queue_create(port);
	if(!queue) {
		fprintf(stderr, "%s: couldn't listen on port %d: %s\n",progname,port,strerror(errno));
		return 1;
	}

	sequence_table = hash_table_create(20000001,0);

	start_time = time(0);

	struct work_queue_task *t;

	while( more_candidates || !work_queue_empty(queue) ) {

		if(last_display_time < time(0))
			display_progress(queue);

		while( more_candidates && work_queue_hungry(queue) ) {
			t = task_create( sequence_table );
			if(t) {
				work_queue_submit(queue,t);
				tasks_submitted++;
			} else {
				break;
			}
		}

		if(work_queue_empty(queue)) {
			if(more_candidates) sleep(5);
		} else {
			if(work_queue_hungry(queue)) {
				t = work_queue_wait(queue,0);
			} else {
				t = work_queue_wait(queue,5);
			}
			if(t) task_complete(t);
		}
	}

	display_progress(queue);

	printf("Completed %i tasks in %i seconds\n", tasks_done, (int) (time(0) - start_time));

	fclose(output_file);
	fclose(candidate_file);

	work_queue_delete(queue);

	return 0;
}
