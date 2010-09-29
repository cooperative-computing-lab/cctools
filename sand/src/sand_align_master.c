
/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "sand_align.h"
#include "sequence.h"
#include "compressed_sequence.h"

static struct work_queue *queue = 0;
static int port = 9068;
static const char *function = 0;
static const char *function_args = "";
static const char *candidate_file_name;
static const char *sequence_file_name;
static const char *output_file_name;

static FILE *candidate_file;
static FILE *output_file;

static time_t start_time = 0;
static time_t last_display_time = 0;

static int more_candidates = 1;
static int tasks_submitted = 0;
static int tasks_done = 0;
static timestamp_t tasks_runtime = 0;
static timestamp_t tasks_filetime = 0;

static int max_pairs_per_task = 10000;

#define CANDIDATE_SUCCESS 0
#define CANDIDATE_EOF 1
#define CANDIDATE_WAIT 2

static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	printf("Use: %s [options] <command> <candidate pairs file> <sequences file> <outputdata>\n", cmd);
	printf("where options are:\n");
	printf(" -p <port>      Port number for queue master to listen on.\n");
	printf(" -n <number>    Maximum number of candidates per task. (default is %d)\n",max_pairs_per_task);
	printf(" -d <subsystem> Enable debugging for this subsystem.  (Try -d all to start.)\n");
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

	double speedup = (((double) tasks_runtime / 1000000.0) / (time(0) - start_time));

	if(row_count==0) {
		printf("%7s | %4s %4s %4s | %6s %4s %4s %6s | %6s %6s %8s | %s\n",
			"Time", "WI", "WR", "WB", "TS", "TW", "TR", "TD", "AR", "AF", "WS", "Speedup");
		row_count = row_limit;
	}

	printf("%6ds | %4d %4d %4d | %6d %4d %4d %6d | %6.02lf %6.02lf %8.02lf | %.02lf\n",
		(int) (time(0) - start_time),
		info.workers_init,
		info.workers_ready,
		info.workers_busy,
		tasks_submitted,
		info.tasks_waiting,
		info.tasks_running,
		tasks_done,
		(tasks_runtime / 1000000.0) / tasks_done,
		(tasks_filetime / 1000000.0) / tasks_done,
		(tasks_runtime / 1000000.0) / (tasks_filetime / 1000000.0),
		speedup);

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

static struct hash_table * build_sequence_library( const char *filename )
{
	struct hash_table *h = hash_table_create(20000001, 0);
	if(!h) fatal("couldn't create hash table\n");

	FILE *file = fopen(filename, "r");
	if(!file) fatal("couldn't open %s: %s\n",filename,strerror(errno));

	struct cseq *c;

	while((c = cseq_read(file))) {
		hash_table_insert(h,c->name,c);
	}

	fclose(file);

	return h;
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

	return CANDIDATE_SUCCESS;
}

struct cseq * sequence_lookup( struct hash_table *h, const char *name )
{
	struct cseq *c = hash_table_lookup(h,name);
	if(!c) fatal("candidate file contains invalid sequence name: %s\n",name);
	return c;
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

#define NAME_MAX 256

static struct work_queue_task * task_create( struct hash_table *sequence_table )
{
	char aname1[NAME_MAX];
	char aname2[NAME_MAX];
	char aextra[NAME_MAX];

	char bname1[NAME_MAX];
	char bname2[NAME_MAX];
	char bextra[NAME_MAX];

	struct cseq *s1, *s2;

	static int buffer_size = 1024;
	char *buffer = malloc(buffer_size);
	int buffer_pos = 0;

	int result = candidate_read(candidate_file,aname1,aname2,aextra);
	if(result!=CANDIDATE_SUCCESS) return 0;

	s1 = sequence_lookup(sequence_table,aname1);
	s2 = sequence_lookup(sequence_table,aname2);

	buffer_ensure(&buffer,&buffer_size,buffer_pos,cseq_size(s1)+cseq_size(s2)+10);

	buffer_pos += cseq_sprint(&buffer[buffer_pos],s1,aextra);
	buffer_pos += cseq_sprint(&buffer[buffer_pos],s2,"");

	int npairs = 1;

	do {
		result = candidate_read(candidate_file,bname1,bname2,bextra);
		if(result!=CANDIDATE_SUCCESS) break;

		s1 = sequence_lookup(sequence_table,bname1);
		s2 = sequence_lookup(sequence_table,bname2);

		if(strcmp(aname1,bname1)!=0) {
			buffer_ensure(&buffer,&buffer_size,buffer_pos,cseq_size(s1)+cseq_size(s2)+10);
			buffer_pos += cseq_sprint(&buffer[buffer_pos],0,"");
			buffer_pos += cseq_sprint(&buffer[buffer_pos],s1,bextra);
			strcpy(aname1,bname1);
			strcpy(aname2,bname2);
			strcpy(aextra,bextra);
		}

		buffer_ensure(&buffer,&buffer_size,buffer_pos,cseq_size(s2)+10);
		buffer_pos += cseq_sprint(&buffer[buffer_pos],s2,"");

		npairs++;

	} while( npairs < max_pairs_per_task );

	char cmd[strlen(function)+strlen(function_args)+100];

	sprintf(cmd, "./%s %s aligndata", function, function_args);

	struct work_queue_task *t = work_queue_task_create(cmd);
	work_queue_task_specify_input_file(t, function, function);
	work_queue_task_specify_input_buf(t, buffer, buffer_pos, "aligndata");

	return t;
}

int main(int argc, char *argv[])
{
	char c;

	const char *progname = "sand_align_master";

	debug_config(progname);

	while((c = getopt(argc, argv, "e:p:n:d:o:vh")) != (char) -1) {
		switch (c) {
		case 'p':
			port = atoi(optarg);
			break;
		case 'n':
			max_pairs_per_task = atoi(optarg);
			break;
		case 'e':
			function_args = strdup(optarg);
			break;
		case 'd':
			debug_flags_set(optarg);
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

	function = argv[optind];
	candidate_file_name = argv[optind + 1];
	sequence_file_name = argv[optind + 2];
	output_file_name = argv[optind + 3];

	candidate_file = fopen(candidate_file_name,"r");
	if(!candidate_file) {
		fprintf(stderr, "couldn't open candidate file %s: %s\n", candidate_file_name, strerror(errno));
		return 1;
	}

	output_file = fopen(output_file_name, "a");
	if(!output_file) {
		fprintf(stderr, "couldn't open output file %s: %s\n", output_file_name, strerror(errno));
		return 1;
	}

	queue = work_queue_create(port);
	if(!queue) {
		fprintf(stderr, "couldn't listen on port %d: %s\n",port,strerror(errno));
		return 1;
	}

	start_time = time(0);
	last_display_time = 0;

	printf("Loading sequences...\n");
	time_t temp_time = time(0);
	struct hash_table *sequence_table = build_sequence_library(sequence_file_name);
	printf("%i sequences loaded in %6ds\n", hash_table_size(sequence_table), (int) (time(0) - temp_time));

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
			if(more_candidates) {
				sleep(5);
			}
		} else {
			t = work_queue_wait(queue,5);
			if(t) task_complete(t);
		}
	}

	printf("%7s | %4s %4s %4s | %6s %4s %4s %4s | %6s %6s %6s %8s | %s\n", "Time", "WI", "WR", "WB", "TS", "TW", "TR", "TC", "TD", "AR", "AF", "WS", "Speedup");
	display_progress(queue);
	work_queue_delete(queue);
	printf("Completed %i tasks in %i seconds\n", tasks_done, (int) (time(0) - start_time));
	return 0;
}
