
/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "assembly_master.h"

static const char *function = 0;
static struct work_queue *queue = 0;
static int port = 9068;
static const char *candidate_file;
static const char *sequence_data_file;
static const char *outfile;
static FILE *logfile;
static int priority_mode = 0;

static double sequential_run_time;
static time_t start_time = 0;
static time_t last_display_time = 0;

static int tasks_done = 0;
static timestamp_t tasks_runtime = 0;
static timestamp_t tasks_filetime = 0;

int global_count = 0;
int fast_fill = 50; // how many tasks to fast-submit on start.

static int NUM_PAIRS_PER_FILE;
static int LIMIT;

static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	printf("Use: %s [options] <command> <candidate pairs file> <sequences file> <outputdata>\n", cmd);
	printf("where options are:\n");
	printf(" -p <port>      Port number for queue master to listen on.\n");
	printf(" -n <number>    Maximum number of candidates per task.\n");
	printf(" -d <subsystem> Enable debugging for this subsystem.  (Try -d all to start.)\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

static void display_progress( struct work_queue *q )
{
        struct work_queue_stats info;
        time_t current = time(0);
        work_queue_get_stats(queue,&info);
        if(current==start_time) current++;
        double speedup = (sequential_run_time*tasks_done)/(current-start_time);
        printf("%6ds | %4d %4d %4d | %6d %4d %4d %4d | %6d %6.02lf %6.02lf %8.02lf | %.02lf\n",(int)(time(0)-start_time),info.workers_init,info.workers_ready,info.workers_busy,global_count,info.tasks_waiting,info.tasks_running,info.tasks_complete,tasks_done,(tasks_runtime/1000000.0)/tasks_done,(tasks_filetime/1000000.0)/tasks_done,(tasks_runtime/1000000.0)/(tasks_filetime/1000000.0),speedup);
        last_display_time = current;
}

struct sequence* seqdup(struct sequence* s) {
    struct sequence* n;
    n = (struct sequence*)malloc(sizeof(struct sequence));
    strcpy(n->sequence_name,s->sequence_name);
    n->num_bases = s->num_bases;
    n->num_bytes = s->num_bytes;
    n->sequence_data = malloc(s->num_bytes);
    memcpy(n->sequence_data,s->sequence_data,s->num_bytes);
    return n;
}

static struct hash_table* build_completed_table(const char* filename, int* numDone)
{

    static struct hash_table* t;
    t = hash_table_create(20000000,0);

    char A_sequence_name[ASSEMBLY_LINE_MAX];
    char B_sequence_name[ASSEMBLY_LINE_MAX];
    char tmp[2*ASSEMBLY_LINE_MAX+1];
    int i = 0;

    char ob,cb; // test character for the brace so that we can detect output truncated before first real results token or after last real results token. 
    char ori;
    char olt;
    int ahg;
    int bhg;
    float qua;
    int mno;
    int mxo;
    int pct;
    
    /*
      {OVL
      afr:1101685031240
      bfr:1101693766107
      ori:N
      olt:D
      ahg:63
      bhg:1
      qua:0.002037
      mno:0
      mxo:0
      pct:261
      }
    */

    FILE* infile = fopen(filename,"r");
    if(!infile)
	return t;
    int j;
    while((j = fscanf(infile," %cOVL afr:%s bfr:%s ori:%c olt:%c ahg:%i bhg:%i qua:%f mno:%i mxo:%i pct:%i %c",&ob,A_sequence_name,B_sequence_name,&ori,&olt,&ahg,&bhg,&qua,&mno,&mxo,&pct,&cb)) == 12 && ob=='{' && cb == '}') {
	sprintf(tmp,"%s-%s",A_sequence_name,B_sequence_name);
	if(hash_table_insert(t,tmp,"") != 1) {
	    if(!hash_table_lookup(t,tmp)) {
		printf("Could not add completed result: %s \n",tmp);
	    }
	    else {
		printf("Duplicate result: %s \n",tmp);
	    }
	}
	else {
	    //printf("Added result: %s (%i)\n",tmp,j);
	    i++;
	}
    }
    *numDone = i;

    if(!feof(infile) || j > 0) { // bailed out of fscanf, but not done
	printf("Unexpected results data, %i uncertain tokens. Possibly corrupted. Please examine %s.\n",j,filename);
	exit(1);
    }

    fclose(infile);
    return t;
    
}

int isAllWhitespace(char* s) {
    int i;
    for(i=0; i<strlen(s); i++)
	if(!isspace(s[i]))
	   return 0;
    return 1;
}

static int confirm_output(char* output) {

    char A_sequence_name[ASSEMBLY_LINE_MAX];
    char B_sequence_name[ASSEMBLY_LINE_MAX];
    int i = 0;

    char ori;
    char olt;
    int ahg;
    int bhg;
    float qua;
    int mno;
    int mxo;
    int pct;

    char* buf = strdup(output);
    char* rec = strtok(buf,"}");
    if(!rec) {
	debug(D_DEBUG,"No } found! Output:\n%s\n",buf);
	return 0;
    }
    while(rec) {
	if(sscanf(rec," {OVL afr:%s bfr:%s ori:%c olt:%c ahg:%i bhg:%i qua:%f mno:%i mxo:%i pct:%i ",A_sequence_name,B_sequence_name,&ori,&olt,&ahg,&bhg,&qua,&mno,&mxo,&pct) == 10) {
	    i++;
	    rec = strtok(0,"}");
	}
	else
	{
	    if(isAllWhitespace(rec)) { // all that's left is a newline
		free(buf);
		return i;
	    }
	    debug(D_DEBUG,"Confirm Output Error. Buffer:\n=====\n%s\n=====\n",buf);
	    if(sscanf(rec," {OVL afr:%s bfr:%s ", A_sequence_name,B_sequence_name) == 2)
		fprintf(stderr, "Unexpected output format for comarison of %s and %s. ", A_sequence_name,B_sequence_name);
	    else
		fprintf(stderr, "Unexpected output format. ");
	    return 0;
	}
    }
    free(buf);
    return i;
}
    

static struct hash_table* build_sequence_library(const char* filename)
{
    static struct hash_table* h;
    h = hash_table_create(20000000,0);
    if(!h) {
	fprintf(stderr,"Couldn't create hash table.\n");
	exit(1);
    }

    struct sequence s;
    struct sequence* verification;
    FILE* infile = fopen(filename,"r");
    if(!infile) {
	fprintf(stderr,"Couldn't open file %s.\n",filename);
	exit(1);
    }
    while(fscanf(infile," >%s %i %i%*1[ ]%*1[\n]",s.sequence_name,&s.num_bases,&s.num_bytes) == 3) {
	s.sequence_data = malloc(s.num_bytes);
	if(s.sequence_data) {
	    if(fread(s.sequence_data,1,s.num_bytes,infile) == s.num_bytes) {
		hash_table_insert(h,s.sequence_name,seqdup(&s));
	    }
	    else
	    {
		fprintf(stderr,"Sequence %s read error.\n",s.sequence_name);
		exit(1);
	    }
	}
	else
	{
	    fprintf(stderr,"Sequence %s is too long (%i bytes), could not allocate memory\n",s.sequence_name,s.num_bytes);
	    exit(1);
	}
	verification = (struct sequence*)hash_table_lookup(h,s.sequence_name);
	printf("%s Added %i bytes from %c(%i) to %c(%i)\n",s.sequence_name,verification->num_bytes,verification->sequence_data[0],(int)verification->sequence_data[0],verification->sequence_data[verification->num_bytes - 1],(int)verification->sequence_data[verification->num_bytes - 1]);
	free(s.sequence_data);
	s.sequence_data = NULL; 
    }

    return h;
}


static int handle_done_task(struct work_queue_task *t) {
    if(!t)
	return 0;
    
    if(t->return_status==0) {
	if(confirm_output(t->output)) {
	    debug(D_DEBUG,"Completed task!\n");
	    fputs(t->output,logfile);
	    fflush(logfile);
	    tasks_done++;
	    tasks_runtime+=(t->finish_time-t->start_time);
	    tasks_filetime+=t->total_transfer_time;
	}
	else { // error in output
	    fprintf(stderr,"Failure of task on host %s. Output not confirmed:\n%s\n",t->host,t->output);
	    return 0;
	}
    } else {
	fprintf(stderr,"Failure of task on host %s. Failed with result: %i and return value %i. Output:\n%s\n",t->host,t->result,t->return_status,t->output);
	return 0;
    }
    work_queue_task_delete(t);
    return 1;
}

static int get_task_ratio(  struct work_queue *q ) {
    struct work_queue_stats info;
    int i,j;
    work_queue_get_stats(queue,&info);

    //i = 2 * number of current workers
    //j = # of queued tasks.
    //i-j = # of tasks to queue to re-reach the status quo.
    i = (2*(info.workers_init + info.workers_ready + info.workers_busy));
    j = (info.tasks_waiting);
    return i-j;
}

static int task_consider( void* taskfiledata, int size )
{
	char cmd[2*MAX_FILENAME+4];
	char job_filename[10];
	string_cookie( job_filename, 10 );
	sprintf(cmd,"./%s < %s",function,job_filename);

	struct work_queue_task* t;
	

	while(!work_queue_hungry(queue)) {
	    handle_done_task(work_queue_wait(queue, 5));
	}
	t = work_queue_task_create(cmd);
	work_queue_task_specify_input_file( t, function, function);
	work_queue_task_specify_input_buf( t, taskfiledata, size, job_filename);
	work_queue_submit(queue,t);
	global_count++;

	return 1;
}

static int build_jobs(const char* candidate_filename, struct hash_table* h, struct hash_table* t)
{

    char tmp[(2*SEQUENCE_ID_MAX)+2];
    char sequence_name1[SEQUENCE_ID_MAX+1];
    char sequence_name2[SEQUENCE_ID_MAX+1];
    
    struct sequence* s1;
    struct sequence* s2;

    int alignment_flag;
    
    int already_done = hash_table_size(t);
    char *key;
    void *value;
        
    int pair_count = 0;

    int res;
    FILE* fp = fopen(candidate_filename,"r");
    if(!fp) {
	fprintf(stderr,"Couldn't open file %s.\n",candidate_filename);
	exit(1);
    }

    char* buf = NULL;
    char* ins = NULL;
    while(!buf) {
	    buf = (char*) malloc(LIMIT*sizeof(char));
	    if(!buf)
	    {
		fprintf(stderr,"Out of memory for buf! Waiting for a bit.\n");
		handle_done_task(work_queue_wait(queue,WAITFORTASK));
	    }
    }
    ins = buf;
    while(pair_count == 0) {
	if(fscanf(fp,"%s %s %i",sequence_name1,sequence_name2, &alignment_flag ) == 3) {
	    sprintf(tmp,"%s-%s",sequence_name1,sequence_name2);
	    if(!hash_table_lookup(t,tmp)) {
		s1 = (struct sequence*) hash_table_lookup(h, sequence_name1);
		s2 = (struct sequence*) hash_table_lookup(h, sequence_name2);
		res = sprintf(ins,">%s %i %i\n",s1->sequence_name,s1->num_bases,s1->num_bytes);
		ins+=res;
		memcpy(ins,s1->sequence_data,s1->num_bytes);
		ins+=s1->num_bytes;
		res = sprintf(ins,"\n>%s %i %i %i\n",s2->sequence_name,s2->num_bases,s2->num_bytes,alignment_flag);
		ins+=res;
		memcpy(ins,s2->sequence_data,s2->num_bytes);
		ins+=s2->num_bytes;
		
		pair_count++;
	    }
	    else {
		already_done--;
		if(already_done == 0) {
		    /*cleanup t*/
		    hash_table_firstkey(t);
		    while(hash_table_nextkey(t,&key,&value)) {
			hash_table_remove(t,key);
		    }
		    if(hash_table_size(t) != 0) {
			fprintf(stderr,"Could not clear completed pairs hash table!\n");
		    }
		}
		if(already_done % 10000 == 0)
		    fprintf(stderr,"%i completed pairs left\n",already_done);
	    }
	
	}
	else {
	    if(!feof(fp)) {
		fprintf(stderr,"Badly formatted candidate file %s.\n",candidate_filename);
		exit(1);
	    }
	    else {
		fprintf(stderr,"All candidate pairs in %s are already complete in provided output!\n",candidate_filename);
		exit(0);
	    }
	}
    }
    
    while(fscanf(fp,"%s %s %i",sequence_name1,sequence_name2, &alignment_flag) == 3)
    {
	sprintf(tmp,"%s-%s",sequence_name1,sequence_name2);
	if(!hash_table_lookup(t,tmp)) {
	    if(!strcmp(sequence_name1,s1->sequence_name) && pair_count < NUM_PAIRS_PER_FILE) { // same first sequence, not exceeded max pairs.
		s2 = (struct sequence*) hash_table_lookup(h, sequence_name2);
		if(!s2)
		{
		    fprintf(stderr,"No such sequence: %s",sequence_name2);
		    exit(1);
		}
		res = sprintf(ins,"\n>%s %i %i %i\n",s2->sequence_name,s2->num_bases,s2->num_bytes,alignment_flag);
		ins+=res;
		memcpy(ins,s2->sequence_data,s2->num_bytes);
		ins+=s2->num_bytes;

		pair_count++;
	    }
	    else {
		if(!(pair_count < NUM_PAIRS_PER_FILE)){ // exceeded max pairs (may or may not be same first sequence, doesn't matter)
		    //printf("Count exceeded so doing new file with %s,%s\n",sequence_name1,sequence_name2);
		    task_consider(buf,ins-buf);
		    pair_count = 0;
		    buf[0]='\0';
		    ins = buf;
		}
		else { //different first sequence
		    //printf("%s!=%s so adding separator before %s,%s\n",sequence_name1,sequence_name0,sequence_name1,sequence_name2);
		    sprintf(ins,"\n>>\n");
		    ins+=strlen(ins);
		}

		//printf("PC:%i\n",pair_count);
		s1 = (struct sequence*) hash_table_lookup(h, sequence_name1);
		if(!s1)
		{
		    fprintf(stderr,"No such sequence: %s",sequence_name1);
		    exit(1);
		}
		s2 = (struct sequence*) hash_table_lookup(h, sequence_name2);
		if(!s2)
		{
		    fprintf(stderr,"No such sequence: %s",sequence_name2);
		    exit(1);
		}
		//printf("@%i:>%s %i %i\n",(int)(ins-buf),s1->sequence_name,s1->num_bases,s1->num_bytes);
		res = sprintf(ins,">%s %i %i\n",s1->sequence_name,s1->num_bases,s1->num_bytes);
		ins+=res;
		memcpy(ins,s1->sequence_data,s1->num_bytes);
		ins+=s1->num_bytes;
		res = sprintf(ins,"\n>%s %i %i %i\n",s2->sequence_name,s2->num_bases,s2->num_bytes,alignment_flag);
		ins+=res;
		memcpy(ins,s2->sequence_data,s2->num_bytes);
		ins+=s2->num_bytes;

		pair_count++;
	    }
	}
	else {
	    already_done--;
	    if(already_done == 0) {
		/*cleanup t*/
		hash_table_firstkey(t);
		while(hash_table_nextkey(t,&key,&value)) {
		    hash_table_remove(t,key);
		}
		if(hash_table_size(t) != 0) {
		    fprintf(stderr,"Could not clear completed pairs hash table!\n");
		}
		if(already_done % 10000 == 0)
		    fprintf(stderr,"%i completed pairs left\n",already_done);
	    }
	}
    }
    if(buf[0] != '\0') {
	task_consider(buf,ins-buf);
    }
    
    free(buf);
    return 0;
    
}

int main( int argc, char *argv[] )
{
	char c;

	const char *progname = "assembly";
	
	debug_config(progname);

	int task_size_specified = 0;
	
	while((c=getopt(argc,argv,"p:n:Pd:o:vh"))!=(char)-1) {
		switch(c) {
		case 'p':
			port = atoi(optarg);
			break;
		case 'n':
			task_size_specified = atoi(optarg);
			break;
		case 'P':
			priority_mode = 1;
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


	if(task_size_specified != 0) {
	       NUM_PAIRS_PER_FILE = task_size_specified;
	}
	else {
		NUM_PAIRS_PER_FILE = 1000;
	}
	LIMIT = ((NUM_PAIRS_PER_FILE)*(SEQUENCE_ID_MAX+ASSEMBLY_LINE_MAX+3));
		
	sequential_run_time = NUM_PAIRS_PER_FILE*.04;

	if( (argc-optind)!=4 ) {
		show_help(progname);
		exit(1);
	}

	function = argv[optind];
	candidate_file=argv[optind+1];
	sequence_data_file=argv[optind+2];
	outfile=argv[optind+3];


	struct rlimit rl;

	/* Obtain the current limits. */
	getrlimit (RLIMIT_AS, &rl);
	/* Set a CPU limit of 1 second. */
	//rl.rlim_cur = 1000000000;
	//rl.rlim_max = 3000000000;
	//setrlimit (RLIMIT_AS, &rl);
			
			
	logfile = fopen(outfile,"a");
	if(!logfile) {
	    fprintf(stderr,"couldn't open %s for append: %s\n",outfile,strerror(errno));
	    return 1;
	}

	time_t loop_time = time(0);
	queue = work_queue_create(port,loop_time+300);
	if(!queue) {
	    fprintf(stderr,"couldn't create queue on port %i, timed out\n",port);
	    return 1;
	}
	
	start_time = time(0);
	last_display_time = 0;


	printf("Building sequence library\n");
	time_t temp_time = time(0);
	struct hash_table* mh = build_sequence_library(sequence_data_file);
	printf("Time to build library (%i sequences): %6ds\n", hash_table_size(mh),(int)(time(0)-temp_time));
	
	int num_complete = 0;
	printf("Building completed results\n");
	temp_time = time(0);
	struct hash_table* mt = build_completed_table(outfile, &num_complete);
	printf("%i candidate alignments already completed.\n",num_complete);
	printf("Time to build completed results (%i candidates): %6ds\n", hash_table_size(mt),(int)(time(0)-temp_time));
	
	printf("%7s | %4s %4s %4s | %6s %4s %4s %4s | %6s %6s %6s %8s | %s\n","Time","WI","WR","WB","TS","TW","TR","TC","TD","AR","AF","WS","Speedup");
	build_jobs(candidate_file,mh,mt);
	printf("%7s | %4s %4s %4s | %6s %4s %4s %4s | %6s %6s %6s %8s | %s\n","Time","WI","WR","WB","TS","TW","TR","TC","TD","AR","AF","WS","Speedup");
		
	struct work_queue_task *t;


	while(1) {
		if(time(0)!=last_display_time) display_progress(queue);
		t = work_queue_wait(queue,WAITFORTASK);
		if(! handle_done_task(t)) break;		
	}
	
	display_progress(queue);
	work_queue_delete(queue);
	printf("Completed %i tasks in %i seconds\n",tasks_done,(int)(time(0)-start_time));
	return 0;
}


