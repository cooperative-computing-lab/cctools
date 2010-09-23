
/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "sand_align.h"

static const char *function = 0;
static struct work_queue *queue = 0;
static int port = 9068;
static const char *candidate_file;
static const char *sequence_data_file;
static const char *outfile;
static FILE *logfile;
static int priority_mode = 0;
static char end_char = '\0';

static double sequential_run_time;
static time_t start_time = 0;
static time_t last_display_time = 0;
static time_t last_flush_time = 0;

static int tasks_done = 0;
static timestamp_t tasks_runtime = 0;
static timestamp_t tasks_filetime = 0;

int global_count = 0;
int fast_fill = 50; // how many tasks to fast-submit on start.

static int NUM_PAIRS_PER_FILE;
static int LIMIT;

char function_args[256];

#define GET_CAND_LINE_RESULT_SUCCESS 0
#define GET_CAND_LINE_RESULT_EOF 1
#define GET_CAND_LINE_RESULT_WAIT 2
#define GET_CAND_LINE_RESULT_BAD_FORMAT 3

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
	printf(" -f <character>	Stop when you see this character (for use in conjunction\n");
	printf("				with candidate selection\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

static void display_progress( struct work_queue *q )
{
	struct work_queue_stats info;
	time_t current = time(0);
	work_queue_get_stats(q,&info);
	if(current==start_time) current++;

	double speedup = (((double)tasks_runtime/1000000.0)/ (time(0)-start_time));
	printf("%6ds | %4d %4d %4d | %6d %4d %4d %4d | %6d %6.02lf %6.02lf %8.02lf | %.02lf\n",
	   (int)(time(0)-start_time),
	   info.workers_init,info.workers_ready,info.workers_busy,
	   global_count,info.tasks_waiting,info.tasks_running,info.tasks_complete,
	   tasks_done,(tasks_runtime/1000000.0)/tasks_done,(tasks_filetime/1000000.0)/tasks_done,(tasks_runtime/1000000.0)/(tasks_filetime/1000000.0),
	   speedup);
	last_display_time = current;
	if (current - last_flush_time >= 5)
	{
		fflush(stdout);
		last_flush_time = current;
	}
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

// returns 1 if all whitespace, 0 if not.
int isAllWhitespace(char* s) {
    int i;
    for(i=0; i<strlen(s); i++)
	if(!isspace((int)s[i]))
	   return 0;
    return 1;
}

// returns 1 if valid, 0 if not.
int isValidClosing(char* s) {
    int i;
    int closeFound = 0;
    for(i=0;i<strlen(s);i++)
    {
	if(s[i]==']') {
	    if(closeFound) {
		fprintf(stderr, "Multiple OVL Record Envelope Closing Characters in task!\n");
		return 0;
	    }
	    else {
		closeFound=1;
	    }
	}
	else { //make sure nothing except close and whitespace 
	    if(!isspace((int)s[i])) {
		return 0;
	    }
	}
    }
    if(closeFound)
	return 1;
    return 0;
}


// returns -1 on error, 0 on blank output (just an open and close envelope), and the index into s of the first record if valid and non-blank.
int findFirstOVLRecord(char* s) {

    int i;
    int openFound = 0;
    int closeFound = 0;
    int firstOVLRecord = strpos(s,'{');
    char next_char_to_find;
        
    if(firstOVLRecord != -1) { // there's at least one OVL record opener '{'
	next_char_to_find = '[';
	for(i=0;i<firstOVLRecord;i++) // up until then, the only thing should be a [ and maybe whitespace
	{
	    if(s[i]==next_char_to_find) {
		if(openFound) {
		    fprintf(stderr, "Multiple OVL Record Envelope Opening Characters in task!\n");
		    return -1;
		}
		else {
		    openFound=1;
		}
	    }
	    else { //make sure nothing except an open and whitespace before first record
		if(!isspace((int)s[i])) {
		    fprintf(stderr, "Non-whitespace before first OVL Record in task!\n");
		    return -1;
		}
	    }
	}
	if(!openFound) { // if we did not find a '[', fail.
	    fprintf(stderr, "No OVL Record Envelope Opening Character in task!\n");
	    return -1;
	}
	// if we're here, we have a successful start.
	return firstOVLRecord;
    }
    else {// there's no OVL record opener '{', s must be of the format "(whitespace)*[(whitespace)*](whitespace)*"
	next_char_to_find = '[';
	for(i=0;i<strlen(s);i++)
	{
	    if(s[i]==next_char_to_find) {
		if(!openFound) { //we've hit the '['.
		    openFound=1;
		    next_char_to_find = ']';
		}
		else { // we've hit the ']'
		    closeFound=1;
		    next_char_to_find = '\0';
		}
	    }
	    else { //make sure nothing except an open and whitespace before first record
		if(!isspace((int)s[i])) {
		    fprintf(stderr, "Non-whitespace before first OVL Record in task!\n");
		    return -1;
		}
	    }
	}
	if(!openFound) { // if we did not find a '[', fail.
	    fprintf(stderr, "No OVL Record Envelope Opening Character in presumed blank task! Output:\n%s\n",s);
	    return -1;
	}
	if(!closeFound) { // if we did not find a ']', fail.
	    fprintf(stderr, "No OVL Record Envelope Closing Character in presumed blank task! Output:\n%s\n",s);
	    return -1;
	}
	return 0;
    }
}


char* removeEnvelope(char* output) {

    char A_sequence_name[ASSEMBLY_LINE_MAX];
    char B_sequence_name[ASSEMBLY_LINE_MAX];
    
    int i = 0;
	int obuf_len;
    int firstOVLRecord;
    
    char ori;
    char olt;
    int ahg;
    int bhg;
    float qua;
    int mno;
    int mxo;
    int pct;

    char* obuf = strdup(output);
	obuf_len = strlen(obuf);
    char* final_buf;
    firstOVLRecord=findFirstOVLRecord(obuf);     // find envelope opening.
    if(firstOVLRecord==-1) { // error
		debug(D_DEBUG,"Unexpected output format parsing OVL envelope! Output:\n%s\n",obuf);
		free(obuf);
		return NULL;
    }
    if(firstOVLRecord==0) { // blank task
		free(obuf);
		final_buf = malloc(1*sizeof(char));
		final_buf[0]='\0';
		return final_buf;
    }
    //else, i is the first character of the first record.
    char* buf = &(obuf[firstOVLRecord]);
    char* rec = strtok(buf,"}");
    if(!rec) {
		debug(D_DEBUG,"No } found! Output:\n%s\n",obuf);
		buf=NULL;
		free(obuf);
		return NULL;
    }
    while(rec) {
		if(sscanf(rec," {OVL afr:%s bfr:%s ori:%c olt:%c ahg:%i bhg:%i qua:%f mno:%i mxo:%i pct:%i ",A_sequence_name,B_sequence_name,&ori,&olt,&ahg,&bhg,&qua,&mno,&mxo,&pct) == 10) {
	    	i++;
	    	rec = strtok(0,"}");
		} else {
	   		if(isValidClosing(rec)) { // all that's left is an envelope closing and whitespace
				int diff = &(rec[0]) - buf;
				final_buf = (char*) malloc((diff+1)*sizeof(char));
	    		if(!final_buf) {
					fprintf(stderr, "Cannot allocate memory to store the result sent back from the worker!\n");
	    			buf=NULL;
	    			free(obuf);
					return NULL;
				}
				final_buf[diff] = '\0';
				strncpy(final_buf, &(output[firstOVLRecord]),diff); // cut off starting envelope.
				buf=NULL;
				free(obuf);
				return final_buf;
	    	}
	    	debug(D_DEBUG,"Confirm Output Error. Buffer:\n=====\n%s\n=====\n",buf);
	    	if(sscanf(rec," {OVL afr:%s bfr:%s ", A_sequence_name,B_sequence_name) == 2)
				fprintf(stderr, "Unexpected output format for comparison of %s and %s. ", A_sequence_name,B_sequence_name);
	    	else
				fprintf(stderr, "Unexpected output format. ");
	    	buf=NULL;
	    	free(obuf);
	    	return NULL;
		}
    }
    //if we got here, we didn't have a valid closing.
    debug(D_DEBUG,"Unexpected output format parsing OVL envelope! Output:\n%s\n",obuf);
    buf=NULL;
    free(obuf);
    return NULL;
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
	    if(isAllWhitespace(rec)) { // all that's left is whitespace
		free(buf);
		return i;
	    }
	    debug(D_DEBUG,"Confirm Output Error. Buffer:\n=====\n%s\n=====\n",buf);
	    if(sscanf(rec," {OVL afr:%s bfr:%s ", A_sequence_name,B_sequence_name) == 2)
		fprintf(stderr, "Unexpected output format for comparison of %s and %s. ", A_sequence_name,B_sequence_name);
	    else
		fprintf(stderr, "Unexpected output format. ");
	    free(buf);
	    return 0;
	}
    }
    free(buf);
    return i;
}    

static struct hash_table* build_sequence_library(const char* filename)
{
	int num_items;
    static struct hash_table* h;
	char tmp[SEQUENCE_METADATA_MAX];
	char line[SEQUENCE_METADATA_MAX];
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
	while (fgets(line, SEQUENCE_FILE_LINE_MAX, infile))
	{
	if ((num_items = sscanf(line,">%s %i %i%[^\n]%*1[\n]",s.sequence_name,&s.num_bases,&s.num_bytes, tmp)) != 4)
	{
		fprintf(stderr, "Error reading sequence file. Only read %d items: %s", num_items, line);
		exit(1);
	}
	s.metadata = (unsigned char*) strdup(tmp+1);
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
	free(s.sequence_data);
	s.sequence_data = NULL; 
	fgetc(infile); // Ignore the newline at the end.
    }

    return h;
}


static int handle_done_task(struct work_queue_task *t) {

    char* final_output;
    if(!t)
	return 0;
    
    if(t->return_status==0) {
	final_output = removeEnvelope(t->output); // NULL on error, empty on blank, otherwise OVL records remaining.
	if( final_output != NULL ) { // successfully removed envelope. valid so far.
	    if(!strcmp(final_output,"") || confirm_output(final_output)) { // if we have a blank task or if we successfully confirm the output
		debug(D_DEBUG,"Completed task!\n");
		fputs(final_output,logfile);
		fputs("\n",logfile);	// make sure the ovl record from the next done task starts at a new line
		fflush(logfile);
		tasks_done++;
		tasks_runtime+=(t->finish_time-t->start_time);
		tasks_filetime+=t->total_transfer_time;
		free(final_output); // free final_output because it is what we strduped in removeEnvelope
	    } // drop to end to delete task.
	    else { // error in output, free final_output because it is what we strduped in removeEnvelope
		fprintf(stderr,"Failure of task on host %s. Output not confirmed:\n%s\n",t->host,t->output);
		free(final_output);
		return 0;
	    }
	}
	else { // if(output_status == NULL) --> error in output envelope, don't free final_output because it has already been freed above.
	    fprintf(stderr,"Failure of task on host %s. Output not confirmed, bad OVL record envelope:\n%s\n",t->host,t->output);
	    return 0;
	} 
    } else {
	fprintf(stderr,"Function failed with results %d: %s\non host: %s\n",t->result, t->output,t->host);
	return 0;
    }
    work_queue_task_delete(t);
    return 1;
}

static int task_consider( void* taskfiledata, int size )
{
	char cmd[2*MAX_FILENAME+4];
	char job_filename[10];
	string_cookie( job_filename, 10 );
	sprintf(cmd,"./%s %s %s",function,function_args,job_filename);

	struct work_queue_task* t;
	

	while(!work_queue_hungry(queue)) {
		if (last_display_time < time(0)) display_progress(queue);
	    handle_done_task(work_queue_wait(queue, 5));
	}

	t = work_queue_task_create(cmd);
	work_queue_task_specify_input_file( t, function, function);
	work_queue_task_specify_input_buf( t, taskfiledata, size, job_filename);
	work_queue_submit(queue,t);
	//fprintf(stderr,"Task command:\"%s\"\n",cmd);
	//work_queue_task_delete(t);
	global_count++;

	return 1;
}

static int get_next_cand_line(FILE * fp, char * sequence_name1, char * sequence_name2, int * alignment_flag, char * extra_data)
{
	char line[CAND_FILE_LINE_MAX];
	char * result;
	unsigned long start_of_line;
	int length;

	// Make sure all the buffers are cleared from the previous file writing.
	//fflush(fp);
	start_of_line = ftell(fp);

	// Get the next line.
	result = fgets(line, CAND_FILE_LINE_MAX, fp);

	// If we're at the end of the file and we're not waiting for
	// a special character, then just say we're done.
	if ((result == 0) && (end_char == '\0'))
		return GET_CAND_LINE_RESULT_EOF;

	// If it returns 0, it's because it didn't read anything (it's at the current end of the file).
	// Don't return EOF here, because we're assuming that more data could be read to the file in the
	// future, so we tell it to wait.
	if (result == 0)
		return GET_CAND_LINE_RESULT_WAIT;

	// If we only got a partial line, then we need to wait until the whole
	// line is printed, so go back to the beginning of the line and wait.
	length = strlen(line);
	if (line[length-1] != '\n')
	{
		fseek(fp, start_of_line, SEEK_SET);
		return GET_CAND_LINE_RESULT_WAIT;
	}

	// When the file is truly done, it will have an EOF character delimiter, determined by
	// the -f option, stored in end_char.
	if (*line == end_char)
		return GET_CAND_LINE_RESULT_EOF;

	// Get the actual data out of the line.
	int resulta = sscanf(line, "%s %s %i%[^\n]%*1[\n]", sequence_name1, sequence_name2, alignment_flag, extra_data);
	if ((resulta != 4) && ( !( (resulta == 3) && !strcmp(extra_data, ""))))
	{
		fprintf(stderr, "Bad line: %s, alignment_flag: %d, extra_data: |%s|, %d, resulta = %d\n", line, *alignment_flag, extra_data, *extra_data, resulta);
		return GET_CAND_LINE_RESULT_BAD_FORMAT;
	}

	// If we get this far, we were successful.
	return GET_CAND_LINE_RESULT_SUCCESS;
}

// We could just sleep, but it would be better to use this free
// time to handle done tasks.
static void wait_for_cands(struct work_queue * queue, int wait_time)
{
	struct work_queue_task * t;

	// Handle done tasks for up to wait_time seconds. If there are tasks
	// done, then work_queue_wait will return immediately and we will handle
	// them.  If all the tasks get handled, then we will wait for however many
	// seconds we are left in the "waiting" time.  After this time, the loop
	// will exit and the program can check if more candidates have arrived.
	time_t give_up_time = time(0)+wait_time;
	while (time(0) <= give_up_time)
	{
		if (last_display_time < time(0) ) display_progress(queue);
		t = work_queue_wait(queue, give_up_time - time(0));
		if (t)
			handle_done_task(t);
	}

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

	int get_line_result;

    int res;
    FILE* fp = fopen(candidate_filename,"r");
    if(!fp) {
		fprintf(stderr,"Couldn't open file %s.\n",candidate_filename);
		exit(1);
    }
	
	const unsigned int max_one_candidate_pair_size = (SEQUENCE_ID_MAX+ASSEMBLY_LINE_MAX+3)*4;
	unsigned int current_buffer_size = 4*1024*1024; // 4MB
	unsigned int used_buffer_size;

    char* buf = NULL;
    char* ins = NULL;
    while(!buf) {
	    buf = (char*) malloc(current_buffer_size*sizeof(char));
	    if(!buf) {
			fprintf(stderr,"Out of memory for buf! Waiting for a bit.\n");
			if(last_display_time < time(0)) 
				display_progress(queue);
			handle_done_task(work_queue_wait(queue,WORK_QUEUE_WAITFORTASK));
	    }
    }
    ins = buf;
	char extra_data[ALIGNMENT_METADATA_MAX] = "";

	// try to add the first sequence alignment pair
    while(pair_count == 0) {
	if ((get_line_result = get_next_cand_line(fp, sequence_name1, sequence_name2, &alignment_flag, extra_data)) == GET_CAND_LINE_RESULT_SUCCESS)
	{	// got a "seq_A seq_B ..." record
	    sprintf(tmp,"%s-%s",sequence_name1,sequence_name2);
	    if(!hash_table_lookup(t,tmp)) {
			s1 = (struct sequence*) hash_table_lookup(h, sequence_name1);
			s2 = (struct sequence*) hash_table_lookup(h, sequence_name2);
			res = sprintf(ins,">%s %i %i\n",s1->sequence_name,s1->num_bases,s1->num_bytes);
			ins+=res;
			memcpy(ins,s1->sequence_data,s1->num_bytes);
			ins+=s1->num_bytes;
			res = sprintf(ins,"\n>%s %i %i %i%s\n",s2->sequence_name,s2->num_bases,s2->num_bytes,alignment_flag, extra_data);
			ins+=res;
			memcpy(ins,s2->sequence_data,s2->num_bytes);
			ins+=s2->num_bytes;
		
			pair_count++;

			// If buffer is almost used up, realloc more memory for it
			used_buffer_size = ins - buf;
			while(current_buffer_size - used_buffer_size < max_one_candidate_pair_size) {
				unsigned int new_buffer_size = current_buffer_size * 1.5;
				buf = (char*)realloc(buf, new_buffer_size*sizeof(char));
				if(buf) {
					current_buffer_size = new_buffer_size;
					ins = buf + used_buffer_size;
					break;
				} else {
					fprintf(stderr,"Out of memory for buf! Waiting for a bit.\n");
					if(last_display_time < time(0)) 
						display_progress(queue);
					handle_done_task(work_queue_wait(queue,WORK_QUEUE_WAITFORTASK));
				}
			}

			
	    } else {
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
	
	} else { // did not get a "seq_A seq_B ..." record
		if (get_line_result == GET_CAND_LINE_RESULT_EOF)
		{
			fprintf(stderr, "All candidate pairs in %s are already complete in provided output!\n", candidate_filename);
			exit(0);
		}
		else if (get_line_result == GET_CAND_LINE_RESULT_BAD_FORMAT)
		{
			fprintf(stderr,"Badly formatted candidate file %s.\n",candidate_filename);
			exit(1);
		}
		else if (get_line_result == GET_CAND_LINE_RESULT_WAIT)
		{
			debug(D_DEBUG, "No candidates found, waiting (1).\n");
			wait_for_cands(queue, 5);
		}
		else
		{
			fprintf(stderr, "get_next_cand_line returned invalid result %d\n", get_line_result);
			exit(1);
		}
	}// if
    }// while
    
	
	// Add more sequence alignment pairs
	while ((get_line_result = get_next_cand_line(fp, sequence_name1, sequence_name2, &alignment_flag, extra_data)) != GET_CAND_LINE_RESULT_EOF)
    {
	if(last_display_time < time(0)) display_progress(queue);
	if (get_line_result == GET_CAND_LINE_RESULT_SUCCESS)
	{ // got a "seq_A seq_B ..." record
		sprintf(tmp,"%s-%s",sequence_name1,sequence_name2);
		if(!hash_table_lookup(t,tmp)) {
	    	if(!strcmp(sequence_name1,s1->sequence_name) && pair_count < NUM_PAIRS_PER_FILE) { // same first sequence, not exceeded max pairs.
				s2 = (struct sequence*) hash_table_lookup(h, sequence_name2);
				if(!s2) {
			   		fprintf(stderr,"No such sequence: %s",sequence_name2);
		    		exit(1);
				}
				res = sprintf(ins,"\n>%s %i %i %i%s\n",s2->sequence_name,s2->num_bases,s2->num_bytes,alignment_flag, extra_data);
				ins+=res;
				memcpy(ins,s2->sequence_data,s2->num_bytes);
				ins+=s2->num_bytes;
		
				pair_count++;

				// If buffer is almost used up, realloc more memory for it
				used_buffer_size = ins - buf;
				while(current_buffer_size - used_buffer_size < max_one_candidate_pair_size) {
					unsigned int new_buffer_size = current_buffer_size * 1.5;
					buf = (char*)realloc(buf, new_buffer_size*sizeof(char));
					if(buf) {
						current_buffer_size = new_buffer_size;
						ins = buf + used_buffer_size;
						break;
					} else {
						fprintf(stderr,"Out of memory for buf! Waiting for a bit.\n");
						if(last_display_time < time(0)) 
							display_progress(queue);
						handle_done_task(work_queue_wait(queue,WORK_QUEUE_WAITFORTASK));
					}
				}

	    	} else {
				if(!(pair_count < NUM_PAIRS_PER_FILE)){ // exceeded max pairs (may or may not be same first sequence, doesn't matter)
		   		 	//printf("Count exceeded so doing new file with %s,%s\n",sequence_name1,sequence_name2);
			   		task_consider(buf,ins-buf);
			    	pair_count = 0;
			    	buf[0]='\0';
		    		ins = buf;
				} else { //different first sequence, add separator ">>"
		    		sprintf(ins,"\n>>\n");
			    	ins+=strlen(ins);
				}

				//printf("PC:%i\n",pair_count);
				s1 = (struct sequence*) hash_table_lookup(h, sequence_name1);
				if(!s1) {
			    	fprintf(stderr,"No such sequence: %s",sequence_name1);
			   		exit(1);
				}
				s2 = (struct sequence*) hash_table_lookup(h, sequence_name2);
				if(!s2) {
		    		fprintf(stderr,"No such sequence: %s",sequence_name2);
			    	exit(1);
				}
				//printf("@%i:>%s %i %i\n",(int)(ins-buf),s1->sequence_name,s1->num_bases,s1->num_bytes);
				res = sprintf(ins,">%s %i %i\n",s1->sequence_name,s1->num_bases,s1->num_bytes);
				ins+=res;
				memcpy(ins,s1->sequence_data,s1->num_bytes);
				ins+=s1->num_bytes;
				res = sprintf(ins,"\n>%s %i %i %i%s\n",s2->sequence_name,s2->num_bases,s2->num_bytes,alignment_flag, extra_data);
				ins+=res;
				memcpy(ins,s2->sequence_data,s2->num_bytes);
				ins+=s2->num_bytes;

				pair_count++;

				// If buffer is almost used up, realloc more memory for it
				used_buffer_size = ins - buf;
				while(current_buffer_size - used_buffer_size < max_one_candidate_pair_size) {
					unsigned int new_buffer_size = current_buffer_size * 1.5;
					buf = (char*)realloc(buf, new_buffer_size*sizeof(char));
					if(buf) {
						current_buffer_size = new_buffer_size;
						ins = buf + used_buffer_size;
						break;
					} else {
						fprintf(stderr,"Out of memory for buf! Waiting for a bit.\n");
						if(last_display_time < time(0)) 
							display_progress(queue);
						handle_done_task(work_queue_wait(queue,WORK_QUEUE_WAITFORTASK));
					}
				}
			}
		} else {// if(!hash_table_lookup(t,tmp))
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
		} // else
	} else if (get_line_result == GET_CAND_LINE_RESULT_BAD_FORMAT) {// if (get_line_result == GET_CAND_LINE_RESULT_SUCCESS)
		fprintf(stderr, "Badly formatted candidate file %s:\n", candidate_filename);
		exit(1);
	} else if (get_line_result == GET_CAND_LINE_RESULT_WAIT) {
		debug(D_DEBUG, "No candidates found, waiting (2).\n");
		wait_for_cands(queue, 5);
	} else {
		fprintf(stderr, "get_next_cand_line returned invalid result %d\n", get_line_result);
		exit(1);
	}
    } // while

    if(buf[0] != '\0') {
		task_consider(buf,ins-buf);
    }
    
    free(buf);

    return 0;
}

int main( int argc, char *argv[] )
{
	char c;

	const char *progname = "sand_align_master";
	
	debug_config(progname);

	int task_size_specified = 0;

	strcpy(function_args, " ");
	
	while((c=getopt(argc,argv,"e:p:n:Pd:o:f:vh"))!=(char)-1) {
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
		case 'e':
			strcat(function_args, optarg);
			strcat(function_args, " ");
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'f':
			end_char = optarg[0];
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

	queue = work_queue_create(port);
	if(!queue) {
	    fprintf(stderr,"Couldn't create queue on port %i.\n",port);
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
		if(last_display_time < time(0)) display_progress(queue);
		t = work_queue_wait(queue,WORK_QUEUE_WAITFORTASK);
		if((!handle_done_task(t)) && (work_queue_empty(queue))) {
		    break;		
		}
	}

	printf("%7s | %4s %4s %4s | %6s %4s %4s %4s | %6s %6s %6s %8s | %s\n","Time","WI","WR","WB","TS","TW","TR","TC","TD","AR","AF","WS","Speedup");
	display_progress(queue);
	work_queue_delete(queue);
	printf("Completed %i tasks in %i seconds\n",tasks_done,(int)(time(0)-start_time));
	return 0;
}


