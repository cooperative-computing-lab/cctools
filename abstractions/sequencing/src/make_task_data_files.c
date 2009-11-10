
/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "assembly_master.h"

static const char *candidate_file;
static const char *sequence_data_file;
static const char *outdir;
static int priority_mode = 0;

static time_t start_time = 0;
static time_t last_display_time = 0;



int global_count = 0;
int fast_fill = 50; // how many tasks to fast-submit on start.

static int NUM_PAIRS_PER_FILE;
static int LIMIT;

static char end_char = '\0';
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
	printf("Use: %s [options] <candidate pairs file> <sequences file> <outputdir>\n",cmd);
	printf("where options are:\n");
	printf(" -n <number>    Maximum number of candidates per task.\n");
	printf(" -d <subsystem> Enable debugging for this subsystem.  (Try -d all to start.)\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
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
    //while(fscanf(infile," >%s %i %i%*1[ ]%*1[\n]",s.sequence_name,&s.num_bases,&s.num_bytes) == 3) {
    //while((num_items = fscanf(infile,">%s %i %i%[^\n]%*1[\n]",s.sequence_name,&s.num_bases,&s.num_bytes, tmp)) == 4) {
	while (fgets(line, 2048, infile))
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
	//printf("%s Added %i bytes from %c(%i) to %c(%i)\n",s.sequence_name,verification->num_bytes,verification->sequence_data[0],(int)verification->sequence_data[0],verification->sequence_data[verification->num_bytes - 1],(int)verification->sequence_data[verification->num_bytes - 1]);
	free(s.sequence_data);
	s.sequence_data = NULL; 
	fgetc(infile); // Ignore the newline at the end.
    }

    return h;
}



static int write_task_data_file( void* taskfiledata, int size )
{
    FILE* f;
    char filename[ASSEMBLY_LINE_MAX];
    sprintf(filename,"%s/%i",outdir,global_count++);
    f = fopen(filename,"w");
    if(f) {
	fwrite(taskfiledata, 1, size, f);
	fclose(f);
    }
    else {
	fprintf(stderr,"Couldn't open file %s!\n",filename);
	exit(1);
    }
    
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
	//if (sscanf(line, "%s %s %i %i %i", sequence_name1, sequence_name2, alignment_flag, start_pos1, start_pos2) != 5)
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
static void wait_for_cands(int wait_time)
{
    fprintf(stderr,"Have to wait for cands? What?\n");
    exit(1);
}

static int build_jobs(const char* candidate_filename, struct hash_table* h)
{

    char tmp[(2*SEQUENCE_ID_MAX)+2];
    char sequence_name1[SEQUENCE_ID_MAX+1];
    char sequence_name2[SEQUENCE_ID_MAX+1];
    
    struct sequence* s1;
    struct sequence* s2;

    int alignment_flag;

    int pair_count = 0;

	int get_line_result;

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
    }
    ins = buf;
	char extra_data[128] = "";
    while(pair_count == 0) {
	//if(fscanf(fp,"%s %s %i",sequence_name1,sequence_name2, &alignment_flag ) == 3) {
	//if ((get_line_result = get_next_cand_line(fp, sequence_name1, sequence_name2, &alignment_flag, &start_pos_1, &start_pos_2)) == GET_CAND_LINE_RESULT_SUCCESS)
	if ((get_line_result = get_next_cand_line(fp, sequence_name1, sequence_name2, &alignment_flag, extra_data)) == GET_CAND_LINE_RESULT_SUCCESS)
	{
	    sprintf(tmp,"%s-%s",sequence_name1,sequence_name2);
	    
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
	}
	else {
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
			wait_for_cands(5);
		}
		else
		{
			fprintf(stderr, "get_next_cand_line returned invalid result %d\n", get_line_result);
			exit(1);
		}
		/*
	    if(!feof(fp)) {
		fprintf(stderr,"Badly formatted candidate file %s.\n",candidate_filename);
		exit(1);
	    }
	    else {
		fprintf(stderr,"All candidate pairs in %s are already complete in provided output!\n",candidate_filename);
		exit(0);
	    }
		*/
	}
    }
    
    //while(fscanf(fp,"%s %s %i",sequence_name1,sequence_name2, &alignment_flag) == 3)
	//while ((get_line_result = get_next_cand_line(fp, sequence_name1, sequence_name2, &alignment_flag, &start_pos_1, &start_pos_2)) != GET_CAND_LINE_RESULT_EOF)
	while ((get_line_result = get_next_cand_line(fp, sequence_name1, sequence_name2, &alignment_flag, extra_data)) != GET_CAND_LINE_RESULT_EOF)
    {
	if (get_line_result == GET_CAND_LINE_RESULT_SUCCESS)
	{
	sprintf(tmp,"%s-%s",sequence_name1,sequence_name2);
	
	    if(!strcmp(sequence_name1,s1->sequence_name) && pair_count < NUM_PAIRS_PER_FILE) { // same first sequence, not exceeded max pairs.
			s2 = (struct sequence*) hash_table_lookup(h, sequence_name2);
			if(!s2)
			{
			    fprintf(stderr,"No such sequence: %s",sequence_name2);
		    	exit(1);
			}
			res = sprintf(ins,"\n>%s %i %i %i%s\n",s2->sequence_name,s2->num_bases,s2->num_bytes,alignment_flag, extra_data);
			ins+=res;
			memcpy(ins,s2->sequence_data,s2->num_bytes);
			ins+=s2->num_bytes;

			pair_count++;
	    }
	    else {
			if(!(pair_count < NUM_PAIRS_PER_FILE)){ // exceeded max pairs (may or may not be same first sequence, doesn't matter)
		    	//printf("Count exceeded so doing new file with %s,%s\n",sequence_name1,sequence_name2);
			    //task_consider(buf,ins-buf);
			     write_task_data_file(buf,ins-buf);

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
			res = sprintf(ins,"\n>%s %i %i %i%s\n",s2->sequence_name,s2->num_bases,s2->num_bytes,alignment_flag, extra_data);
			ins+=res;
			memcpy(ins,s2->sequence_data,s2->num_bytes);
			ins+=s2->num_bytes;

			pair_count++;
		}
	} // if (get_line_result == GET_CAND_LINE_RESULT_SUCCESS)
	else if (get_line_result == GET_CAND_LINE_RESULT_BAD_FORMAT)
	{
		fprintf(stderr, "Badly formatted candidate file %s:\n", candidate_filename);
		exit(1);
	}
	else if (get_line_result == GET_CAND_LINE_RESULT_WAIT)
	{
		debug(D_DEBUG, "No candidates found, waiting (2).\n");
		wait_for_cands(5);
	}
	else
	{
		fprintf(stderr, "get_next_cand_line returned invalid result %d\n", get_line_result);
		exit(1);
	}
    } // while
    if(buf[0] != '\0') {
	 write_task_data_file(buf,ins-buf);
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
	
	while((c=getopt(argc,argv,"n:Pd:o:vh"))!=(char)-1) {
		switch(c) {
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
		

	if( (argc-optind)!=3 ) {
		show_help(progname);
		exit(1);
	}

	candidate_file=argv[optind];
	sequence_data_file=argv[optind+1];
	outdir=argv[optind+2];


	struct rlimit rl;

	/* Obtain the current limits. */
	getrlimit (RLIMIT_AS, &rl);
	/* Set a CPU limit of 1 second. */
	//rl.rlim_cur = 1000000000;
	//rl.rlim_max = 3000000000;
	//setrlimit (RLIMIT_AS, &rl);
			
			
	start_time = time(0);
	last_display_time = 0;


	printf("Building sequence library\n");
	time_t temp_time = time(0);
	struct hash_table* mh = build_sequence_library(sequence_data_file);
	printf("Time to build library (%i sequences): %6ds\n", hash_table_size(mh),(int)(time(0)-temp_time));
	
	printf("Building task files\n");
	build_jobs(candidate_file,mh);
	printf("Wrote files for %i tasks in %i seconds\n",global_count,(int)(time(0)-start_time));
	return 0;
}


