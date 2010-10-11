/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <time.h>
#include <sys/types.h>
#include <pthread.h>

#include "allpairs_compare.h"

#include "debug.h"
#include "stringtools.h"
#include "xmalloc.h"
#include "fast_popen.h"
#include "text_list.h"
#include "memory_info.h"
#include "load_average.h"
#include "macros.h"
#include "full_io.h"

const char *progname = "allpairs_multicore";
static int block_size = 0;
static int num_cores = 0;

static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	printf("Usage: %s [options] <set A> <set B> <compare function>\n", cmd);
	printf("where options are:\n");
	printf(" -b <integer>	Block size: number of items to hold in memory at once. (default: 50%% of RAM\n");
	printf(" -c <integer>	Number of cores to be used. (default: # of cores in machine)\n");
	printf(" -d <flag>	Enable debugging for this subsystem.\n");
	printf(" -v         	Show program version.\n");
	printf(" -h         	Display this message.\n");
}

static int get_file_size( const char *path )
{
	struct stat info;
	if(stat(path,&info)==0) {
		return info.st_size;
	} else {
		return 0;
	}
}

/*
block_size_estimate computes how many items we can effectively
get in memory at once by measuring the first 100 elements of the set,
and then choosing a number to fit within 1/2 of the available RAM.
*/

int block_size_estimate( struct text_list *seta )
{
	int count = MIN(100,text_list_size(seta));
	int i;
	UINT64_T total_data,free_mem,total_mem;
	int block_size;

	memory_info_get(&free_mem, &total_mem);

	for(i=0;i<count;i++) {
		total_data += get_file_size(text_list_get(seta,i));
	}

	total_mem = total_mem/2;
	
	if(total_data>=total_mem) {
		block_size = text_list_size(seta) * total_mem / total_data;
		if(block_size<1) block_size = 1;
		if(block_size>text_list_size(seta)) block_size = text_list_size(seta);
	} else {
		block_size = text_list_size(seta);
	}

	return block_size;
}

/*
Load the named file into memory, returning the actual data of
the file, and filling length with the length of the buffer in bytes.
The result should be free()d when done.
*/

char * load_one_file( const char *filename, int *length )
{
	FILE *file = fopen(filename,"r");
	if(!file) {
		fprintf(stderr,"%s: couldn't open %s: %s\n",progname,filename,strerror(errno));
		exit(1);
	}

	fseek(file,0,SEEK_END);
	*length = ftell(file);
	fseek(file,0,SEEK_SET);

	char *data = malloc(*length);
	if(!data) {
		fprintf(stderr,"%s: out of memory!\n",progname);
		exit(1);
	}

	full_fread(file,data,*length);
	fclose(file);

	return data;
}

/*
pthreads requires that we pass all arguments through as a single
pointer, so we are forced to use a little structure to send all
of the desired arguments.
*/

struct thread_args {
	allpairs_compare_t func;
	char **xname;
	char **xdata;
	int   *xdata_length;
	char  *yname;
	char  *ydata;
	int    ydata_length;
};

/*
A single thread will loop over a whole block-row of the results,
calling the comparison function once for each pair of items.
*/

static void * row_loop_threaded( void *args )
{
	int i;
	struct thread_args *targs = args;
	for(i=0;i<block_size;i++) {
		targs->func(targs->xname[i],targs->xdata[i],targs->xdata_length[i],targs->yname,targs->ydata,targs->ydata_length);
	}
	return 0;
}

/*
The threaded main loop loads an entire block of objects into memory,
then forks threads, one for each row in the block, until done.
This only applies to functions loaded via dynamic linking.
Up to num_cores threads will be running simultaneously.
*/

static int main_loop_threaded( allpairs_compare_t funcptr, struct text_list *seta, struct text_list *setb )
{
	int x,i,j,c;

	char *xname[block_size];
	char *xdata[block_size];
	int xdata_length[block_size];

	char *yname[num_cores];
	char *ydata[num_cores];
	int ydata_length[num_cores];

	struct thread_args args[num_cores];
	pthread_t thread[num_cores];

	/* for each block sized vertical stripe... */
	for(x=0;x<text_list_size(seta);x+=block_size) {

		/* load the horizontal members of the stripe */
		for(i=0;i<block_size;i++) {
			xname[i] = text_list_get(seta,x+i);
			xdata[i] = load_one_file(text_list_get(seta,x+i),&xdata_length[i]);
		}

		/* for each row in the stripe ... */
		for(j=0;j<text_list_size(setb);j+=num_cores) {

			/* don't start more threads than rows remaining. */
			int n = MIN(num_cores,text_list_size(setb)-j);

			/* start one thread working on a whole row. */
			for(c=0;c<n;c++) {
				yname[c] = text_list_get(setb,j+c);
				ydata[c] = load_one_file(text_list_get(setb,j+c),&ydata_length[c]);

				args[c].func         = funcptr;
				args[c].xname        = xname;
				args[c].xdata        = xdata;
				args[c].xdata_length = xdata_length;
				args[c].yname	     = yname[c];
				args[c].ydata        = ydata[c];
				args[c].ydata_length = ydata_length[c];
				pthread_create(&thread[c],0,row_loop_threaded,&args[c]);
			}

			/* wait for each one to finish */
			for(c=0;c<n;c++) {
				pthread_join(thread[c],0);
				free(ydata[c]);
			}
		}

		for(i=0;i<block_size;i++) {
			free(xdata[i]);
		}
	}

	return 0;
}

/*
The program-oriented main loop iterates over the result matrix,
forking a comparison function for each result.  Up to num_cores
programes will be running simultaneously.
*/

static int main_loop_program( const char *funcpath, struct text_list *seta, struct text_list *setb )
{
	int x,i,j,c;
	char line[1024];
	FILE *proc[num_cores];

	/* for each block sized vertical stripe... */
	for(x=0;x<text_list_size(seta);x+=block_size) {

		/* for each row in the stripe ... */
		for(j=0;j<text_list_size(setb);j++) {

			/* for each group of num_cores in the stripe... */
			for(i=x;i<(x+block_size);i+=num_cores) {

				int n = MIN(num_cores,block_size-i);

				/* start one process for each core */
				for(c=0;c<n;c++) {
					sprintf(line,"%s %s %s\n",funcpath,text_list_get(seta,i+c),text_list_get(setb,j));
					proc[c] = fast_popen(line);
					if(!proc[c]) {
						fprintf(stderr,"%s: couldn't execute %s: %s\n",progname,line,strerror(errno));
						return 1;
					}
				}

				/* then finish one process for each core */
				for(c=0;c<n;c++) {
					printf("%s\t%s\t",text_list_get(seta,i+c),text_list_get(setb,j));
					while(fgets(line,sizeof(line),proc[c])) {
						printf("%s",line);
					}
					printf("\n");
					fast_pclose(proc[c]);
				}
			}
		}
	}

	return 0;
}

int main(int argc, char *argv[])
{
	char c;
	int result;

	debug_config(progname);

	while((c = getopt(argc, argv, "b:c:dvh"))!=-1) {
		switch (c) {
		case 'b':
			block_size = atoi(optarg);
			break;
		case 'c':
			num_cores = atoi(optarg);
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'v':
			show_version(progname);
			exit(0);
			break;
		default:
		case 'h':
			show_help(progname);
			exit(0);
			break;
		}
	}

	if((argc - optind) < 3) {
		show_help(progname);
		exit(1);
	}

	const char * setapath = argv[optind];
	const char * setbpath = argv[optind+1];
	const char * funcpath = argv[optind+2];

	struct text_list *seta = text_list_load(setapath);
	if(!seta) {
		fprintf(stderr, "allpairs_multicore: cannot open %s: %s\n",setapath,strerror(errno));
		exit(1);
	}

	struct text_list *setb = text_list_load(setbpath);
	if(!setb) {
		fprintf(stderr, "allpairs_multicore: cannot open %s: %s\n",setbpath,strerror(errno));
		exit(1);
	}

	if(num_cores==0) num_cores = load_average_get_cpus();
	debug(D_DEBUG,"num_cores: %d\n",num_cores);

	if(block_size==0) block_size = block_size_estimate(seta);
	debug(D_DEBUG,"block_size: %d elements",block_size);

	allpairs_compare_t funcptr = allpairs_compare_function_get(funcpath);
	if(funcptr) {
		result = main_loop_threaded(funcptr,seta,setb);
	} else {
		if(access(funcpath,X_OK)!=0) {
			fprintf(stderr, "%s: %s is neither an executable program nor an internal function.\n",progname,funcpath);
			return 1;
		}

		result = main_loop_program(funcpath,seta,setb);
	}

	return result;
}
