/*
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <dirent.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <pthread.h>

#include "chirp_reli.h"
#include "chirp_matrix.h"

#include "debug.h"
#include "auth_all.h"
#include "stringtools.h"
#include "xmalloc.h"
#include "fast_popen.h"

#include "memory_info.h"
#include "load_average.h"

#define CHIRP_STABLE_ELEMENTS_MAX 100000
#define CHIRP_MOVING_ELEMENTS_MAX 100000
#define CHIRP_PROCESSOR_MAX 64
#define MAX_FILENAME_LEN 512
#define MAX_FUNCNAME_LEN 128
#define MAX_THREAD_NUMBER 128
#define USING_INNER_FUNCTION 0
#define USING_OUTER_FUNCTION 1

int wq = 0;

int *thr_status;
int *thr_count_within_block;

double (*compare_two_files)(const void *mmap1, size_t size1, const void *mmap2, size_t size2) = NULL;

void get_absolute_path(char *, char *);
int resolve_block_size(const char *, const char *, int *, int *, int *, int *, int *, int *);
int get_element_size(const char *);
int file_line_count(const char *);

// Inner compare function for irises application
double compare_irises(const void *mmap1, size_t size1, const void *mmap2, size_t size2) {
    const char *data1, *data2;
    int i, j, count=0;
	int size,band,inner,outer,quality;
	const int power2[8] = { 1, 2, 4, 8, 16, 32, 64, 128 };

	data1 = (char *)mmap1;
	data2 = (char *)mmap2;
	
	// Start processing data in mmap1	
	sscanf(data1, "%d %d %d %d %d", &size,&band,&inner,&outer,&quality);
	data1 = strchr(data1, '\n');

	// Let data1 points to the start of code data
	data1++;
	
	if(size1 - (data1 - (char *)mmap1) != size/8*2) {
		fprintf(stderr, "allpairs_multicore: Image 1 data size error!\n");
		exit(1);
	}

	int code1[size];
	int mask1[size];

	count =0;
	for(i = 0; i < size / 8; i++) {
		for (j = 0; j < 8; j++)
		{
			if (data1[i] & power2[j]) {
				code1[count] = 1;
				count++;
			} else {
				code1[count] = 0;
				count++;
			}
		}
	}
	
	// Let data1 now points to the start of mask data
	data1 += i;
	
	count =0;
	for(i = 0; i < size / 8; i++) {
		for (j = 0; j < 8; j++) {
			if (data1[i] & power2[j]) {
				mask1[count] = 1;
				count++;
			} else {
				mask1[count] = 0;
				count++;
			}
		}
	}
	
	sscanf(data2, "%d %d %d %d %d", &size,&band,&inner,&outer,&quality);
	data2 = strchr(data2, '\n');

	// Let data2 points to the start of code data
	data2++;
	
	if(size2 - (data2 - (char *)mmap2) != size / 8 * 2) {
		fprintf(stderr, "allpairs_multicore: Image 2 data size error!\n");
		exit(1);
	}

	int code2[size];
	int mask2[size];	
	
	count =0;
	for(i = 0; i < size / 8; i++) {
		for (j = 0; j < 8; j++)
		{
			if (data2[i] & power2[j]) {
				code2[count] =1;
				count++;
			} else {
				code2[count] =0;
				count++;
			}
		}
	}
	
	// Let data1 now points to the start of mask data
	data2 += i;
	
	count =0;
	for(i = 0; i < size / 8; i++) {
		for (j = 0; j < 8; j++) {
			if (data2[i] & power2[j]) {
				mask2[count] =1;
				count++;
			} else {
				mask2[count] =0;
				count++;
			}
		}
	}

	int codes[size];
	int masks[size];
	int results[size];
	int distance=0;
	int total = 0;
	for(i=0;i<size;i++)
	{
		codes[i] = code1[i]^code2[i];
		masks[i] = mask1[i]&mask2[i];
		results[i] = codes[i]&masks[i];
		distance = distance + results[i];
		total = total + masks[i];
	}

	return (double)distance/total;
}

// Using mmap() in file I/O
double compare_bitwise(const void *mmap1, size_t size1, const void *mmap2, size_t size2){
    const char *data1, *data2;
    int i, count=0;
    unsigned int minsize;

	data1 = mmap1;
	data2 = mmap2;

    minsize = size1 - size2 ? size2 : size1;
    
	count = 0;
	for(i=0; i<minsize; i++) {
        if(data1[i] != data2[i]) {
			count++;
			break;
		}
	}

    return (double)count;
}

// Using mmap() in file I/O
double compare_bitdumb(const void *mmap1, size_t size1, const void *mmap2, size_t size2){
    const char *data1, *data2;
    int i, count=0;
    unsigned int minsize;
	long tmp;

	data1 = mmap1;
	data2 = mmap2;

    minsize = size1 - size2 ? size2 : size1;

	int j;
	count = 0;
	for(i=0; i<minsize; i++) {
		tmp = 0;
		for(j=0; j<2; j++) {
			tmp += ((long)(data1[i]))*((long)(data1[i])) + ((long)(data2[i]))*((long)(data2[i]));
			tmp = tmp % 3;
		}
        if(data1[i]==data2[i]) {
			count += (int)tmp + 1;
			count -= tmp;
		}
	}
	
    return (double)count;
}

// Compare function that takes O(n*n) time
double compare_nsquare(const void *mmap1, size_t size1, const void *mmap2, size_t size2) {
    const char *data1, *data2;
    int i, j, count=0;

	data1 = mmap1;
	data2 = mmap2;

	count = 0;
	for(i = 0; i < size1; i++)
		for(j = 0; j < size2; j++)
        	if(data1[i] == data2[j] && i == j) {
					count++;
			}

    return (double)count;
}

struct thr_func_arg {
	char filename1[CHIRP_PATH_MAX];
	char filename2[CHIRP_PATH_MAX];
	int thr_index;
};

pthread_mutex_t thr_mutex;
int maxThrCount = 0;
int maxProCount = 0;
int countInBlock = 0;
int rowInBlock;
int colInBlock;

char cmdrun[3*CHIRP_PATH_MAX];
int gnum = 0;
double *resbuff = NULL;

int numOfAvailableStableElements;
int numOfAvailableMovingElements;
int numOfCores;
char stableElements[CHIRP_STABLE_ELEMENTS_MAX][CHIRP_PATH_MAX];
char movingElements[CHIRP_MOVING_ELEMENTS_MAX][CHIRP_PATH_MAX];

char *filename1;
struct stat filestat1;
int movingFd;
int stableFd; 
char *movingData[CHIRP_MOVING_ELEMENTS_MAX];
char *stableData[CHIRP_STABLE_ELEMENTS_MAX];
size_t movingSize[CHIRP_MOVING_ELEMENTS_MAX];
size_t stableSize[CHIRP_STABLE_ELEMENTS_MAX];

void *thr_function_compare(void *arg) {
    int count=0;
	double rval = 0;
	int rowInBlock, colInBlock;
	
	while(1) {
		pthread_mutex_lock(&thr_mutex);
		count = countInBlock;
		countInBlock++;
		pthread_mutex_unlock(&thr_mutex);
	
		if(count >= maxThrCount) break;
	
		rowInBlock = (int)(count/numOfAvailableStableElements);
		colInBlock = count % numOfAvailableStableElements;
		
		rval = compare_two_files(stableData[colInBlock], stableSize[colInBlock], movingData[rowInBlock], movingSize[rowInBlock]);

		resbuff[gnum+count] = rval;
	}

	pthread_exit(NULL);
}


struct compare_function {
	char name[MAX_FUNCNAME_LEN];
	double (*pointer)(const void *mmap1, size_t size1, const void *mmap2, size_t size2);
	struct compare_function *next;
};

struct compare_function *compare_function_head = NULL;

void register_compare_function(const char *function_name, double (*compare_function_pointer)(const void *mmap1, size_t size1, const void *mmap2, size_t size2)) {
	struct compare_function *function;
		
	if(!compare_function_head) {
		compare_function_head = (struct compare_function *)malloc(sizeof(struct compare_function));
		if(!compare_function_head) {
			fprintf(stderr, "allpairs_multicore: Cannot initialize compare function list: %s\n", strerror(errno));
			exit(1);
		}

		compare_function_head->next = NULL;
	}

	function = (struct compare_function *)malloc(sizeof(struct compare_function));
	if(!function) {
		fprintf(stderr, "allpairs_multicore: Cannot initialize compare function - %s: %s\n", function_name, strerror(errno));
		exit(1);
	}

	strcpy(function->name, function_name);
	function->pointer = compare_function_pointer;
	function->next = compare_function_head->next;
	compare_function_head->next = function;
}
	
int set_compare_function(const char *function_name) {
	struct compare_function *p;
	
	if(!compare_function_head) return 0;
	p = compare_function_head;
	while((p = p->next) != NULL) {
		if(strcmp(p->name, function_name) == 0) {
			compare_two_files = p->pointer;
			break;
		}
	}

	if(!p) return 0;
	else return 1;
}


static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help( const char *cmd )
{
	printf("Usage: %s [options] <set A> <set B> <compare function>\n",cmd);
	printf("where options are:\n");
	printf(" -d <string>	Enable debugging for this subsystem.\n");
	printf(" -i <integer>  	x coordinate of the start point of computation in the matrix. \n");
	printf(" -j <integer>  	y coordinate of the start point of computation in the matrix. \n");
	printf(" -k <integer>  	x coordinate of the end point of computation in the matrix. \n");
	printf(" -l <integer>  	y coordinate of the end point of computation in the matrix. \n");
	printf(" -x <integer>	Block width.  (default is chosen according to hardware conditions)\n");
	printf(" -y <integer>	Block height. (default is chosen according to hardware conditions)\n");
	printf(" -X <integer> 	x coordinate of starting point in a distributed context.\n");
	printf(" -Y <integer>  	y coordinate of starting point in a distributed context.\n");
	printf(" -c <integer>	Number of cores to be used.\n");
	printf(" -v         	Show program version.\n");
	printf(" -h         	Display this message.\n");
}

int main(int argc, char *argv[]) {

    char c;

    int setAindex, setBindex, funcindex;
    FILE *setA = NULL;
    FILE *setB = NULL;
    char setApath[CHIRP_PATH_MAX];
    char setBpath[CHIRP_PATH_MAX];
    char *LIST_FILE_NAME = "set.list";
    char setAfilename[CHIRP_PATH_MAX];
    char setBfilename[CHIRP_PATH_MAX];
    int len = CHIRP_PATH_MAX;

    int i,j,k,l; 
    int numOfMovingElements, numOfStableElements;
    int setACount, setBCount;
    int setAPos, setBPos;
    long setBStartPos;
    int x1, y1, x2, y2, topLeftX, topLeftY;		// [x1,y1]-start position, [x2,y2]-end position, the sub matrix we compute in a round
    int x_rel, y_rel;
	
	FILE *tmpresult[CHIRP_PROCESSOR_MAX];
	
	int function_flag;
	int thread_err;
	void *tret;
	pthread_t tids[MAX_THREAD_NUMBER];
			
	
	int init_threads = 0;

    
    numOfMovingElements = -1;
    numOfStableElements = -1;
    numOfCores = -1;
			
    x1 = y1 = x2 = y2 = -1;

    topLeftX = topLeftY = 0;

    debug_config(argv[0]);
	
	/**
		To register an inner compare function, add code in the following format:
		register_compare_function("YOUR INNER FUNCTION NAME", YOUR INNER FUNCTION);
	*/
	register_compare_function("compare_bitdumb", compare_bitdumb);
	
    while((c=getopt(argc,argv,"d:vhx:y:i:j:k:l:X:Y:c:r"))!=(char)-1) {
			switch(c) {
				case 'd':
					debug_flags_set(optarg);
					break;
				case 'v':
					show_version(argv[0]);
					exit(0);
					break;
				case 'h':
					show_help(argv[0]);
					exit(0);
					break;
				case 'x':
					numOfStableElements = atoi(optarg);
					break;
				case 'y':
					numOfMovingElements = atoi(optarg);
					break;
				case 'i':
					x1 = atoi(optarg);
					break;
				case 'j':
					y1 = atoi(optarg);
					break;
				case 'k':
					x2 = atoi(optarg);
					break;
				case 'l':
					y2 = atoi(optarg);
					break;
				case 'X':
					topLeftX = atoi(optarg);
					break;
				case 'Y':
					topLeftY = atoi(optarg);
            		break;
        		case 'c':
            		numOfCores = atoi(optarg);
            		break;
        		case 'r':
            		wq = 1;
            		break;
	    
			}
    }

    if( (argc-optind)<3 ) {
		fprintf(stderr, "allpairs_multicore: after all options, you must have: setA setB function\n");
		exit(1);
    }
    
    setAindex=optind;
    setBindex=optind+1;
    funcindex=optind+2;
	
	// Set compare function: INNER or OUTER
	if(set_compare_function(argv[funcindex]) == 0) {
		// Cannot find a internal function for the specified function name, try external function
		function_flag = USING_OUTER_FUNCTION;
		if(access(argv[funcindex], X_OK) != 0) {
			fprintf(stderr, "allpairs_multicore: Cannot execute program - %s (no permission or program does not exist)! : %s\n", argv[funcindex], strerror(errno));
			exit(1);
		}
    	debug(D_DEBUG, "Using outer function.\n");
	} else {
		function_flag = USING_INNER_FUNCTION;
    	debug(D_DEBUG, "Using inner function.\n");
	}
	
    // Get absolute paths for data sets directories
	if(wq) {
		strcpy(setApath, "setA.");
		strcpy(setBpath, "setB.");
	} else {
    	get_absolute_path(setApath, argv[setAindex]);
		get_absolute_path(setBpath, argv[setBindex]);
	}
		
    // setA and setB each contains a list of file names that points to the data files
    char setAlistfile[CHIRP_PATH_MAX];
    char setBlistfile[CHIRP_PATH_MAX];
   
    strcpy(setAlistfile, setApath);
    strcat(setAlistfile, LIST_FILE_NAME);
    if((setA=fopen(setAlistfile, "r")) == NULL) {
        fprintf(stderr, "allpairs_multicore: Cannot open setA list file - %s! : %s\n", setAlistfile, strerror(errno));
        exit(1);
    }

    strcpy(setBlistfile, setBpath);
    strcat(setBlistfile, LIST_FILE_NAME);
    if((setB=fopen(setBlistfile, "r")) == NULL) {
        fprintf(stderr, "allpairs_multicore: Cannot open setB list file - %s! : %s\n", setBlistfile, strerror(errno));
        exit(1);
    }
	
    
    // Resolve block size and number of cores 
    if(numOfCores <= 0) {
        numOfCores = load_average_get_cpus();
    } else {
        numOfCores = numOfCores > load_average_get_cpus() ? load_average_get_cpus() : numOfCores;
    }
    resolve_block_size(setAlistfile, setBlistfile, &numOfStableElements, &numOfMovingElements, &x1, &y1, &x2, &y2);

	if(x1==-1 && x2==-1 && y1==-1 && y2==-1) {
		x1 = 0;
		y1 = 0;
    	x2 = file_line_count(setAlistfile);
    	y2 = file_line_count(setBlistfile);
	}


	// Stable and moving elements max are 100000!!!
    debug(D_DEBUG, "Number of Stalbe Elements: %d\n", numOfStableElements);
    debug(D_DEBUG, "Number of Moving Elements: %d\n", numOfMovingElements);
    debug(D_DEBUG, "Number of Cores: %d\n", numOfCores);
    debug(D_DEBUG, "Top left X: %d ; Top left Y: %d \n", topLeftX, topLeftY);
   
	// Multithread control initializing
	thr_status = (int *)malloc(numOfCores * sizeof(int));
	memset((void *)thr_status, 0, numOfCores);
	thr_count_within_block = (int *)malloc(numOfCores * sizeof(int));
	memset((void *)thr_count_within_block, 0, numOfCores);
	
    // Initialize position parameters and allocate memory for storing results of a block (sub matrix)
    x_rel = y_rel = 0; // relative to the sub-matrix we are actually working on
    resbuff = (double *) malloc(numOfMovingElements*numOfStableElements*sizeof(double));

    // Go forward until line x1 in Set A list file
    for(i = 0; i < x1 && !feof(setA); i++) {
        fgets(setAfilename, len, setA);
    }
    if(i < x1) {
        fprintf(stderr, "allpairs_multicore: Set A has less elements than specified in option 'p'!\n");
        exit(1);
    }

    // Go forward until line y1 in Set B list file
    for(i = 0; i < y1 && !feof(setB); i++) {
        fgets(setBfilename, len, setB);
    }
    if(i < y1) {
        fprintf(stderr, "allpairs_multicore: Set B has less elements than specified in option 'q'!\n");
        exit(1);
    }
    setBStartPos = ftell(setB);

    // start loop
    fgets(setAfilename, len, setA);
    if (setAfilename != NULL) {
	    size_t last = strlen (setAfilename) - 1;
	    if (setAfilename[last] == '\n') setAfilename[last] = '\0';
    }

    setAPos = x1;
    while(!feof(setA) && setAPos <= x2) { // Set A - row of matrix
        for(setACount = 0; !feof(setA) && setACount < numOfStableElements && setAPos <= x2; setACount++, setAPos++){
			// Get the rows within a block
            strcpy(stableElements[setACount], setApath);
            strcat(stableElements[setACount], setAfilename); 
            fgets(setAfilename, len, setA);
            if (setAfilename != NULL) {
	        	size_t last = strlen (setAfilename) - 1;	    
	        	if (setAfilename[last] == '\n') setAfilename[last] = '\0';
            } 
        }

        numOfAvailableStableElements = setACount;

		if(function_flag == USING_INNER_FUNCTION) {
			// Mmap all the stable elements
			for(i = 0; i < numOfAvailableStableElements; i++) {	
				filename1 = stableElements[i];
			
				if((stableFd = open(filename1, O_RDONLY)) == -1){
					fprintf(stderr, "allpairs_multicore: Cannot open file - %s for comparison. : %s\n", filename1, strerror(errno));
					exit(1);
				}


				if(stat(filename1, &filestat1) == -1){
					fprintf(stderr, "allpairs_multicore: Get file - %s status error. : %s\n", filename1, strerror(errno));
					exit(1);
				}

				stableSize[i] = filestat1.st_size;


				if((stableData[i] = mmap((caddr_t)0, filestat1.st_size, PROT_READ, MAP_SHARED, stableFd, 0)) == (caddr_t)(-1)) {
					fprintf(stderr, "allpairs_multicore: Attempt to mmap file - %s failed. : %s\n", filename1, strerror(errno));
					exit(1);
				}

				close(stableFd);
			}
		}

        // Go directly to line y1 in Set B list file
        fseek(setB, setBStartPos, SEEK_SET);

        fgets(setBfilename, len, setB);
        if (setBfilename != NULL) {
	    	size_t last = strlen (setBfilename) - 1;	    
	    	if (setBfilename[last] == '\n') setBfilename[last] = '\0';
        }

        setBPos = y1;
        while(!feof(setB) && setBPos <= y2) { // Set B - column of matrix
            for(setBCount = 0; !feof(setB) && setBCount < numOfMovingElements && setBPos <= y2; setBCount++, setBPos++) {
            	// Get the columns within a block	
				strcpy(movingElements[setBCount], setBpath);
            	strcat(movingElements[setBCount], setBfilename); 
					
            	fgets(setBfilename, len, setB);
	        	if(setBfilename != NULL) {
		    		size_t last = strlen (setBfilename) - 1;    
		    		if(setBfilename[last] == '\n') setBfilename[last] = '\0';
	        	}
            }
            numOfAvailableMovingElements = setBCount;
			
			// Multiprocess routine
			if(function_flag == USING_OUTER_FUNCTION) {
				// Multiprocess control flags
				countInBlock = 0;
				maxProCount = setBCount * numOfAvailableStableElements;
				k = 0;
				for(i = 0; i < numOfAvailableStableElements; i++) {
					for(j = 0; j < setBCount; j++, k++) {
						if(k == numOfCores) {
							for(l = 0; l < k; l++) {
								fscanf(tmpresult[l], "%lf", &(resbuff[countInBlock]));
								debug(D_DEBUG, "cell index: [%d, %d] value: %lf\n", x1+setAPos-setACount+(countInBlock%numOfAvailableStableElements), y1+setBPos-setBCount+(int)(countInBlock/numOfAvailableStableElements) , resbuff[gnum+countInBlock]);
								fast_pclose(tmpresult[l]);
								countInBlock++;
							}
							k = -1;
							j--;
							continue;
						}
						rowInBlock = (int)((countInBlock+k)/numOfAvailableStableElements);
						colInBlock = (countInBlock+k) % numOfAvailableStableElements;
		
						strcpy(cmdrun,"");
						strcat(cmdrun, argv[funcindex]);
						strcat(cmdrun, " ");
						strcat(cmdrun, stableElements[colInBlock]);
						strcat(cmdrun, " ");
						strcat(cmdrun, movingElements[rowInBlock]);
								
						debug(D_DEBUG, "Starting command - %s ...\n", cmdrun);
						if((tmpresult[k] = fast_popen(cmdrun)) == NULL){
							fprintf(stderr, "allpairs_multicore: Executing user specified program failed. : %s\n", strerror(errno));
							exit(1);
						}
						
					}
				}	
				for(l = 0; l < k; l++) {
					fscanf(tmpresult[l], "%lf", &(resbuff[countInBlock]));
					debug(D_DEBUG, "cell index: [%d, %d] value: %lf\n", x1+setAPos-setACount+(countInBlock%numOfAvailableStableElements), y1+setBPos-setBCount+(int)(countInBlock/numOfAvailableStableElements) , resbuff[gnum+countInBlock]);
					fast_pclose(tmpresult[l]);
					countInBlock++;
				}
			} else {// Multithread routine
					
				// Mmap all the current moving elements into main memory
				for(i = 0; i < numOfAvailableMovingElements; i++) {	
					filename1 = movingElements[i];
					
					if((movingFd = open(filename1, O_RDONLY)) == -1){
						fprintf(stderr, "allpairs_multicore: Cannot open file - %s for comparison. : %s\n", filename1, strerror(errno));
						exit(1);
					}


					if(stat(filename1, &filestat1) == -1){
						fprintf(stderr, "allpairs_multicore: Get file - %s status error. : %s\n", filename1, strerror(errno));
						exit(1);
					}
					
					movingSize[i] = filestat1.st_size;


					if((movingData[i] = mmap((caddr_t)0, filestat1.st_size, PROT_READ, MAP_SHARED, movingFd, 0)) == (caddr_t)(-1)) {
						fprintf(stderr, "allpairs_multicore: Attempt to mmap file - %s failed. : %s\n", filename1, strerror(errno));
						exit(1);
					}

					close(movingFd);
				}

				// Multithread control flags
				countInBlock = 0;
				maxThrCount = numOfAvailableMovingElements * numOfAvailableStableElements;
				init_threads = numOfCores > maxThrCount ? maxThrCount : numOfCores;
					
				// Start couple of threads, each thread will grab a pair of files for comparison.
				// When one pair finishes, the thread will grab a new pair until no pair left in this 'block'
				for(j = 0; j < init_threads; j++) {
					thread_err = pthread_create(&(tids[j]), NULL, thr_function_compare, NULL);
					if(thread_err != 0) {
						fprintf(stderr, "allpairs_multicore: Cannot create thread for comparison. : %s\n", strerror(errno));
						exit(1);
					}
				}
				// Join the threads that have been started. After all have been joined, move on to the next 'block'
				for(j = 0; j < init_threads; j++) {
					thread_err = pthread_join(tids[j], &tret);
					if(thread_err != 0) {
						fprintf(stderr, "allpairs_multicore: Cannot join with thread. : %s\n", strerror(errno));
						exit(1);
					}
				}
				
				// Munmap data
				for(i = 1; i < numOfAvailableMovingElements; i++) {
					if(munmap(movingData[i], movingSize[i]) == -1)
						fprintf(stderr, "allpairs_multicore: Attempt to munmap file - %s failed. : %s\n", movingElements[i], strerror(errno));
				}
			}
			
			
			if(function_flag == USING_INNER_FUNCTION) {
				for(i = 1; i < numOfAvailableStableElements; i++) {
					if(munmap(stableData[i], stableSize[i]) == -1)
						fprintf(stderr, "allpairs_multicore: Attempt to munmap file - %s failed. : %s\n", stableElements[i], strerror(errno));
				}
			}
			
			// Write data to the remote matrix
            debug(D_DEBUG, "Output to matrix at (%d, %d), width:%d, height:%d.\n", topLeftX+x1+x_rel, topLeftY+y1+y_rel, numOfAvailableStableElements, numOfMovingElements);
			
			int x, y;
			x = topLeftX + x1 + x_rel;
			y = topLeftY + y1 + y_rel;
			for(i = 0; i < numOfAvailableMovingElements; i++)
				for(j = 0; j < numOfAvailableStableElements; j++)
					printf("%d\t%d\t%f\n", x+j, y+i, resbuff[i*numOfAvailableStableElements+j]);
	    	gnum = 0;
            y_rel += numOfAvailableMovingElements;
		}
        x_rel += numOfAvailableStableElements;
        y_rel = 0;		
    }
   
    free(resbuff);
   
    fclose(setA);
    fclose(setB);
    
    return 0;
}

void get_absolute_path(char *local_path, char *path) {
    char *p;

    p = strchr(path, '/');
    if(p == NULL || p != path) {
        getcwd(local_path, CHIRP_PATH_MAX);
        strcat(local_path, "/");
        strcat(local_path, path);
        if(local_path[strlen(local_path)-1] != '/') strcat(local_path, "/");
    } else {
		// Given path is an absolute path
		strcpy(local_path, path);
        if(local_path[strlen(local_path)-1] != '/') strcat(local_path, "/");
   }
}

int resolve_block_size(const char *file1, const char *file2, int *x, int *y, int *p, int *q, int *r, int *s) {
    UINT64_T free_mem, total_mem;
    int m, n;
	int x1, y1, x2, y2;
	int lineCount1, lineCount2;
    long numOfFileInCache;
    int element_size;
    FILE *fp;
    char setAElementPath[CHIRP_PATH_MAX];
    char setAElementFilename[CHIRP_PATH_MAX];
    char *last_slash;
	int coordsInvalid = 1;

	x1 = *p;
	y1 = *q;
	x2 = *r;
	y2 = *s;
	
    lineCount1 = file_line_count(file1);
    lineCount2 = file_line_count(file2);
	
	if(x1!=-1 && x2!=-1 && y1!=-1 && y2!=-1) {
		if(x1 >= 0 && x1 <= lineCount1 && x2 >= 0 && x2 <= lineCount1 && x2 > x1 && \
			y1 >= 0 && y1 <= lineCount2 && y2 >= 0 && y2 <= lineCount2 && y2 > y1) {
				lineCount1 = x2 - x1 + 1;
				lineCount2 = y2 - y1 + 1;
				coordsInvalid = 0;
		} 	//else {
				// optional coordinates error, compute the whole matrix
			//}
	} 	
	//else if (x1==-1 && x2 ==-1 && y1==-1 && y2==-1) {
		// no optional coordinates, compute the whole matrix
	//} else {
		// missing optional coordinates, compute the whole matrix
	//}
	
	if(coordsInvalid) {
		*p = 0;
		*q = 0;
		*r = lineCount1 - 1;
		*s = lineCount2 - 1;
	}
    debug(D_DEBUG, "Start point:\t[%d, %d]\n", *p, *q);
    debug(D_DEBUG, "End point:\t[%d, %d]\n", *r, *s);

    if(*x > 0 && *y > 0) {
        if(*x > lineCount1) *x = lineCount1;
        if(*y > lineCount2) *y = lineCount2;
        return 1;
    }
    memory_info_get(&free_mem, &total_mem);
    debug(D_DEBUG, "Free memory: %lld KB = %lld MB\n", free_mem>>10, free_mem>>20);

    // default block size: 10 * 10
    *x = 10;
    *y = 10;

    // get a single element's file name (relative)
    if((fp=fopen(file1,"r")) == NULL) return -1;   
    fgets(setAElementFilename, CHIRP_PATH_MAX, fp);
    if (setAElementFilename != NULL) {
        size_t last = strlen (setAElementFilename) - 1;
	if (setAElementFilename[last] == '\n') setAElementFilename[last] = '\0';
    } else {
        fclose(fp);
        return -1;
    }
    fclose(fp);
    
    // get element absolute path
	if(wq) {
		strcpy(setAElementPath, "setA.");
	} else {
   		strcpy(setAElementPath, file1);
    	last_slash = strrchr(setAElementPath, '/');
    	*(last_slash+1) = '\0';
	}
    strcat(setAElementPath, setAElementFilename);
    element_size = get_element_size(setAElementPath);
    debug(D_DEBUG, "Estimate element size using file - %s\n", setAElementPath);
    debug(D_DEBUG, "Element size: %d Bytes = %d KB = %d MB\n", element_size, element_size>>10, element_size>>20);

    if(free_mem == -1 || element_size == -1) return -1;

    numOfFileInCache = ((free_mem) / element_size) / 2;

    m = n = (int)(numOfFileInCache / 2);
    
    if(m > lineCount1 && n > lineCount2) {
        m = lineCount1;
        n = lineCount2;
    } else if(m > lineCount1) {
        n += m - lineCount1;
        m = lineCount1;
        if(n > lineCount2) n = lineCount2;
    } else if(n > lineCount2) {
        m += n - lineCount2;
        n = lineCount2;    
        if(m > lineCount1) m = lineCount1;
    } else {
		//m = lineCount1;
		//n = lineCount2;
    }
 
    *x = m;
    *y = n; 
    return 1;
}

int get_element_size(const char *filename) { // In Bytes
    struct stat s;

    if(stat(filename, &s) == -1) return -1;

    return s.st_size;
}

    
int file_line_count(const char *filename){
    FILE *fp;
    char buffer[MAX_FILENAME_LEN]; 
    int count = 0;
	int i;

    if((fp=fopen(filename, "r")) == NULL) return -1;

    while(fgets(buffer, MAX_FILENAME_LEN, fp) != NULL) {
       	for(i = 0; i < strlen(buffer); i++) {
			if(isspace(buffer[i]) != 1) {
				count++;
				break;
			}
		}
    }

    fclose(fp);

    return count;
}
