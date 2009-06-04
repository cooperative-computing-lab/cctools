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

// Defined by Li
#define CHIRP_STABLE_ELEMENTS_MAX 100000
#define CHIRP_MOVING_ELEMENTS_MAX 100000
#define CHIRP_PROCESSOR_MAX 64
#define MAX_FILENAME_LEN 512
#define MAX_FUNCNAME_LEN 128
#define MAX_THREAD_NUMBER 128
#define USING_INNER_FUNCTION 0
#define USING_OUTER_FUNCTION 1
// ~Defined by Li

static int timeout=3600;
static int buffer_size=1048576;
int *thr_status;
int *thr_count_within_block;

double (*compare_two_files)(const void *mmap1, size_t size1, const void *mmap2, size_t size2) = NULL;

INT64_T get_local_path(char *, char *, time_t);
int get_num_of_processors();
int resolve_block_size(const char *, const char *, int *, int *);
int get_free_mem();
int get_element_size(const char *);
int file_line_count(const char *);

// TODO Test this new mmap version!!!
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
	//while(isspace((*data1))) data1++;
	data1++;
	
	if(size1 - (data1 - (char *)mmap1) != size/8*2) {
		fprintf(stderr, "Image 1 data size error!!\n");
		exit(1);
	}
	
	//fscanf(fp1,"%d %d %d %d %d",&size,&band,&inner,&outer,&quality);

	//printf ("%d %d %d %d %d\n",size,band,inner,outer,quality);
	int code1[size];
	int mask1[size];
	//printf("%d\n",sizeof(buf));

	count =0;
	//printf("Code:\n");
	for(i = 0; i < size / 8; i++) {
		for (j = 0; j < 8; j++)
		{
			if (data1[i] & power2[j]) {
				code1[count] = 1;
				count++;
				//printf ("1");
			} else {
				code1[count] = 0;
				count++;
				//printf ("0");
			}
		}
	}
	//printf("\n\nMask:\n");
	
	// Let data1 now points to the start of mask data
	data1 += i;
	
	count =0;
	for(i = 0; i < size / 8; i++) {
		for (j = 0; j < 8; j++) {
			if (data1[i] & power2[j]) {
				mask1[count] = 1;
				count++;
				//printf ("1");
			} else {
				mask1[count] = 0;
				count++;
				//printf ("0");
			}
		}
	}
	//printf("\n");

	
	sscanf(data2, "%d %d %d %d %d", &size,&band,&inner,&outer,&quality);
	data2 = strchr(data2, '\n');

	// Let data2 points to the start of code data
	//while(isspace((*data2))) data2++;
	data2++;
	
	if(size2 - (data2 - (char *)mmap2) != size / 8 * 2) {
		fprintf(stderr, "Image 2 data size error!!\n");
		exit(1);
	}

	
	//fscanf(fp2,"%d %d %d %d %d",&size,&band,&inner,&outer,&quality);

	//printf ("%d %d %d %d %d\n",size,band,inner,outer,quality);
	int code2[size];
	int mask2[size];	
	//printf("%d\n",sizeof(buf));
	
	count =0;
	//printf("Code:\n");
	for(i = 0; i < size / 8; i++) {
		for (j = 0; j < 8; j++)
		{
			if (data2[i] & power2[j]) {
				code2[count] =1;
				count++;
				//printf ("1");
			} else {
				code2[count] =0;
				count++;
				//printf ("0");
			}
		}
	}
	//printf("\n\nMask:\n");
	
	// Let data1 now points to the start of mask data
	data2 += i;
	
	count =0;
	for(i = 0; i < size / 8; i++) {
		for (j = 0; j < 8; j++) {
			if (data2[i] & power2[j]) {
				mask2[count] =1;
				count++;
				//printf ("1");
			} else {
				mask2[count] =0;
				count++;
				//printf ("0");
			}
		}
	}
	//printf("\n");

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
	//printf ("%lf\n",(double)distance/total);
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
    debug(D_CHIRP, "min memory map size: %d\n file1 size: %ld\n file2 size: %ld\n", minsize, size1, size2);    
    
	count = 0;
	for(i=0; i<minsize; i++) {
        if(data1[i] != data2[i]) {
			count++;
			break;
		}
	}

    //debug(D_CHIRP, "%s and %s: found %dth character different. (bitwise)\n", filename1, filename2, count+1);
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
    debug(D_CHIRP, "min memory map size: %d\n file1 size: %ld\n file2 size: %ld\n", minsize, size1, size2);    

	int j;
	count = 0;
	for(i=0; i<minsize; i++) {
		tmp = 0;
		for(j=0; j<2; j++) {
			tmp += ((long)(data1[i]))*((long)(data1[i])) + ((long)(data2[i]))*((long)(data2[i]));
			tmp = tmp % 3;
		}
        if(data1[i]==data2[i]) {
			//printf("data1: %c, data2: %c\n", data1[i], data2[i]);
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
	//char func[MAX_FUNCNAME_LEN];
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
int movingFd; //[CHIRP_MOVING_ELEMENTS_MAX];
int stableFd; //[CHIRP_STABLE_ELEMENTS_MAX];
char *movingData[CHIRP_MOVING_ELEMENTS_MAX];
char *stableData[CHIRP_STABLE_ELEMENTS_MAX];
size_t movingSize[CHIRP_MOVING_ELEMENTS_MAX];
size_t stableSize[CHIRP_STABLE_ELEMENTS_MAX];

void *thr_function_compare(void *arg) {
	//char *filename1, *filename2;
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
		
		//filename1 = stableElements[colInBlock];
		//filename2 = movingElements[rowInBlock];
		
		rval = compare_two_files(stableData[colInBlock], stableSize[colInBlock], movingData[rowInBlock], movingSize[rowInBlock]);
		//debug(D_CHIRP, "Compare: %s and %s, result is: %2f\n", filename1, filename2, rval);

		// Write back result to temporary buffer
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
			fprintf(stderr, "Cannot initialize compare function. Failed.\n");
			exit(1);
		}

		compare_function_head->next = NULL;
	}

	function = (struct compare_function *)malloc(sizeof(struct compare_function));
	if(!function) {
		fprintf(stderr, "Cannot initialize compare function. Failed.\n");
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

static void show_version( const char *cmd ) {

	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n",cmd,CCTOOLS_VERSION_MAJOR,CCTOOLS_VERSION_MINOR,CCTOOLS_VERSION_MICRO,BUILD_USER,BUILD_HOST,__DATE__,__TIME__);
}

static void show_help( const char *cmd )
{
    /*
	printf("use: %s [options] <local-file> <hostname[:port]> <remote-file>\n",cmd);
	printf("where options are:\n");
	5A
	printf(" -a <flag>  Require this authentication mode.\n");
	printf(" -b <size>  Set transfer buffer size. (default is %d bytes)\n",buffer_size);
	printf(" -d <flag>  Enable debugging for this subsystem.\n");
	printf(" -f         Follow input file like tail -f.\n");
	printf(" -t <time>  Timeout for failure. (default is %ds)\n",timeout);
	printf(" -v         Show program version.\n");
	printf(" -h         This message.\n");
    */
}

int main(int argc, char *argv[]) {


    int did_explicit_auth = 0;
    int follow_mode = 0;
    time_t stoptime;
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
    struct chirp_matrix *mat = NULL;
    int mathost, matpath;

    // Variables defined by Li
    int i,j,k,l; // for multiprocess/multithread control
    int numOfMovingElements, numOfStableElements;
    int setACount, setBCount;
    int setAPos, setBPos;
    long setBStartPos;
    // [x1,y1]-start position, [x2,y2]-end position, the sub matrix we compute in a round
    int x1, y1, x2, y2, topLeftX, topLeftY;
    int x_rel, y_rel;
	
	FILE *tmpresult[CHIRP_PROCESSOR_MAX];
	
	// multithread
	int function_flag;
	int thread_err;
	void *tret;
	pthread_t tids[MAX_THREAD_NUMBER];
			
	
	int init_threads = 0;
    // ~Variables defined by Li
    
    int w,h,e,n;
    w=10;
    h=10;
    e=8;
    n=1;
    
    numOfMovingElements = -1;
    numOfStableElements = -1;
    numOfCores = -1;
			
    x1 = y1 = x2 = y2 = -1;

    topLeftX = topLeftY = 0;

    debug_config(argv[0]);
	register_compare_function("compare_bitdumb", compare_bitdumb);
	//register_compare_function("compare_bitwise", compare_bitwise);
	//register_compare_function("compare_nsquare", compare_nsquare);
	register_compare_function("compare_irises", compare_irises);
    while((c=getopt(argc,argv,"a:b:d:ft:vhw:i:e:n:x:y:p:q:r:s:X:Y:c:"))!=(char)-1) {
	switch(c) {
	case 'a':
	    auth_register_byname(optarg);
	    did_explicit_auth = 1;
	    break;
	case 'b':
	    buffer_size = atoi(optarg);
	    break;
	case 'd':
	    debug_flags_set(optarg);
	    break;
	case 'f':
	    follow_mode = 1;
	    break;
	case 't':
	    timeout = string_time_parse(optarg);
	    break;
	case 'w':
	    w = atoi(optarg);
	    break;
	case 'i':
	    h = atoi(optarg);
	    break;
	case 'e':
	    e = atoi(optarg);
	    break;
	case 'n':
	    n = atoi(optarg);
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
        case 'p':
            x1 = atoi(optarg);
            break;
        case 'q':
            y1 = atoi(optarg);
            break;
        case 'r':
            x2 = atoi(optarg);
            break;
        case 's':
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
	    
	}
    }

    if(!did_explicit_auth) auth_register_all();

    if( (argc-optind)<5 ) {
		fprintf(stderr, "after all options, you must have: setA setB function mathost matpath\n");
		exit(1);
    }
    
    stoptime = time(0) + timeout;

    setAindex=optind;
    setBindex=optind+1;
    funcindex=optind+2;
    mathost=optind+3;
    matpath=optind+4;
	
	// Deletion should be done, otherwise, it will open the previous matrix whose size is not determined
	// TODO check matrix size here, because you shouldn't delete the matrix in the distributed context
	chirp_matrix_delete(argv[mathost], argv[matpath], stoptime);
    // Create matrix at specified host and path to store results
    mat=chirp_matrix_open(argv[mathost], argv[matpath], stoptime);
    if(mat == NULL)
    {
	mat=chirp_matrix_create( argv[mathost], argv[matpath], w, h, e, n, stoptime);
	if(mat == NULL) {
	    fprintf(stderr, "Cannot create matrix at %s:%s.\n", argv[mathost], argv[matpath]);
	    exit(1);
	}
	
    }
	// Set compare function: INNER or OUTER
	if(set_compare_function(argv[funcindex]) == 0) {// Cannot find a internal function for the specified function name 
		function_flag = USING_OUTER_FUNCTION;
		if(access(argv[funcindex], X_OK) != 0) {
			fprintf(stderr, "Cannot execute program - %s (no permission) or program does not exist!\n", argv[funcindex]);
			exit(1);
		}
    	debug(D_CHIRP, "Using outer function.\n");
	} else {
		function_flag = USING_INNER_FUNCTION;
    	debug(D_CHIRP, "Using inner function.\n");
	}
	
    // Get local path for data sets directories
    if(get_local_path(setApath, argv[setAindex], stoptime) != 0 || get_local_path(setBpath, argv[setBindex], stoptime) != 0) {
        fprintf(stderr, "Paths to data sets are invalid!\n");
        exit(1);
    }

    // setA and setB each contains a list of file names that points to the data files
    char setAlistfile[CHIRP_PATH_MAX];
    char setBlistfile[CHIRP_PATH_MAX];
   
    strcpy(setAlistfile, setApath);
    strcat(setAlistfile, LIST_FILE_NAME);
    if((setA=fopen(setAlistfile, "r")) == NULL) {
        fprintf(stderr, "Cannot open data set A list file - %s!\n", setAlistfile);
        exit(1);
    }

    strcpy(setBlistfile, setBpath);
    strcat(setBlistfile, LIST_FILE_NAME);
    if((setB=fopen(setBlistfile, "r")) == NULL) {
        fprintf(stderr, "Cannot open data set B list file - %s!\n", setBlistfile);
        exit(1);
    }
    // Resolve block size and number of cores 
    if(numOfCores <= 0) {
        numOfCores = get_num_of_processors();
    } else {
        numOfCores = numOfCores > get_num_of_processors() ? get_num_of_processors() : numOfCores;
    }
    resolve_block_size(setAlistfile, setBlistfile, &numOfStableElements, &numOfMovingElements);
	// Stable and moving elements max are 2000!!!
    debug(D_CHIRP, "\nNumber of  Moving Elements: %d\nNumber of Stalbe Elements: %d\nNumber of Cores: %d\n", numOfMovingElements, numOfStableElements, numOfCores);
    debug(D_CHIRP, "Top left X: %d ; Top left Y: %d \n", topLeftX, topLeftY);
   
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
        fprintf(stderr, "Set A has less then x1 elements!\n");
        exit(1);
    }

    // Go forward until line y1 in Set B list file
    for(i = 0; i < y1 && !feof(setB); i++) {
        fgets(setBfilename, len, setB);
    }
    if(i < y1) {
        fprintf(stderr, "Set A has less then x1 elements!\n");
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
					fprintf(stderr, "Cannot open file - %s for comparison.\n", filename1);
					exit(1);
				}


				if(stat(filename1, &filestat1) == -1){
					fprintf(stderr, "Get %s stat error!\n", filename1);
					exit(1);
				}

				stableSize[i] = filestat1.st_size;


				if((stableData[i] = mmap((caddr_t)0, filestat1.st_size, PROT_READ, MAP_SHARED, stableFd, 0)) == (caddr_t)(-1)) {
					fprintf(stderr, "mmap file - %s error!\n", filename1);
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
								debug(D_CHIRP, "cell index: [%d, %d] value: %lf\n", x1+setAPos-setACount+(countInBlock%numOfAvailableStableElements), y1+setBPos-setBCount+(int)(countInBlock/numOfAvailableStableElements) , resbuff[gnum+countInBlock]);
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
								
						debug(D_CHIRP, "Starting command - %s ...\n", cmdrun);
						if((tmpresult[k] = fast_popen(cmdrun)) == NULL){
							fprintf(stderr, "Cannot execute user specified program! Function \'fast_popen\' failed. \n");
							exit(1);
						}
						
					}
				}	
				for(l = 0; l < k; l++) {
					fscanf(tmpresult[l], "%lf", &(resbuff[countInBlock]));
					debug(D_CHIRP, "cell index: [%d, %d] value: %lf\n", x1+setAPos-setACount+(countInBlock%numOfAvailableStableElements), y1+setBPos-setBCount+(int)(countInBlock/numOfAvailableStableElements) , resbuff[gnum+countInBlock]);
					fast_pclose(tmpresult[l]);
					countInBlock++;
				}
			} else {// Multithread routine
					
				// Mmap all the current moving elements into main memory
				for(i = 0; i < numOfAvailableMovingElements; i++) {	
					filename1 = movingElements[i];
					
					if((movingFd = open(filename1, O_RDONLY)) == -1){
						fprintf(stderr, "Cannot open file - %s for comparison.\n", filename1);
						exit(1);
					}


					if(stat(filename1, &filestat1) == -1){
						fprintf(stderr, "Get %s stat error!\n", filename1);
						exit(1);
					}
					
					movingSize[i] = filestat1.st_size;


					if((movingData[i] = mmap((caddr_t)0, filestat1.st_size, PROT_READ, MAP_SHARED, movingFd, 0)) == (caddr_t)(-1)) {
						fprintf(stderr, "mmap file - %s error!\n", filename1);
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
						fprintf(stderr, "Cannot create thread: %s\n", strerror(thread_err));
						exit(1);
					}
				}
				// Join the threads that have been started. After all have been joined, move on to the next 'block'
				for(j = 0; j < init_threads; j++) {
					thread_err = pthread_join(tids[j], &tret);
					if(thread_err != 0) {
						fprintf(stderr, "Cannot join with thread: %s\n", strerror(thread_err));
						exit(1);
					}
				}
				
				// Munmap data
				for(i = 1; i < numOfAvailableMovingElements; i++) {
					if(munmap(movingData[i], movingSize[i]) == -1)
						printf("munmap file - %s error!\n", movingElements[i]);
				}
			}
			
			
			if(function_flag == USING_INNER_FUNCTION) {
				for(i = 1; i < numOfAvailableStableElements; i++) {
					if(munmap(stableData[i], stableSize[i]) == -1)
						printf("munmap file - %s error!\n", stableElements[i]);
					//close(stableFd[i]);
				}
			}
			
			// Write data to the remote matrix
            debug(D_CHIRP, "Output to matrix at (%d, %d), width:%d, height:%d.\n", topLeftX+x1+x_rel, topLeftY+y1+y_rel, numOfAvailableStableElements, numOfMovingElements);
	    	chirp_matrix_set_range( mat, topLeftX + x1 + x_rel, topLeftY + y1 + y_rel, numOfAvailableStableElements, numOfAvailableMovingElements, resbuff, stoptime );
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

INT64_T get_local_path(char *local_path, char *path, time_t stoptime) {
    char *hostname, *chirp_path; 
    char *p;
    int i, count;

    p = strchr(path, '/');
    if(p == NULL || p != path) {
        getcwd(local_path, CHIRP_PATH_MAX);
        strcat(local_path, "/");
        strcat(local_path, path);
        if(local_path[strlen(local_path)-1] != '/') strcat(local_path, "/");
        return 0;
    } else {
        if(strncmp(p+1, "chirp/", 6) != 0) {
            // Given path is already a local path, return directly.
            strcpy(local_path, path);
            if(local_path[strlen(local_path)-1] != '/') strcat(local_path, "/");
            return 0;
        }
   }

    hostname = (char *) malloc(CHIRP_PATH_MAX*sizeof(char)); // allocate space for the source host
    if(hostname == NULL) {
        fprintf(stderr, "Allocating hostname memory failed! \n");
        return -1;
    }

    gethostname(hostname, CHIRP_PATH_MAX); // this may not have domain name, though!
    if(hostname == NULL) {
        printf("no hostname!\n");
        return -1;
    }
    
   
    INT64_T retval;

    // get chirp path
    count = 0;
    for(i = 0; i < strlen(path); i++) {
        if(path[i] == '/') count++;
        if(count == 3) break;
    }
    if(count != 3) {
        fprintf(stderr, "Cannot resolve chirp path - %s. Failed!\n", path);
        return -1;
    }
    while(i < strlen(path)) {
        i++;
        if(path[i] != '/') break;
    }

    chirp_path = path + i - 1;        
    for(i = 0; i < CHIRP_PATH_MAX; i++) local_path[i] = '\0'; 
    debug(D_CHIRP, "chirp_path: %s\n", chirp_path);
    debug(D_CHIRP, "local_path before resolve: %s\n", local_path);
 
    // get local path for the given chirp path on current machine   
    retval = chirp_reli_localpath(hostname, chirp_path, local_path, CHIRP_PATH_MAX, stoptime);
    if(retval < 0) {
        return retval;
    } else {
        debug(D_CHIRP, "local_path after resolve: %s\n", local_path);
        if(local_path[strlen(local_path)-1] != '/') strcat(local_path, "/");
        return 0;
    }
}

int resolve_block_size(const char *file1, const char *file2, int *x, int *y) {
    int free_mem;
    int lineCount1, lineCount2, m, n;
    long numOfFileInCache;
    int element_size;
    FILE *p;
    char setAElementPath[CHIRP_PATH_MAX];
    char setAElementFilename[CHIRP_PATH_MAX];
    char *last_slash;

    lineCount1 = file_line_count(file1);
    lineCount2 = file_line_count(file2);
    
    if(*x > 0 && *y > 0) {
        if(*x > lineCount1) *x = lineCount1;
        if(*y > lineCount2) *y = lineCount2;
        return 1;
    }
    free_mem = get_free_mem();
    debug(D_CHIRP, "Free memory: %d KB = %d MB\n", free_mem, free_mem>>10);

    // default block size: 10 * 10
    *x = 10;
    *y = 10;

    // get a single element's file name (relative)
    if((p=fopen(file1,"r")) == NULL) return -1;   
    fgets(setAElementFilename, CHIRP_PATH_MAX, p);
    if (setAElementFilename != NULL) {
        size_t last = strlen (setAElementFilename) - 1;
	if (setAElementFilename[last] == '\n') setAElementFilename[last] = '\0';
    } else {
        fclose(p);
        return -1;
    }
    fclose(p);
    
    // get element absolute path
    strcpy(setAElementPath, file1);
    last_slash = strrchr(setAElementPath, '/');
    *(last_slash+1) = '\0';
    strcat(setAElementPath, setAElementFilename);
    element_size = get_element_size(setAElementPath);
    debug(D_CHIRP, "Estimate element size using file - %s\nElement size: %d Bytes = %d KB = %d MB\n", setAElementPath, element_size, element_size>>10, element_size>>20);

    if(free_mem == -1 || element_size == -1) return -1;

    numOfFileInCache = ((free_mem * 1024L) / element_size) / 2;

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
		m = lineCount1;
		n = lineCount2;
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
    FILE *p;
    char c;
    int count = 0;

    if((p=fopen(filename, "r")) == NULL) return -1;

    while((c = fgetc(p)) != EOF) {
       if(c == '\n') count++;
    }

    fclose(p);

    return count;
}

int get_free_mem() { // kb
    FILE *meminfo;
    int mem_free, mem_buffer, mem_cached, tmp;
    char buffer[128];
    char item[20];
    //int shiftbytes = 10;
    if((meminfo = fopen("/proc/meminfo", "r")) == NULL) {
        fprintf(stderr, "Cannot open /proc/meminfo!\n");
        return -1;
    }

    mem_free = mem_buffer = mem_cached = -1;

    while(fgets(buffer, 128, meminfo) != NULL) {
        if(mem_free != -1 && mem_buffer != -1 && mem_cached != -1) break;
        if(sscanf(buffer, "%s%d", item, &tmp) == 2) {
            if(!strcmp(item, "MemFree:")) {
                mem_free = tmp;
            } else if (!strcmp(item, "Buffers:")) {
                mem_buffer = tmp;
            } else if (!strcmp(item, "Cached:")) {
                mem_cached = tmp;
            } else {
            }
         }
    }

    fclose(meminfo);
    if(mem_free == -1 || mem_buffer == -1 || mem_cached == -1) 
        return -1;
    else 
        return (mem_free + mem_buffer + mem_cached); // >> shiftbytes;
}

int get_num_of_processors() {
    FILE *cpuinfo;
    char buffer[128];
    char item1[20], item2[20];
    int tmp;
    int count = 0;
    
    if((cpuinfo = fopen("/proc/cpuinfo", "r")) == NULL) {
        fprintf(stderr, "Cannot open /proc/cpuinfo!\n");
        return -1;
    }

    while(fgets(buffer, 128, cpuinfo) != NULL) {
        if(sscanf(buffer, "%s%s%d", item1, item2, &tmp) == 3) {
            if(!strcmp(item1, "processor")) {
                count = tmp;
            }
         }
    }
   
    fclose(cpuinfo);
     
    return count+1;
}
