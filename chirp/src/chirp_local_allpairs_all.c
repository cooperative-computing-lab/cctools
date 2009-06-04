#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "chirp_reli.h"
#include "chirp_matrix.h"

#include "debug.h"
#include "auth_all.h"
#include "stringtools.h"
#include "xmalloc.h"

// Defined by Li
#define CHIRP_STABLE_ELEMENTS_MAX 1000
#define CHIRP_PROCESSOR_MAX 16
// ~Defined by Li

static int timeout=3600;
static int buffer_size=1048576;

int get_num_of_processors();
int resolve_block_size(const char *, const char *, int *, int *);
int get_free_mem();
int get_element_size(const char *);
int file_line_count(const char *);

static void show_version( const char *cmd )
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n",cmd,CCTOOLS_VERSION_MAJOR,CCTOOLS_VERSION_MINOR,CCTOOLS_VERSION_MICRO,BUILD_USER,BUILD_HOST,__DATE__,__TIME__);
}

static void show_help( const char *cmd )
{
    /*
	printf("use: %s [options] <local-file> <hostname[:port]> <remote-file>\n",cmd);
	printf("where options are:\n");
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

    int psetindex, gsetindex, funcindex;
    FILE *pset = NULL;
    FILE *gset = NULL;
    char pfilename[CHIRP_PATH_MAX];
    //int pnum = 0;
    char gfilename[CHIRP_PATH_MAX];
    int gnum = 0;
    //char cmd[3*CHIRP_PATH_MAX];
    char cmdrun[CHIRP_PROCESSOR_MAX][3*CHIRP_PATH_MAX];
    int len = CHIRP_PATH_MAX;
    struct chirp_matrix *mat = NULL;
    int mathost, matpath;
    //struct chirp_file *outputs[argc-2];
    //INT64_T offsets[argc-2];
    //INT64_T linelen, written, target;
    //pid_t pid;
    //int chstatus;

    //FILE *resfile = NULL;
    double *resbuff = NULL;
    //int cntr;

    // Variables defined by Li
    int i,j; // for multiprocess calculation
    FILE *tmpresult[CHIRP_PROCESSOR_MAX];
    int numOfMovingElements, numOfStableElements;
    int psetCount, gsetCount;
    // [x1,y1]-start position, [x2,y2]-end position, the sub matrix we compute in a round
    int x1, y1, x2, y2;
    int numOfAvailableStableElements;
    int numOfAvailableMovingElements;
    int numOfProcessors;
    //char *movingElements[CHIRP_MULTICORE_MOVING_ELEMENTS];
    char stableElements[CHIRP_STABLE_ELEMENTS_MAX][CHIRP_PATH_MAX];
    // ~Variables defined by Li
    
    int w,h,e,n;
    w=10;
    h=10;
    e=8;
    n=1;
    
    numOfMovingElements = -1;
    numOfStableElements = -1;
    numOfProcessors = -1;
    
    debug_config(argv[0]);
    
    while((c=getopt(argc,argv,"a:b:d:ft:vhw:i:e:n:x:y:p:"))!=(char)-1) {
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
            numOfProcessors = atoi(optarg);
            break;
	    
	}
    }

    if(!did_explicit_auth) auth_register_all();

    if( (argc-optind)<5 ) {
	fprintf(stderr, "after all options, you must have: pset gset function mathost matpath\n");
	exit(0);
    }
    
    stoptime = time(0) + timeout;

    psetindex=optind;
    gsetindex=optind+1;
    funcindex=optind+2;
    mathost=optind+3;
    matpath=optind+4;
    

    if(numOfProcessors <= 0) {
        numOfProcessors = get_num_of_processors();
    } else {
        numOfProcessors = numOfProcessors > get_num_of_processors() ? get_num_of_processors() : numOfProcessors;
    }
    resolve_block_size(argv[psetindex], argv[gsetindex], &numOfStableElements, &numOfMovingElements);

    printf("moving: %d\nstalbe: %d\nprocessors: %d\n", numOfMovingElements, numOfStableElements, numOfProcessors);
    //exit(0);


    mat=chirp_matrix_open( argv[mathost], argv[matpath], stoptime);
    if(mat == NULL)
    {
	mat=chirp_matrix_create( argv[mathost], argv[matpath], w, h, e, n, stoptime);
	if(mat == NULL) {
	    fprintf(stderr, "Couldn't create matrix. Fail.\n");
	    exit(1);
	}
	
    }
    
    pset=fopen(argv[psetindex],"r");
    
       
    fgets(pfilename, len, pset);
    if (pfilename != NULL) {
	    size_t last = strlen (pfilename) - 1;
	    
	    if (pfilename[last] == '\n') pfilename[last] = '\0';
    }

    x1=y1=x2=y2=0;
    resbuff = (double *) malloc(numOfMovingElements*numOfStableElements*sizeof(double));

    while(!feof(pset)) {
        for(psetCount = 0; !feof(pset) && psetCount<numOfStableElements; psetCount++){
            //printf("stable file %s!\n", pfilename);
            strcpy(stableElements[psetCount], pfilename); 
            x2++;
            fgets(pfilename, len, pset);
            if (pfilename != NULL) {
	        size_t last = strlen (pfilename) - 1;	    
	        if (pfilename[last] == '\n') pfilename[last] = '\0';
            } 
        }

        numOfAvailableStableElements = psetCount;

            	    
        gset=fopen(argv[gsetindex],"r");
        fgets(gfilename, len, gset);
        if (gfilename != NULL) {
	    size_t last = strlen (gfilename) - 1;	    
	    if (gfilename[last] == '\n') gfilename[last] = '\0';
        }
        while(!feof(gset)) {
            for(gsetCount = 0; !feof(gset) && gsetCount<numOfMovingElements; gsetCount++){
                for(psetCount = 0; psetCount < numOfAvailableStableElements; psetCount+=i){
                    for(i = 0; i < numOfProcessors && psetCount+i < numOfAvailableStableElements; i++){
                    
                        strcpy(cmdrun[i],"");
	                strcat(cmdrun[i], argv[funcindex]);
	                strcat(cmdrun[i], " ");
	                strcat(cmdrun[i], stableElements[psetCount+i]);
	                strcat(cmdrun[i], " ");
	                strcat(cmdrun[i], gfilename);
                     }
                    for(j = 0; j < i; j++){
                        if((tmpresult[j] = popen(cmdrun[j], "r")) == NULL){
                            fprintf(stderr, "Cannot excute command. Fail. \n");
                            exit(1);
                        }
                    }
                    for(j = 0; j < i; j++){
                        fscanf(tmpresult[j], "%lf", &(resbuff[gnum+j]));
                        //printf("cell index: [%d, %d] value: %lf\n", x1+psetCount+j, y1+gsetCount, resbuff[gnum+j]);
                        pclose(tmpresult[j]);
                    }
                      
                    gnum += i;
	        }
                fgets(gfilename, len, gset);
	        if (gfilename != NULL) {
		    size_t last = strlen (gfilename) - 1;    
		    if (gfilename[last] == '\n') gfilename[last] = '\0';
	        }
            }
            numOfAvailableMovingElements = gsetCount;
            //printf("x1:%d, y1:%d, width:%d, height:%d.\n", x1, y1, numOfAvailableStableElements, numOfMovingElements);
	    chirp_matrix_set_range( mat, x1, y1, numOfAvailableStableElements, numOfMovingElements, resbuff, stoptime );
	    gnum = 0;
            y1 += numOfAvailableMovingElements;
	}
        fclose(gset);
        x1 += numOfAvailableStableElements;
        y1 = 0;		
    }
    free(resbuff);
    fclose(pset);
    
    return 0;
}

int resolve_block_size(const char *file1, const char *file2, int *x, int *y){
    int free_mem;
    int lineCount1, lineCount2, m, n;
    int numOfFileInCache;
    int element_size;

    lineCount1 = file_line_count(file1);
    lineCount2 = file_line_count(file2);
    
    if(*x > 0 && *y > 0) {
        if(*x > lineCount1) *x = lineCount1;
        if(*y > lineCount2) *y = lineCount2;
        return 1;
    }
    free_mem = get_free_mem();
    printf("free memory: %d KB = %d MB\n", free_mem, free_mem>>10);

    element_size = get_element_size(file1);
    printf("element size: %d Bytes = %d KB = %d MB\n", element_size, element_size>>10, element_size>>20);

    if(free_mem == -1 || element_size == -1) return -1;

    numOfFileInCache = (int)((free_mem * 1024L) / element_size);

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
    }
   
    //printf("m: %d ; n: %d\n", m, n);
 
    *x = m;
    *y = n; 
    return 1;
}

int get_element_size(const char *filename) {
    FILE *p;
    char pfilename[256];
    struct stat s;
    
    if((p=fopen(filename,"r")) == NULL) return -1;
    
    fgets(pfilename, 256, p);
    if (pfilename != NULL) {
        size_t last = strlen (pfilename) - 1;
	if (pfilename[last] == '\n') pfilename[last] = '\0';
    } else {
        fclose(p);
        return -1;
    }
    fclose(p);

    if(stat(pfilename, &s) == -1) return -1;

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

    //printf("line count: %s - %d\n", filename, count); 
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
    //printf("free:\t\t%d MB\n", mem_free>>shiftbytes);
    //printf("buffer:\t\t%d MB\n", mem_buffer>>shiftbytes);
    //printf("cached:\t\t%d MB\n", mem_cached>>shiftbytes);
    //printf("true free memory: %d MB\n", (mem_free+mem_buffer+mem_cached)>>shiftbytes); 
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
                //printf("i= %d, item in cpuinfo: %s : %d\n", i, item1, tmp);
                count = tmp;
            }
         }
    }
   
    fclose(cpuinfo);
     
    return count+1;
}
