/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
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

#include "cctools.h"
#include "debug.h"
#include "auth_all.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "fast_popen.h"

// Defined by Li
#define MAX_FILENAME_LEN 512
#define MAX_FUNCNAME_LEN 128
#define USING_INNER_FUNCTION 0
#define USING_OUTER_FUNCTION 1
// ~Defined by Li

static int timeout = 3600;
static int buffer_size = 1048576;
int function_flag;

int (*compare_two_files) (char *filename1, char *filename2) = NULL;

INT64_T get_local_path(char *, char *, time_t);

// Using mmap() in file I/O
int compare_bitwise(char *filename1, char *filename2)
{
	int fd1, fd2;
	char *data1, *data2;
	int i, count = 0;
	unsigned int minfilesize;
	struct stat filestat1, filestat2;

	if((fd1 = open(filename1, O_RDONLY)) == -1) {
		fprintf(stderr, "Cannot open file - %s for comparison.\n", filename1);
		exit(1);
	}

	if((fd2 = open(filename2, O_RDONLY)) == -1) {
		fprintf(stderr, "Cannot open file - %s for comparison.\n", filename2);
		exit(1);
	}

	if(stat(filename1, &filestat1) == -1) {
		fprintf(stderr, "Get %s stat error!\n", filename1);
		exit(1);
	}

	if(stat(filename2, &filestat2) == -1) {
		fprintf(stderr, "Get %s stat error!\n", filename2);
		exit(1);
	}

	if((data1 = mmap((caddr_t) 0, filestat1.st_size, PROT_READ, MAP_SHARED, fd1, 0)) == (caddr_t) (-1)) {
		fprintf(stderr, "mmap file - %s error!\n", filename1);
		exit(1);
	}

	if((data2 = mmap((caddr_t) 0, filestat2.st_size, PROT_READ, MAP_SHARED, fd2, 0)) == (caddr_t) (-1)) {
		fprintf(stderr, "mmap file - %s error!\n", filename2);
		exit(1);
	}

	minfilesize = filestat1.st_size - filestat2.st_size ? filestat2.st_size : filestat1.st_size;
	debug(D_CHIRP, "min file size: %d\n file1 size: %ld\n file2 size: %ld\n", minfilesize, filestat1.st_size, filestat2.st_size);

	count = 0;
	for(i = 0; i < minfilesize; i++) {
		if(data1[i] != data2[i]) {
			count++;
			break;
		}
	}

	if(munmap(data1, filestat1.st_size) == -1)
		fprintf(stderr, "munmap file - %s error!\n", filename1);

	if(munmap(data2, filestat2.st_size) == -1)
		fprintf(stderr, "munmap file - %s error!\n", filename2);

	close(fd1);
	close(fd2);

	debug(D_CHIRP, "%s and %s: found %dth character different. (bitwise)\n", filename1, filename2, count + 1);
	return count;
}

// Using mmap() in file I/O
int compare_bitdumb(char *filename1, char *filename2)
{
	int fd1, fd2;
	char *data1, *data2;
	int i, count = 0;
	unsigned int minfilesize;
	struct stat filestat1, filestat2;
	long tmp;

	if((fd1 = open(filename1, O_RDONLY)) == -1) {
		fprintf(stderr, "Cannot open file - %s for comparison.\n", filename1);
		exit(1);
	}

	if((fd2 = open(filename2, O_RDONLY)) == -1) {
		fprintf(stderr, "Cannot open file - %s for comparison.\n", filename2);
		exit(1);
	}

	if(stat(filename1, &filestat1) == -1) {
		fprintf(stderr, "Get %s stat error!\n", filename1);
		exit(1);
	}

	if(stat(filename2, &filestat2) == -1) {
		fprintf(stderr, "Get %s stat error!\n", filename2);
		exit(1);
	}

	if((data1 = mmap((caddr_t) 0, filestat1.st_size, PROT_READ, MAP_SHARED, fd1, 0)) == (caddr_t) (-1)) {
		fprintf(stderr, "mmap file - %s error!\n", filename1);
		exit(1);
	}

	if((data2 = mmap((caddr_t) 0, filestat2.st_size, PROT_READ, MAP_SHARED, fd2, 0)) == (caddr_t) (-1)) {
		fprintf(stderr, "mmap file - %s error!\n", filename2);
		exit(1);
	}

	minfilesize = filestat1.st_size - filestat2.st_size ? filestat2.st_size : filestat1.st_size;
	debug(D_CHIRP, "min file size: %d\n file1 size: %ld\n file2 size: %ld\n", minfilesize, filestat1.st_size, filestat2.st_size);

	int j;
	count = 0;
	for(i = 0; i < minfilesize; i++) {
		tmp = 0;
		for(j = 0; j < 2; j++) {
			tmp += ((long) data1[i]) * ((long) data1[i]) + ((long) data2[i]) * ((long) data2[i]);
			tmp = tmp % 3;
		}
		if(data1[i] == data2[i]) {
			count += (int) tmp + 1;
			count -= tmp;
		}
	}

	if(munmap(data1, filestat1.st_size) == -1)
		printf("munmap file - %s error!\n", filename1);

	if(munmap(data2, filestat2.st_size) == -1)
		printf("munmap file - %s error!\n", filename2);

	close(fd1);
	close(fd2);

	debug(D_CHIRP, "%s and %s: %d characters are the same.\n", filename1, filename2, count);
	return count;
}

// Using mmap() in file I/O
int compare_nsquare(char *filename1, char *filename2)
{
	int fd1, fd2;
	char *data1, *data2;
	int i, j, count = 0;
	unsigned int minfilesize;
	struct stat filestat1, filestat2;

	if((fd1 = open(filename1, O_RDONLY)) == -1) {
		fprintf(stderr, "Cannot open file - %s for comparison.\n", filename1);
		exit(1);
	}

	if((fd2 = open(filename2, O_RDONLY)) == -1) {
		fprintf(stderr, "Cannot open file - %s for comparison.\n", filename2);
		exit(1);
	}

	if(stat(filename1, &filestat1) == -1) {
		fprintf(stderr, "Get %s stat error!\n", filename1);
		exit(1);
	}

	if(stat(filename2, &filestat2) == -1) {
		fprintf(stderr, "Get %s stat error!\n", filename2);
		exit(1);
	}

	if((data1 = mmap((caddr_t) 0, filestat1.st_size, PROT_READ, MAP_SHARED, fd1, 0)) == (caddr_t) (-1)) {
		fprintf(stderr, "mmap file - %s error!\n", filename1);
		exit(1);
	}

	if((data2 = mmap((caddr_t) 0, filestat2.st_size, PROT_READ, MAP_SHARED, fd2, 0)) == (caddr_t) (-1)) {
		fprintf(stderr, "mmap file - %s error!\n", filename2);
		exit(1);
	}

	minfilesize = filestat1.st_size - filestat2.st_size ? filestat2.st_size : filestat1.st_size;
	debug(D_CHIRP, "min file size: %d\n file1 size: %ld\n file2 size: %ld\n", minfilesize, filestat1.st_size, filestat2.st_size);

	count = 0;
	for(i = 0; i < filestat1.st_size; i++)
		for(j = 0; j < filestat2.st_size; j++)
			if(data1[i] == data2[j] && i == j) {
				count++;
				printf("%d, %d match!\n", i, j);
			}

	if(munmap(data1, filestat1.st_size) == -1)
		printf("munmap file - %s error!\n", filename1);

	if(munmap(data2, filestat2.st_size) == -1)
		printf("munmap file - %s error!\n", filename2);

	close(fd1);
	close(fd2);

	debug(D_CHIRP, "%s and %s: %d characters are the same.\n", filename1, filename2, count);
	// for debug
	//count = 0;
	return count;
}


struct compare_function {
	char name[MAX_FUNCNAME_LEN];
	int (*pointer) (char *filename1, char *filename2);
	struct compare_function *next;
};

struct compare_function *compare_function_head = NULL;

void register_compare_function(const char *function_name, int (*compare_function_pointer) (char *filename1, char *filename2))
{
	struct compare_function *function;

	if(!compare_function_head) {
		compare_function_head = (struct compare_function *) malloc(sizeof(struct compare_function));
		if(!compare_function_head) {
			fprintf(stderr, "Cannot initialize compare function. Failed.\n");
			exit(1);
		}

		compare_function_head->next = NULL;
	}

	function = (struct compare_function *) malloc(sizeof(struct compare_function));
	if(!function) {
		fprintf(stderr, "Cannot initialize compare function. Failed.\n");
		exit(1);
	}

	strcpy(function->name, function_name);
	function->pointer = compare_function_pointer;
	function->next = compare_function_head->next;
	compare_function_head->next = function;
}

int set_compare_function(const char *function_name)
{
	struct compare_function *p;

	if(!compare_function_head)
		return 0;
	p = compare_function_head;
	while((p = p->next) != NULL) {
		if(strcmp(p->name, function_name) == 0) {
			compare_two_files = p->pointer;
			break;
		}
	}

	if(!p)
		return 0;
	else
		return 1;
}

static void show_help(const char *cmd)
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

int main(int argc, char *argv[])
{
	int did_explicit_auth = 0;
	int follow_mode = 0;
	time_t stoptime;
	signed char c;

	int setAindex, setBindex, funcindex;
	FILE *setA = NULL;
	FILE *setB = NULL;
	char setApath[CHIRP_PATH_MAX];
	char setBpath[CHIRP_PATH_MAX];
	char *LIST_FILE_NAME = "set.list";
	char setAfilename[CHIRP_PATH_MAX];
	char setBfilename[CHIRP_PATH_MAX];
	char param_fileA[CHIRP_PATH_MAX];
	char param_fileB[CHIRP_PATH_MAX];
	int len = CHIRP_PATH_MAX;
	struct chirp_matrix *mat = NULL;
	int mathost, matpath;
	double *resbuff = NULL;
	int numels;
	int cntr;

	// Variables defined by Li
	int i;			// for multiprocess calculation
	int numOfMovingElements, numOfStableElements;
	int setACount, setBCount;
	int setAPos, setBPos;
	long setAStartPos;
	// [x1,y1]-start position, [x2,y2]-end position, the sub matrix we compute in a round
	int x1, y1, x2, y2, topLeftX, topLeftY;
	int x_rel, y_rel;

	// ~Variables defined by Li

	int w, h, e, n;
	w = 10;
	h = 10;
	e = 8;
	n = 1;

	x1 = y1 = x2 = y2 = -1;

	topLeftX = topLeftY = 0;

	debug_config(argv[0]);
	register_compare_function("compare_bitdumb", compare_bitdumb);
	register_compare_function("compare_bitwise", compare_bitwise);
	register_compare_function("compare_nsquare", compare_nsquare);

	while((c = getopt(argc, argv, "a:b:d:ft:vhw:i:e:n:x:y:p:q:r:s:X:Y:c:")) > -1) {
		switch (c) {
		case 'a':
			if (!auth_register_byname(optarg))
				fatal("could not register authentication method `%s': %s", optarg, strerror(errno));
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
			cctools_version_print(stdout, argv[0]);
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
			//numOfCores = atoi(optarg);
			break;

		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if(!did_explicit_auth)
		auth_register_all();

	if((argc - optind) < 5) {
		fprintf(stderr, "after all options, you must have: setA setB function mathost matpath\n");
		exit(0);
	}

	stoptime = time(0) + timeout;

	setAindex = optind;
	setBindex = optind + 1;
	funcindex = optind + 2;
	mathost = optind + 3;
	matpath = optind + 4;

	// Load matrix to be verified
	printf("X1,X2,Y1,Y2: %i,%i,%i,%i\n", x1, x2, y1, y2);
	mat = chirp_matrix_open(argv[mathost], argv[matpath], stoptime);
	if(mat == NULL) {
		fprintf(stderr, "No such matrix. Fail.\n");
		exit(1);
	}

	printf("width, height: %d, %d\n", chirp_matrix_width(mat), chirp_matrix_height(mat));

	numels = (x2 - x1 + 1) * (y2 - y1 + 1);
	resbuff = (double *) malloc(numels * sizeof(double));
	double *pilot_resbuff;
	pilot_resbuff = resbuff;
	// TODO get range function can get at most 10*10 matrix a time
	/**
	for(j=0; j<y2-y1+1; j++) {
		for(i=0; i<x2-x1+1; i++) {
			int matrtn = chirp_matrix_get_range( mat, x1+i, y1+j, 1, 1, pilot_resbuff, stoptime); //(x2-x1+1), (y2-y1+1), resbuff, stoptime );
			if(matrtn == -1) printf("return mat error @ [%d, %d]!!\n", x1+i, y1+j);
			pilot_resbuff++;
		}
	}*/

	int matrtn = chirp_matrix_get_range(mat, x1, y1, x2 - x1 + 1, y2 - y1 + 1, resbuff, stoptime);	//(x2-x1+1), (y2-y1+1), resbuff, stoptime );
	if(matrtn == -1) {
		fprintf(stderr, "return mat error @ [%d, %d], width: %d; height: %d!\n", x1, y1, x2 - x1 + 1, y2 - y1 + 1);
		exit(1);
	}

	/** test
	for(i=x1; i<=x2; i++) {
		for(j=y1; j<=y2; j++) {
			cntr=((i-x1)*(x2-x1+1))+(j-y1);
			printf("%lf\t", resbuff[cntr]);
		}
		printf("\n");
	}*/

	// Set compare function
	if(set_compare_function(argv[funcindex]) == 0) {
		function_flag = USING_OUTER_FUNCTION;
		if(access(argv[funcindex], X_OK) != 0) {
			fprintf(stderr, "Cannot execute program - %s or program does not exist!\n", argv[funcindex]);
			exit(1);
		}
	} else {
		function_flag = USING_INNER_FUNCTION;
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
	if((setA = fopen(setAlistfile, "r")) == NULL) {
		fprintf(stderr, "Cannot open data set A list file - %s!\n", setAlistfile);
		exit(1);
	}

	strcpy(setBlistfile, setBpath);
	strcat(setBlistfile, LIST_FILE_NAME);
	if((setB = fopen(setBlistfile, "r")) == NULL) {
		fprintf(stderr, "Cannot open data set B list file - %s!\n", setBlistfile);
		exit(1);
	}
	// Initialize position parameters and allocate memory for storing results of a block (sub matrix)
	x_rel = y_rel = 0;	// relative to the sub-matrix we are actually working on


	// Go forward until line y1 in Set B list file
	for(i = 0; i < y1 && !feof(setB); i++) {
		fgets(setBfilename, len, setB);
	}
	if(i < y1) {
		fprintf(stderr, "Set B has less then y1 elements!\n");
		exit(1);
	}
	// Go forward until line y1 in Set B list file
	for(i = 0; i < y1 && !feof(setA); i++) {
		fgets(setAfilename, len, setA);
	}
	if(i < x1) {
		fprintf(stderr, "Set A has less then x1 elements!\n");
		exit(1);
	}
	setAStartPos = ftell(setA);


	// start loop
	fgets(setBfilename, len, setB);
	if(setBfilename != NULL) {
		size_t last = strlen(setBfilename) - 1;
		if(setBfilename[last] == '\n')
			setBfilename[last] = '\0';
	}


	setAPos = x1;
	setBPos = y1;
	double rval;
	cntr = 0;
	printf("Progress: %d%%", 0);
	for(setBCount = 0; !feof(setB) && setBPos <= y2; setBCount++, setBPos++) {	// Set B - column of matrix
		// Go directly to line y1 in Set B list file
		fseek(setA, setAStartPos, SEEK_SET);

		fgets(setAfilename, len, setA);
		if(setAfilename != NULL) {
			size_t last = strlen(setAfilename) - 1;
			if(setAfilename[last] == '\n')
				setAfilename[last] = '\0';
		}
		setAPos = x1;

		for(setACount = 0; !feof(setA) && setAPos <= y2; setACount++, setAPos++) {

			strcpy(param_fileA, setApath);
			strcat(param_fileA, setAfilename);
			strcpy(param_fileB, setBpath);
			strcat(param_fileB, setBfilename);


			rval = (double) compare_two_files(param_fileA, param_fileB);
			//printf(" compare: %s and %s \nresult: %f, while in matrix[%d] it was: %f\n", param_fileA, param_fileB, rval, cntr, resbuff[cntr]);
			if(rval != resbuff[cntr]) {
				printf("Verification failed at [%d, %d] !\n", x1 + setACount, y1 + setBCount);
				exit(0);
			}

			cntr++;

			int progress = ((double) cntr / numels) * 100;

			printf("\rProgress: %d%%", progress);

			fgets(setAfilename, len, setA);
			if(setAfilename != NULL) {
				size_t last = strlen(setAfilename) - 1;
				if(setAfilename[last] == '\n')
					setAfilename[last] = '\0';
			}
		}

		fgets(setBfilename, len, setB);
		if(setBfilename != NULL) {
			size_t last = strlen(setBfilename) - 1;
			if(setBfilename[last] == '\n')
				setBfilename[last] = '\0';
		}
	}

	free(resbuff);

	fclose(setA);
	fclose(setB);

	printf("\nVerification Completed!\n%d elements in the matrix are tested!\n", cntr);
	return 0;
}


INT64_T get_local_path(char *local_path, char *path, time_t stoptime)
{
	char *hostname, *chirp_path;
	char *p;
	int i, count;

	p = strchr(path, '/');
	if(p == NULL || p != path) {
		getcwd(local_path, CHIRP_PATH_MAX);
		strcat(local_path, "/");
		strcat(local_path, path);
		if(local_path[strlen(local_path) - 1] != '/')
			strcat(local_path, "/");
		return 0;
	} else {
		if(strncmp(p + 1, "chirp/", 6) != 0) {
			// Given path is already a local path, return directly.
			strcpy(local_path, path);
			if(local_path[strlen(local_path) - 1] != '/')
				strcat(local_path, "/");
			return 0;
		}
	}

	hostname = (char *) malloc(CHIRP_PATH_MAX * sizeof(char));	// allocate space for the source host
	if(hostname == NULL) {
		fprintf(stderr, "Allocating hostname memory failed! \n");
		return -1;
	}

	gethostname(hostname, CHIRP_PATH_MAX);	// this may not have domain name, though!
	if(hostname == NULL) {
		printf("no hostname!\n");
		return -1;
	}


	INT64_T retval;

	// get chirp path
	count = 0;
	for(i = 0; i < strlen(path); i++) {
		if(path[i] == '/')
			count++;
		if(count == 3)
			break;
	}
	if(count != 3) {
		fprintf(stderr, "Cannot resolve chirp path - %s. Failed!\n", path);
		return -1;
	}
	while(i < strlen(path)) {
		i++;
		if(path[i] != '/')
			break;
	}

	chirp_path = path + i - 1;
	for(i = 0; i < CHIRP_PATH_MAX; i++)
		local_path[i] = '\0';
	debug(D_CHIRP, "chirp_path: %s\n", chirp_path);
	debug(D_CHIRP, "local_path before resolve: %s\n", local_path);

	// get local path for the given chirp path on current machine
	retval = chirp_reli_localpath(hostname, chirp_path, local_path, CHIRP_PATH_MAX, stoptime);
	if(retval < 0) {
		return retval;
	} else {
		debug(D_CHIRP, "local_path after resolve: %s\n", local_path);
		if(local_path[strlen(local_path) - 1] != '/')
			strcat(local_path, "/");
		return 0;
	}
}

/* vim: set noexpandtab tabstop=8: */
