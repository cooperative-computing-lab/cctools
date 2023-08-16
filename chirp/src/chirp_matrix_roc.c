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

int isSubjectIdEqual(const char *setAfilename, const char *setBfilename);
INT64_T get_local_path(char *, char *, time_t);

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

	int setAindex, setBindex;	// funcindex;
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
	double *resbuff = NULL;
	int numels;
	int cntr;

	// Variables defined by Li
	int i;			// for multiprocess calculation
	int numOfMovingElements, numOfStableElements;
	//int setACount, setBCount;
	int setAPos, setBPos;
	long setAStartPos;
	//long setBStartPos;
	int x1, y1, x2, y2, topLeftX, topLeftY;	// [x1,y1]-start position, [x2,y2]-end position, the sub matrix we compute in a round
	int x_rel, y_rel;

	double threshold;
	double threshold_min = 0;
	double threshold_max = 1;
	double threshold_interval = 0.2;
	int count_thresholds;
	int count_genuine = 0;
	int count_impostar = 0;
	//int count_fa = 0;
	//int count_fr = 0;
	double (*roc_data)[3];

	int subject_equal;
	// ~Variables defined by Li

	int w, h, e, n;
	w = 10;
	h = 10;
	e = 8;
	n = 1;

	x1 = y1 = x2 = y2 = -1;

	topLeftX = topLeftY = 0;

	debug_config(argv[0]);

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

	if((argc - optind) < 4) {
		fprintf(stderr, "after all options, you must have: setA setB function mathost matpath\n");
		exit(0);
	}

	stoptime = time(0) + timeout;

	setAindex = optind;
	setBindex = optind + 1;
	mathost = optind + 2;
	matpath = optind + 3;

	// Set threshhold min, max and interval
	threshold_min = 0;
	threshold_max = 1;
	threshold_interval = 0.01;

	// Initialize result array - roc_data
	count_thresholds = (threshold_max - threshold_min) / threshold_interval + 1;
	roc_data = malloc(count_thresholds * 3 * sizeof(double));
	if(!roc_data) {
		fprintf(stderr, "Cannot initialize result buffer!\n");
		exit(1);
	}
	for(i = 0, threshold = threshold_min; i < count_thresholds; i++, threshold += threshold_interval) {
		roc_data[i][0] = threshold;
	}

	// Load matrix to be verified
	printf("X1,X2,Y1,Y2: %i,%i,%i,%i\n", x1, x2, y1, y2);
	mat = chirp_matrix_open(argv[mathost], argv[matpath], stoptime);
	if(mat == NULL) {
		fprintf(stderr, "No such matrix. Fail.\n");
		exit(1);
	}

	printf("Start loading matrix ... \n");
	printf("Width, height: %d, %d\n\n", chirp_matrix_width(mat), chirp_matrix_height(mat));

	numels = (x2 - x1 + 1) * (y2 - y1 + 1);
	resbuff = (double *) malloc(numels * sizeof(double));
	double *pilot_resbuff;
	pilot_resbuff = resbuff;

	// TODO get_range function can get at most 10*10 matrix a time (actually it can get more)
	/**
	for(j=0; j<y2-y1+1; j++) {
		for(i=0; i<x2-x1+1; i++) {
			int matrtn = chirp_matrix_get_range( mat, x1+i, y1+j, 1, 1, pilot_resbuff, stoptime); //(x2-x1+1), (y2-y1+1), resbuff, stoptime );
			printf("%.2f\t", (*pilot_resbuff));
			if(matrtn == -1) printf("return mat error @ [%d, %d]!!\n", x1+i, y1+j);
			pilot_resbuff++;
		}
		printf("\n");
	}*/
	int matrtn = chirp_matrix_get_range(mat, x1, y1, x2 - x1 + 1, y2 - y1 + 1, resbuff, stoptime);	//(x2-x1+1), (y2-y1+1), resbuff, stoptime );
	if(matrtn == -1) {
		fprintf(stderr, "return mat error @ [%d, %d], width: %d; height: %d!\n", x1, y1, x2 - x1 + 1, y2 - y1 + 1);
		exit(1);
	}

	printf("*******end of loading matrix********\n\n");

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

	// Go forward until line x1 in Set A list file
	for(i = 0; i < x1 && !feof(setA); i++) {
		fgets(setAfilename, len, setA);
	}
	if(i < x1) {
		fprintf(stderr, "Set A has less then x1 elements!\n");
		exit(1);
	}
	setAStartPos = ftell(setA);

	// Go forward until line y1 in Set B list file
	for(i = 0; i < y1 && !feof(setB); i++) {
		fgets(setBfilename, len, setB);
	}
	if(i < y1) {
		fprintf(stderr, "Set B has less then x1 elements!\n");
		exit(1);
	}
	//setBStartPos = ftell(setB);

	debug(D_CHIRP, "Matrix data:\n");
	// start loop
	fgets(setBfilename, len, setB);
	if(setBfilename != NULL) {
		size_t last = strlen(setBfilename) - 1;
		if(setBfilename[last] == '\n')
			setBfilename[last] = '\0';
	}

	for(setBPos = y1; !feof(setB) && setBPos <= y2; setBPos++) {	// Set B - column of matrix

		// Go directly to line y1 in Set B list file
		fseek(setA, setAStartPos, SEEK_SET);

		fgets(setAfilename, len, setA);
		if(setAfilename != NULL) {
			size_t last = strlen(setAfilename) - 1;
			if(setAfilename[last] == '\n')
				setAfilename[last] = '\0';
		}

		setAPos = x1;
		for(setAPos = x1; !feof(setA) && setAPos <= x2; setAPos++) {	// Set A- row of matrix
			// Threshhold comparison
			cntr = ((setBPos - y1) * (x2 - x1 + 1)) + (setAPos - x1);

			subject_equal = isSubjectIdEqual(setAfilename, setBfilename);
			if(subject_equal == 1) {	// A genuine match
				for(threshold = threshold_max, i = count_thresholds - 1; 1 - resbuff[cntr] < threshold; threshold -= threshold_interval, i--) {
					// False reject
					roc_data[i][1] += 1;
				}
				count_genuine++;
			} else if(subject_equal == 0) {	// A impostar match
				for(threshold = threshold_min, i = 0; 1 - resbuff[cntr] >= threshold; threshold += threshold_interval, i++) {
					// False accept
					roc_data[i][2] += 1;
				}
				count_impostar++;
			} else {
				fprintf(stderr, "Cannot resolve filename in either %s or %s!\n", setAfilename, setBfilename);
				exit(1);
			}

			debug(D_CHIRP, "%.2f\t", resbuff[cntr]);

			fgets(setAfilename, len, setA);
			if(setAfilename != NULL) {
				size_t last = strlen(setAfilename) - 1;
				if(setAfilename[last] == '\n')
					setAfilename[last] = '\0';
			}
		}
		debug(D_CHIRP, "\n");

		fgets(setBfilename, len, setB);
		if(setBfilename != NULL) {
			size_t last = strlen(setBfilename) - 1;
			if(setBfilename[last] == '\n')
				setBfilename[last] = '\0';
		}
	}


	printf("\n**********************************************************************\n");

	// Printf roc_data
	debug(D_CHIRP, "ROC raw data format: Threshold | False reject count | False accept count\n");
	for(i = 0; i < count_thresholds; i++) {
		debug(D_CHIRP, "%.2f\t%.2f\t%.2f;\t", roc_data[i][0], roc_data[i][1], roc_data[i][2]);
	}
	debug(D_CHIRP, "\n");

	// Transform roc_data to ROC curve data
	for(i = 0; i < count_thresholds; i++) {
		roc_data[i][1] = 1 - (roc_data[i][1] / count_genuine);	// 1 - FRR
		roc_data[i][2] = roc_data[i][2] / count_impostar;	// FAR
	}

	// Printf roc_data
	debug(D_CHIRP, "ROC curve data format: Threshold | 1 - False reject rate | False accept rate\n");
	for(i = 0; i < count_thresholds; i++) {
		debug(D_CHIRP, "%.2f\t%.2f\t%.2f;\t", roc_data[i][0], roc_data[i][1], roc_data[i][2]);
	}
	debug(D_CHIRP, "\n");

	// Write to dat file for gnuplot's use
	FILE *roc_data_fp;
	//char roc_line[20];
	roc_data_fp = fopen("roc.dat", "w");
	for(i = 0; i < count_thresholds; i++) {
		fprintf(roc_data_fp, "%.2f\t%.2f\n", roc_data[i][1], roc_data[i][2]);	// 1 - FRR, FAR
	}
	fclose(roc_data_fp);

	free(resbuff);

	debug(D_CHIRP, "%d comparisons in the matrix are tested! Genuine matches: %d\t Impostar matches: %d\n\n", cntr + 1, count_genuine, count_impostar);
	printf("\nROC curve data generation completed successfully!\n%d comparisons in the matrix are tested!\n", cntr + 1);

	return 0;
}


int isSubjectIdEqual(const char *setAfilename, const char *setBfilename)
{
	char *p;
	int n;

	p = strchr(setAfilename, '_');
	if(!p)
		return -1;	// Invalid filename
	n = p - setAfilename;

	p = strchr(setBfilename, '_');
	if(!p)
		return -1;	// Invalid filename
	if(p - setBfilename != n)
		return 0;	// Not equal

	if(strncmp(setAfilename, setBfilename, n) == 0)
		return 1;	// Equal
	else
		return 0;	// Not equal
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
