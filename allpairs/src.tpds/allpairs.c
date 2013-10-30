/*
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>

#include <math.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

#include <pwd.h>
#include <sys/utsname.h>
#include <netdb.h>

#include <sys/time.h>
#include <time.h>

#include "chirp_reli.h"
#include "chirp_protocol.h"
#include "chirp_acl.h"
#include "chirp_group.h"
#include "chirp_matrix.h"

#include "catalog_query.h"
#include "nvpair.h"
#include "link.h"
#include "stringtools.h"
#include "debug.h"

#include "timestamp.h"
#include "auth_all.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "list.h"
#include "domain_name_cache.h"
#include "md5.h"
#include "macros.h"

#include "ragged_array.h"

const int MAXFILENAME = 256;
const int MAXRESULTLINE = 1024;
int limit;

static time_t stoptime=0;

static void show_help(const char *cmd)
{
	printf("Use: %s [options] <setA> <setB> <Function> <WorkloadID>\n", cmd);
	printf("where options are:\n");
	printf(" -a <mode>      Explicit authentication mode.\n");
	printf(" -d <subsystem> Enable debugging for this subsystem.  (Try -d all to start.)\n");
	printf(" -t <string>    Timeout, e.g. 60s\n");
	printf(" -R             Force remote execution, even if it is modeled to be slower.\n");
	printf(" -L             Force local execution, do not distribute and submit batch jobs.\n");
	printf(" -p <count>     Index into SetA of the first comparison. (Default: 0)\n");
	printf(" -q <count>     Index into SetB of the first comparison. (Default: 0)\n");
	printf(" -r <count>     Index into SetA of the last comparison. (Default: last index of SetA)\n");
	printf(" -s <count>     Index into SetB of the last comparison. (Default: last index of SetB)\n");
	printf(" -l <path>      Prefix for local state (default: /tmp/WorkloadID/)\n");
	printf(" -H <hostname>  Hostname for remote matrix metadata (default: sc0-00.cse.nd.edu)\n");
	printf(" -P <path>      Path for remote matrix metadata (default: /userid/matrixmeta/HOSTNAME_DATE_WorkloadID)\n");
	printf(" -v             Show version string\n");
	printf(" -h            Show this help screen\n");
}

/* Cygwin does not have 64-bit I/O, while Darwin has it by default. */

#if CCTOOLS_OPSYS_CYGWIN || CCTOOLS_OPSYS_DARWIN || CCTOOLS_OPSYS_FREEBSD
#define fopen64 fopen
#define open64 open
#define lseek64 lseek
#define stat64 stat
#define fstat64 fstat
#define lstat64 lstat
#define fseeko64 fseeko
#define ftruncate64 ftruncate
#define truncate64 truncate
#define statfs64 statfs
#define fstatfs64 fstatfs
#endif

static INT64_T do_put_recursive( const char *source_file, const char *target_host, const char *target_file );
static INT64_T do_put_one_dir( const char *source_file, const char *target_host, const char *target_file, int mode );
static INT64_T do_put_one_link( const char *source_file, const char *target_host, const char *target_file );
static INT64_T do_put_one_file( const char *source_file, const char *target_host, const char *target_file, int mode, INT64_T length );

static INT64_T do_put_one_dir( const char *source_file, const char *target_host, const char *target_file, int mode )
{
	char new_source_file[CHIRP_PATH_MAX];
	char new_target_file[CHIRP_PATH_MAX];
	struct list *work_list;
	const char *name;
	int result;
	struct dirent *d;
	DIR *dir;
	//printf("Putting directory %s onto %s at %s\n",source_file, target_host, target_file);

	work_list = list_create();

	result = chirp_reli_mkdir(target_host,target_file,mode,stoptime);
		
	if(result==0 || errno==EEXIST ) {
		result = 0;
		dir = opendir(source_file);
		if(dir) {
			while((d=readdir(dir))) {
				if(!strcmp(d->d_name,".")) continue;
				if(!strcmp(d->d_name,"..")) continue;
				list_push_tail(work_list,xxstrdup(d->d_name));
			}
			closedir(dir);
			while((name=list_pop_head(work_list))) {
				sprintf(new_source_file,"%s/%s",source_file,name);
				sprintf(new_target_file,"%s/%s",target_file,name);
				result = do_put_recursive(new_source_file,target_host,new_target_file);
				free((char*)name);
				if(result<0) break;
			}
		} else {
		    result = -1;
		}
	} else {
	    result = -1;
	}

	while((name=list_pop_head(work_list))) free((char*)name);
	list_delete(work_list);

	return result;
}

static INT64_T do_put_one_link( const char *source_file, const char *target_host, const char *target_file )
{
	char linkdata[CHIRP_PATH_MAX];
	int result;
	//printf("Putting link %s onto %s at %s\n",source_file, target_host, target_file);
	result = readlink(source_file,linkdata,sizeof(linkdata));
	if(result>0) {
	    linkdata[result] = 0;
	    //complete_local_path(linkdata,linkdatafull);
	    chirp_reli_unlink(target_host,target_file,stoptime);
	    result = chirp_reli_symlink(target_host,linkdata,target_file,stoptime);
	    if(result>=0) result = 0;
	}

	return result;
}

static INT64_T do_put_one_file( const char *source_file, const char *target_host, const char *target_file, int mode, INT64_T length )
{
	FILE *file;
	int save_errno;

	file = fopen64(source_file,"r");
	if(!file) return -1;

	//printf("Putting file %s onto %s at %s\n",source_file, target_host, target_file);
	length = chirp_reli_putfile(target_host,target_file,file,mode,length,stoptime);
	if(length<0) {
		save_errno = errno;
		fclose(file);
		errno = save_errno;
		return -1;
	}

	if(length>=0) {
		fclose(file);
		return length;
	} else {
		save_errno = errno;
		fclose(file);
		errno = save_errno;
		return -1;
	}
	fclose(file);
}

static INT64_T do_put_recursive( const char *source_file, const char *target_host, const char *target_file )
{
	int result;
	struct stat64 info;

	//printf("Putting %s onto %s at %s\n",source_file, target_host, target_file);

	result = lstat64(source_file,&info);
	if(result<0) {
		result = -1;
	} else {
		if(S_ISLNK(info.st_mode)) {
		    //printf("link %s\n",target_file);
			result = do_put_one_link(source_file,target_host,target_file);
		} else if(S_ISDIR(info.st_mode)) {
		    //printf("dir  %s\n",target_file);
			result = do_put_one_dir(source_file,target_host,target_file,0700);
		} else if(S_ISREG(info.st_mode)) {
		    //printf("file %s [%sB]\n",target_file,string_metric(info.st_size,-1,0));
			result = do_put_one_file(source_file,target_host,target_file,info.st_mode,info.st_size);
			//if( result >= 0 )
			//  xfer_bytes += result;
		} else {
		    //printf("???  %s\n",target_file);
			result = 0;
		}
		//if( result >= 0 )
		//  xfer_items++;
	}

	if(result<0) {
		printf("couldn't put %s: %s\n",source_file,strerror(errno));
	}

	return result;
}

struct ragged_array getsetarray(char *setdir) {
    char* setfile;
    struct ragged_array nullset = ragged_array_initialize(0);
    setfile = (char*) malloc((strlen(setdir)+1+strlen("set.list")+1)*sizeof(char));
    if(setfile == NULL) {fprintf(stderr,"Allocating set name failed!\n"); return nullset;}
    sprintf(setfile,"%s/set.list",setdir);


    return ragged_array_populate(setfile,setdir,strlen(setdir)+CHIRP_PATH_MAX);
}

int compare_entries( struct nvpair **a, struct nvpair **b )
{
	int result;
	const char *x, *y;

	x = nvpair_lookup_string(*a,"type");
	if(!x) x = "unknown";

	y = nvpair_lookup_string(*b,"type");
	if(!y) y = "unknown";

	result = strcmp(x,y);
	if(result!=0) return result;

        x = nvpair_lookup_string(*a,"name");
        if(!x) x = "unknown";

        y = nvpair_lookup_string(*b,"name");
        if(!y) y = "unknown";

        return strcmp(x,y);
}

struct ragged_array predist_hosts(double constraint) {

    struct catalog_query *q;
    struct nvpair *n;
    time_t timeout=60, stoptime;
    const char * catalog_host = 0;
    int i;
    int count=0;
    
    struct nvpair *table[10000];
    INT64_T minavail=constraint;

    struct ragged_array nullset;
    nullset.arr = NULL;
    nullset.row_count = 0;
    nullset.array_size = 0;

    struct ragged_array retset = ragged_array_initialize(10);
    if(retset.array_size == 0) {fprintf(stderr,"Allocating set failed!\n"); return nullset;}
    
    stoptime = time(0)+timeout;

    q = catalog_query_create(catalog_host,0,stoptime);
    if(!q) {
	fprintf(stderr,"couldn't query catalog: %s\n",strerror(errno));
	return nullset;
    }

    while((n = catalog_query_read(q,stoptime))) {
	table[count++] = n;
    }

    qsort(table,count,sizeof(*table),(void*)compare_entries);

    for(i=0;i<count;i++) {
	if(minavail!=0) {
	    if(minavail>nvpair_lookup_integer(table[i],"avail")) {
		continue;
	    }
	}

	const char *t = nvpair_lookup_string(table[i],"type");
	if(t && !strcmp(t,"chirp")) {
	    // if(1) { /*replace line below to access all nodes in chirp pool */
	    if(strstr(nvpair_lookup_string(table[i],"name"),"sc0-") != NULL) {
		if(ragged_array_add_line(&retset,nvpair_lookup_string(table[i],"name")) < 0) {
		    fprintf(stderr,"Allocating set[%i] failed!\n",retset.row_count+1);
		    return nullset;
		}
	    }
	}
    }
	
    return retset;
}

struct ragged_array postdist_hosts(FILE* fd) {

    int len = MAXRESULTLINE;
    char* line;
    
    struct ragged_array nullset;
    nullset.arr = NULL;
    nullset.row_count = 0;
    nullset.array_size = 0;

    struct ragged_array retset = ragged_array_initialize(10);
    if(retset.array_size == 0) {fprintf(stderr,"Allocating set failed!\n"); return nullset;}
    
    line = (char *) malloc(MAXFILENAME * sizeof(char));
    if(line == NULL) {fprintf(stderr,"Allocating line failed!\n"); return retset;}
    fgets(line, len, fd);
    if (line != NULL) {
	size_t last = strlen(line) - 1;
	if (line[last] == '\n') line[last] = '\0';
    }
    while(!feof(fd)) {
	if((strpos(line,'Y') == 0)&&(strpos(line,'E') == 1)&&(strpos(line,'S') == 2)&&(strpos(line,' ') == 3)) {// a YES verification
	    if(ragged_array_add_line(&retset,&(line[4])) < 0) {
		fprintf(stderr,"Allocating set[%i] failed!\n",retset.row_count+1);
		return nullset;
	    }
	}
	// else, do nothing, we just throw away the line.
	// get next line
	fgets(line, len, fd);
	if (line != NULL) {
	    size_t last = strlen(line) - 1;
	    if (line[last] == '\n') line[last] = '\0';
	}
    }

/*     teststr=`$parrot ls /chirp/$w/${wai}_${fname} 2> /dev/null | wc -l` # test that the data is actually there */
/*     if [ $teststr == $gcount ]; then  */
/* 	goodset=$goodset" "$w # add it to the list */
/* 	((gsc=$gsc+1)) */
/* 	echo "$gsc: $w $teststr"  */
/*     else #if not there, remove any aborted attempt on the chirp server */
/* 	$parrot rm -rf /chirp/$w/${wai}_${fname} */
/*     fi */

    return retset;
}

void msn_prefix(char* shortstr, char * longstr) {
    char* firstdot = NULL;
    strcpy(shortstr,longstr);
    firstdot = strchr(shortstr,'.');
    if(firstdot != NULL)
	firstdot[0]='\0';
}

double getTime() {
       struct timeval t;
       gettimeofday(&t,0);
       return t.tv_sec + t.tv_usec/1000000.0;
}

#define LOG2(x) ( log(x) / log(2) )

double findT(double n,double m,double t,double b,double s,double d, int c, int h) 
{

    if(h <= 0 || c <= 0 || h > 400 || (h*c) > (n*m) || (c*t) > limit) return -1;
    double T = ((((n*m)/c)*(d+(c*t)))/h) + (d*h) + ((((n+m)*s)/b)*LOG2(h));
    //printf("\tWith h:%i c:%i c*t: %.0f T:%.0f\n",h,c,c*t,T);
    return T;
}

int getbest(double arr[],double T)
{
    
    int i;
    double provT = T;
    int provi = -1;
    for(i=0;i<8;i++)
    {
	if(arr[i] > 0 && arr[i] < provT) {
	    provT=arr[i];
	    provi=i;
	}
    }
    return provi;
}

int getch(int *chosen_h, int *chosen_c, double *predicted_T, int n, int m, double t, double b, double s, double d) {

    double newchoices[8];
    int newindex;
    // pick initial h = 1
    int h=1;
    // pick initial c = 1 row
    int c=(int) m;
    limit=3600;
    double T=-1;
    int done;
    while(T == -1) {
    
	// find initial T.
	T=findT(n,m,t,b,s,d,c, h);

	done = 0;
	while(!done) {
	    newchoices[0]=findT(n,m,t,b,s,d,c+m, h);
	    newchoices[1]=findT(n,m,t,b,s,d,c-m, h);
	    newchoices[2]=findT(n,m,t,b,s,d,c+m, h+1);
	    newchoices[3]=findT(n,m,t,b,s,d,c-m, h+1);
	    newchoices[4]=findT(n,m,t,b,s,d,c+m, h-1);
	    newchoices[5]=findT(n,m,t,b,s,d,c-m, h-1);
	    newchoices[6]=findT(n,m,t,b,s,d,c, h+1);
	    newchoices[7]=findT(n,m,t,b,s,d,c, h-1);
	    newindex=getbest(newchoices,T);
	    if(newindex == -1)
		done=1;
	    else
	    {
		T=newchoices[newindex];
		switch(newindex) {
		case 0: c=c+m; break;
		case 1: c=c-m; break;
		case 2: c=c+m; h=h+1; break;
		case 3: c=c-m; h=h+1; break;
		case 4: c=c+m; h=h-1; break;
		case 5: c=c-m; h=h-1; break;
		case 6: h=h+1; break;
		case 7: h=h-1; break;
		default: printf("Error!\n"); exit(1);
		}
		//printf("Now at h:%i c:%i c*t: %.0f T:%.0f\n",h,c,c*t,T);
	    }
	}
	//printf("%i %i %.0f\n",h,c/n,T);
	limit*=2;
    }

    *chosen_h = h;
    *chosen_c = c;
    *predicted_T = T;

    return 0;
    
}

int getchLocal(double *predicted_T, int n, int m, double t) {

    double T = (n*m*t);
    *predicted_T = T;

    return 0;
   
}


int makeStatusScript(char* id, int numjobs)
{
    FILE* fp;
    char* filename = (char*) malloc((strlen(id)+strlen(".APStatus")+1)*sizeof(char));
    sprintf(filename,"allpairs_status.sh");
    fp = fopen(filename,"w");
    if(fp == NULL)
	return 1;
    fprintf(fp,"#!/bin/bash\nallpairs_status %s.logfile %i",id,numjobs);
    fclose(fp);
    return 0;
}

int makeWaitScript(char* id)
{
    FILE* fp;
    char* filename = (char*) malloc((strlen(id)+strlen(".APWait")+1)*sizeof(char));
    sprintf(filename,"allpairs_wait.sh");
    fp = fopen(filename,"w");
    if(fp == NULL)
	return 1;
    fprintf(fp,"#!/bin/bash\nallpairs_wait %s.logfile",id);
    fclose(fp);
    return 0;
}

int makeLocalCleanupScript(char* id, char* local_dir, char* mat_host, char* mat_path, char* fun_path)
{
    FILE* fp;
    char* filename = (char*) malloc((strlen(id)+strlen(".APCleanup")+strlen(".finalize")+1)*sizeof(char));
    sprintf(filename,"%s.finalize",id);
    fp = fopen(filename,"w");
    if(fp == NULL)
	return 1;
    fprintf(fp,"wID=%i %s\n",(int) strlen(id),id);
    fprintf(fp,"local_dir=%i %s\n",(int) strlen(local_dir),local_dir);
    fprintf(fp,"mat_host=%i %s\n",(int) strlen(mat_host), mat_host);
    fprintf(fp,"mat_path=%i %s\n",(int) strlen(mat_path), mat_path);
    if(fun_path[0])
	fprintf(fp,"fun_path=%i %s\n",(int) strlen(fun_path), fun_path);

    fclose(fp);

    return 0;
}

int makeRemoteCleanupScript(char* id, char* local_dir, char* mat_host, char* mat_path, char* remote_dir, char* node_list, char* hostname, char* fun_path)
{
    FILE* fp;
    char* filename = (char*) malloc((strlen(id)+strlen(".APCleanup")+strlen(".finalize")+1)*sizeof(char));
    sprintf(filename,"%s.finalize",id);
    fp = fopen(filename,"w");
    if(fp == NULL)
	return 1;
    fprintf(fp,"wID=%i %s\n",(int) strlen(id),id);
    fprintf(fp,"local_dir=%i %s\n",(int) strlen(local_dir),local_dir);
    fprintf(fp,"mat_host=%i %s\n",(int) strlen(mat_host), mat_host);
    fprintf(fp,"mat_path=%i %s\n",(int) strlen(mat_path), mat_path);
    fprintf(fp,"remote_dir=%i %s\n",(int) strlen(remote_dir), remote_dir);
    fprintf(fp,"node_list=%i %s\n",(int) strlen(node_list), node_list);
    fprintf(fp,"host=%i %s\n",(int) strlen(hostname), hostname);    
    if(fun_path[0])
	fprintf(fp,"fun_path=%i %s\n",(int) strlen(fun_path), fun_path);

    fclose(fp);

    return 0;
}

int main(int argc, char** argv)
{

    /****************************************************************************************
      main function Section 0
      General Declarations, Initializations, Environment, and Command Line Options
    ****************************************************************************************/
    
    int i; // multipurpose counters.
    signed char cl; // command line argument selector
    int retval; // multipurpose return value
    int did_explicit_auth = 0; // flag for whether the command line specified a particular authentication method
    int LOCALorREMOTE = -1; // flag for whether the command line specified forced local computation or remote submission

    int blacoord, blbcoord; // starting indices into the matrix of the first comparison (to allow for  potentially larger results super-matrix)
    int abase, bbase;  // starting indices into the set files
    int abaseend, bbaseend;  // ending indices into the set files
    blacoord = blbcoord = 0;
    abase = bbase = 0;
    abaseend = bbaseend = -1; 

    time_t timeout;

    /*
      declare and initialize the environment: hostname
    */
    char* hostname= (char*) malloc(MAXFILENAME*sizeof(char)); // allocate space for the source host.
    if(hostname == NULL) {
	fprintf(stderr,"Allocating hostname memory failed!\n");
	return 1;
    }
    char* addr= (char*) malloc(MAXFILENAME*sizeof(char)); // allocate space for the source host's IP address.
    if(addr == NULL) {
	fprintf(stderr,"Allocating IP address  memory failed!\n");
	return 1;
    }
    gethostname(hostname,MAXFILENAME); // get hostname, this may not have domain name, though!
    if(hostname == NULL) { // if that failed
	fprintf(stderr,"Could not get hostname!\n");
	return 2;
    }
    domain_name_lookup( hostname, addr ); // so get the IP address of the hostname.
    if (addr != NULL) { // if that worked
	domain_name_lookup_reverse(addr, hostname); // do a canonical reverse lookup that will include the domain name
    }
    else //otherwise, give a warning.
    { fprintf(stderr, "Warning: no IP information. Hostname (%s) may not have a domain name!\n", hostname); }
    free(addr);
    addr=NULL;
    
    /*
      declare and initialize the environment: userid
    */
    struct passwd *pw = NULL; // a password file structure
    uid_t uid;        // a userid container
    uid = geteuid(); // get the current uid
    pw = getpwuid(uid); // pass the current uid to fill the password structure
    if (!pw) // if the password structure wasn't populated: couldn't get username.
    {
	printf("getpwuid() failed. Could not determine username!\n");
	return 3;
    }
    //postcondition: pw->pw_name contains the userid of the user executing this program.

    
    int local_prefix_chosen = 0; // flag for whether the location of local state is specified
    char* local_prefix = (char*) malloc(CHIRP_PATH_MAX*sizeof(char));     // /var/tmp for example ... where to place local state?
    if(local_prefix == NULL) {
	fprintf(stderr,"Allocating local prefix path string  memory failed!\n");
	return 1;
    }
    
    int matrix_host_chosen = 0;  // flag for whether the host of the remote matrix is specified
    char* matrix_host = (char*) malloc(CHIRP_PATH_MAX*sizeof(char)); // a hostname
    if(matrix_host == NULL) {
	fprintf(stderr,"Allocating matrix host string  memory failed!\n");
	return 1;
    }
    
    char* dateString =  (char*) malloc(6*sizeof(char)); // a string to store a date of the form MMMdd
    if(dateString == NULL) {
	fprintf(stderr,"Allocating date string  memory failed!\n");
	return 1;
    }
    if(getDateString(dateString) != 1) { // Store the current date, sub in a default value if this fails 
	fprintf(stderr,"Warning, getting date failed. Jan0 will be used instead.\n");
	strcpy(dateString,"Jan00");
    }

    int matrix_path_chosen = 0;  // flag for whether the path of the remote matrix is specified
    char* matrix_path = (char*) malloc(CHIRP_PATH_MAX*sizeof(char)); // a path
    if(matrix_path == NULL) {
	fprintf(stderr,"Allocating matrix path string  memory failed!\n");
	return 1;
    }
    /*
      Get and process command line options and arguments
    */
    while((cl=getopt(argc,argv,"+a:d:t:LRx:y:p:q:r:s:l:H:P:hv")) > -1) {
	switch(cl) {
	case 'a':
	    auth_register_byname(optarg);
	    did_explicit_auth = 1;
	    break;
	case 'd':
	    debug_flags_set(optarg);
	    break;
	case 't':
	    timeout = string_time_parse(optarg);
	    break;
	case 'L': // force LOCAL execution
	    if(LOCALorREMOTE == 1) {
		printf("Cannot have -L and -R!\n");
		exit(1);
	    }
	    LOCALorREMOTE=0;
	    break;
	case 'R': // force REMOTE submission
	    if(LOCALorREMOTE == 0) {
		printf("Cannot have -L and -R!\n");
		exit(1);
	    }
	    LOCALorREMOTE=1;
	    break;
	case 'x':
	    blbcoord=atoi(optarg);
	    break;
	case 'y':
	    blacoord=atoi(optarg);
	    break;
	case 'p':
	    abase=atoi(optarg);
	    break;
	case 'q':
	    bbase=atoi(optarg);
	    break;
	case 'r':
	    abaseend=atoi(optarg);
	    break;
	case 's':
	    bbaseend=atoi(optarg);
	    break;
	case 'l': // explicitly choose location for local state
	    local_prefix_chosen = 1;
	    strcpy(local_prefix,optarg);
	    break;
	case 'H':
	    matrix_host_chosen = 1; // explicitly choose host for remote matrix metadata
	    strcpy(matrix_host,optarg);
	    break;
	case 'P':
	    matrix_path_chosen = 1; // explicitly choose path for remote matrix metadata
	    strcpy(matrix_path,optarg);
	    break;
	case 'h':
	    show_help(argv[0]);
	    exit(0);
	    break;
	case 'v':
	    cctools_version_print(stdout, argv[0]);
	    exit(0);
	    break;
	}
    }

	cctools_version_debug(D_DEBUG, argv[0]);

    /*
      Declare and initialize indices for where on the command line certain required arguments are.
      Initialize default values for options that were not explicitly chosen.
    */
    int base_index = optind -1 ;
    int setA_index = base_index+1;       // the name of the set file for set 1
    int setB_index = base_index+2;       // the name of the set file for set 2
    int funcdir_index = base_index+3;    // the directory defining the function
    int workloadID_index = base_index+4;      // the workload name identifier
    
    
    if(local_prefix_chosen == 0) // if the local state wasn't specified via command line argument
	sprintf(local_prefix,"/tmp/%s/",argv[workloadID_index]); // default value: /tmp/WORKLOADID
    for(i=1; i < strlen(local_prefix); i++)  // create the prefix hierarchy as necessary
	if(local_prefix[i] == '/') {
	    local_prefix[i] = '\0';
	    mkdir(local_prefix, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH); 
	    local_prefix[i] = '/';
	}
    
    if(matrix_host_chosen == 0) // if the remote matrix host wasn't specified via command line argument
	strcpy(matrix_host, "sc0-00.cse.nd.edu"); // default value: a well-known ND host -- sc0-00.cse.nd.edu
    
    if(matrix_path_chosen == 0) // if the remote matrix path wasn't specified via command line argument
	sprintf(matrix_path,"%s/matrixmeta/%s_%s_%s",pw->pw_name,hostname,dateString,argv[workloadID_index]);// default value: /USERID/matrixmeta/HOSTNAME_DATE_WORKLOADID
    
    if(matrix_host_chosen != matrix_path_chosen) // if one of the two matrix properties was specified, warn the user, as thy may have made a mistake
	fprintf(stderr,"Warning: Only one of output host or output path was specified. The matrix metadata will be stored at: /chirp/%s/%s\n",matrix_host,matrix_path);
    
    if(argc != optind+4) {
	fprintf(stderr,"After all options, there must be the names of the two sets, the directory defining the function, and the workload ID.\n");
	show_help(argv[0]);
	return 4;
    }

    
    // FIXME: other checks: prefix dir must already exist.
    stoptime = time(0) + 3600; // initial stoptime for general operations: 1 hour 
    debug_config(argv[0]); // indicate what string to use as the executable name when printing debugging information
    if(!did_explicit_auth) auth_register_all(); // if an authentication mechanism wasn't chosen, default register all.

    char starting_directory[CHIRP_PATH_MAX];
    getcwd(starting_directory,sizeof(starting_directory));

    /****************************************************************************************
      main function Section 1
      Read sets in to ragged arrays, measure data to determine total set size
      Determine function directory and function name
      Create Matrix using proper environments, etc. or default values.
    ****************************************************************************************/
    
    struct stat64 abuf; // a buffer for stat calls
    double tsize = 0.0; // total size of a set
    double asize = 0.0; // size of a set element
    
    // read aset into ragged array
    struct ragged_array setA = getsetarray(argv[setA_index]);
    if(setA.arr == NULL) { // if no lines in the array
	fprintf(stderr,"Error reading setA!\n");
	return 5;
    }
    if(abase >= setA.row_count || abaseend >= setA.row_count) {
	fprintf(stderr,"Error: -p (%i) or -r (%i) argument larger than size of set %s (%i)!\n", abase, abaseend, argv[setA_index], setA.row_count);
	return 5;
    }
    if(abaseend == -1)
	abaseend = setA.row_count - 1;
    const int WL_HEIGHT = abaseend - abase + 1; 
    // Get the total/average size of the items.
    for(i=abase; i <= abaseend; i++) {
	stat64(setA.arr[i], &abuf); // stat each item
	tsize+=abuf.st_size; // add each item's size to the total size
    }
    

    // read bset into array
    struct ragged_array setB = getsetarray(argv[setB_index]);
    if(setA.arr == NULL) {  // if no lines in the array
	fprintf(stderr,"Error reading setB!\n");
	return 6;
    }
    if(bbase >= setB.row_count || bbaseend >= setB.row_count) {
	fprintf(stderr,"Error: -p (%i) or -r (%i) argument larger than size of set %s (%i)!\n", bbase, bbaseend, argv[setB_index], setB.row_count);
	return 6;
    }
    if(bbaseend == -1)
	bbaseend = setB.row_count - 1;
    const int WL_WIDTH = bbaseend - bbase + 1; 
    // Get the total/average size of the items.
    for(i=bbase; i <= bbaseend; i++) {
	stat64(setB.arr[i], &abuf);  // stat each item
	tsize+=abuf.st_size; // add each item's size to the total size
    }
    asize = tsize/(WL_WIDTH+WL_HEIGHT);
    //printf("Average item size: %f bytes = %f Mb\n",asize,((asize * 8)/(1024*1024)));

    /*
      for(i = 0; i < setA.row_count; i++) {
	printf("%i: %s (%i)\n", i, setA.arr[i], (int) strlen(setA.arr[i]));
      }
      for(i = 0; i < setB.row_count; i++) {
	printf("%i: %s (%i)\n", i, setB.arr[i], (int) strlen(setB.arr[i]));
      }
    */

    /* Echo the geometry for general use. */
    printf("GEOMETRY (x,y): %i %i\n",WL_WIDTH,WL_HEIGHT);


    char* function_directory = (char *) malloc((strlen(argv[funcdir_index])+1)*sizeof(char)); // allocate space for function directory string
    if(function_directory == NULL) {
	fprintf(stderr,"Allocating function_directory string memory failed!\n");
	return 1;
    }
    char* func_name = (char *) malloc((strlen(argv[funcdir_index])+1+4)*sizeof(char)); // allocate space for function name string
    if(func_name == NULL) {
	fprintf(stderr,"Allocating func_name string memory failed!\n");
	return 1;
    }
    char* fq_func_name = (char *) malloc((strlen(argv[funcdir_index])+1+4)*sizeof(char)); // allocate space for function name string
    if(fq_func_name == NULL) {
	fprintf(stderr,"Allocating fq_func_name string memory failed!\n");
	return 1;
    }
    
    short INTERNAL_FUNCTION;
    
    if( stat64(argv[funcdir_index], &abuf) ) { // no such directory ... must be an internal function, copy it exactly.
	INTERNAL_FUNCTION=1;
	strcpy(fq_func_name,argv[funcdir_index]);
	strcpy(func_name,argv[funcdir_index]);
	free(function_directory);
	function_directory = NULL;
    }
    else {
	// funcdir exists ... not an internal function
	INTERNAL_FUNCTION=0;
	/* determine function directory and strip any ending /s off end of directory */
	strcpy(function_directory, argv[funcdir_index]); // copy function directory string from command line argument into allocated string
	while(strlen(function_directory) > 0 && function_directory[strlen(function_directory)-1] == '/') // for all trailing slashes
	    function_directory[strlen(function_directory)-1] = '\0'; // change the trailing slash to a null byte.
	/* determine function name (without the path) */
	int last_slash_index = strrpos(function_directory,'/'); // determine position of the final slash in function directory
	if(last_slash_index == -1) // if no slash, function name IS the function directory name  
	    strcpy(func_name,function_directory);
	else // otherwise, function name is what comes after the final slash. Ex: quux in /foo/bar/baz/quux 
	    strcpy(func_name,&(function_directory[last_slash_index+1]));
	strcat(func_name,".exe"); //quux.exe

	sprintf(fq_func_name,"%s/%s",function_directory,func_name); // /foo/bar/baz/quux/quux.exe
    }
   
	
    /* Determine Chirp Servers to Use for Matrix and Create Matrix*/
    struct chirp_matrix *mat = NULL;
    mat=chirp_matrix_open( matrix_host, matrix_path, stoptime);
    if(mat == NULL)
    {
	char host_file[CHIRP_LINE_MAX];
	if(getenv("CHIRP_HOSTS")) {
	    sprintf(host_file,"%s",getenv("CHIRP_HOSTS"));
	    fprintf(stderr,"Using CHIRP_HOSTS -> %s\n",host_file);
	}
	else {
	    if(getenv("HOME")) {
		sprintf(host_file,"%s/.chirp/",getenv("HOME"));
		if( stat64(host_file, &abuf) ) { // NO .chirp directory defined.
		    fprintf(stderr,"Making .chirp directory\n");
		    if(mkdir(host_file, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)) {       // make that directory
			fprintf(stderr,"mkdir failed to make: %s,using './chirp_matrix_hosts'\n",host_file);
			sprintf(host_file,"./chirp_matrix_hosts");
		    }
		    else {
			sprintf(host_file,"%s/.chirp/chirp_hosts",getenv("HOME"));
			fprintf(stderr,"mkdir succeeded, using %s\n",host_file);
		    }
		}
		else {
		    sprintf(host_file,"%s/.chirp/chirp_hosts",getenv("HOME"));
		    fprintf(stderr,"HOME/.chirp was defined. Using %s\n",host_file);
		}
	    }
	    else {
		sprintf(host_file,"./chirp_hosts");
		fprintf(stderr,"HOME undefined, using %s\n",host_file);
	    }
	}
	
	if(stat64(host_file, &abuf)) { // it's not defined.
	    fprintf(stderr,"%s doesn't exist, creating it\n",host_file);
	    FILE* hfp = fopen(host_file,"w");
	    if(hfp == NULL) {
		fprintf(stderr, "Couldn't determine or assert matrix hosts.\n");
		return 7;
	    }
	    fprintf(hfp,"sc0-00.cse.nd.edu\n");
	    fprintf(hfp,"sc0-01.cse.nd.edu\n");
	    fprintf(hfp,"sc0-02.cse.nd.edu\n");
	    fprintf(hfp,"sc0-03.cse.nd.edu\n");
	    fclose(hfp);
	}

	fprintf(stderr,"Creating matrix: %s %s %i %i\n",matrix_host,matrix_path,blbcoord+WL_WIDTH,blacoord+WL_HEIGHT);
	mat=chirp_matrix_create( matrix_host, matrix_path, blbcoord+WL_WIDTH, blacoord+WL_HEIGHT, 8, 1, stoptime);
	if(mat == NULL) {
	    fprintf(stderr, "Couldn't create matrix.\n");
	    return 8;
	}
	int rv = chirp_matrix_setacl(matrix_host,matrix_path,"hostname:*.nd.edu","rwl",stoptime);
	if(rv < 0)
	{
	    fprintf(stderr, "Couldn't set matrix ACLs.\n");
	    return 9;
	}
    }
    else {
	int rv = chirp_matrix_setacl(matrix_host,matrix_path,"hostname:*.nd.edu","rwl",stoptime);
	if(rv < 0)
	{
	    fprintf(stderr, "Couldn't set matrix ACLs.\n");
	    return 10;
	}
	chirp_matrix_close(mat, stoptime);
    }
        

    /****************************************************************************************
      main function Section 2
      Handle local execution if explicitly specified.
      Do a benchmark using local execution to fill in function time for the model.
      Handle local execution time if called for by the model.
    ****************************************************************************************/

    char* localexecute = NULL;
    printf("allpairs_multicore -w %i -i %i -X %i -Y %i -p %i -q %i -r %i -s %i %s %s %s %s %s\n",blbcoord+WL_WIDTH,blacoord+WL_HEIGHT,blbcoord,blacoord,abase,bbase,abaseend,bbaseend,argv[setA_index],argv[setB_index],fq_func_name,matrix_host,matrix_path);
    fflush(stdout);
    if(LOCALorREMOTE==0) { // if LOCAL execution was explicitly specified
	// run local tool
	printf("FORCE LOCAL!\n\n");
	localexecute = (char *) xxmalloc(1024*sizeof(char));
	if(localexecute == NULL) {
	    fprintf(stderr,"Allocating local execution string memory failed!\n");
	    return 1;
	}
	sprintf(localexecute,"allpairs_multicore -w %i -i %i -X %i -Y %i -p %i -q %i -r %i -s %i %s %s %s %s %s",blbcoord+WL_WIDTH,blacoord+WL_HEIGHT,blbcoord,blacoord,abase,bbase,abaseend,bbaseend,argv[setA_index],argv[setB_index],fq_func_name,matrix_host,matrix_path);
	int retval;
	retval = system(localexecute);
	exit(retval);
    }
    

    char full_function_directory[CHIRP_PATH_MAX];
    if(INTERNAL_FUNCTION == 0) {
	/* change to the function directory */
	chdir(function_directory);
        getcwd(full_function_directory,sizeof(full_function_directory));
    } else {
        full_function_directory[0] = 0;
    }


    /* # This is function dependent: getch n m t b s d */
    /* # n - the number of setA elements */
    /* # m - the number of setB elements */
    /* # t - the time to complete one comparison (1s for Tim's Faces, <.05 for Karen's Irises) */
    /* # b - the bandwidth of the network, in Mbps (100 or 1000) */
    /* # s - the size of each setA/setB element in Mb (10 for Tim's 1.25MB faces, .16 for Karen's 20KB templates) */
    /* # d - the dispatch time for the batch system (10 seconds is v. v. conservative) */

    double func_time, bandwidth, element_size, dispatch;
    double bench_start, bench_end;

    /* Determine the function execution time with a 16 cell benchmark using the local tool*/
    /* FIXME: doesn't work for multi-file functions */
    chdir(starting_directory);
    localexecute = (char *) malloc(6*CHIRP_PATH_MAX*sizeof(char));
    if(localexecute == NULL) {
	fprintf(stderr,"Allocating local execution string memory failed!\n");
	return 1;
    }

    sprintf(localexecute,"allpairs_multicore -w %i -i %i -X %i -Y %i -p %i -q %i -r %i -s %i %s %s %s %s %s",blbcoord+WL_WIDTH,blacoord+WL_HEIGHT,blbcoord,blacoord,abase,bbase,abase+3,bbase+3,argv[setA_index],argv[setB_index],fq_func_name,matrix_host,matrix_path);
    
    bench_start = getTime(); // get the starting mark
    retval = system(localexecute);
    bench_end = getTime(); // get the starting mark
    if(retval != 0) {
	fprintf(stderr,"Benchmarking run failed with exit status %i.\nBenchmark run was:\n%s\n",retval,localexecute);
	return 11;
    }
    free(localexecute);
    localexecute=NULL;
    if(full_function_directory[0]) // if not an internal function, move back to the function directory.
	chdir(full_function_directory);
    
    
    
    func_time = (bench_end-bench_start)/16;
    fprintf(stderr,"Function time: %lf\n",func_time);
    if(func_time < .001) func_time = .001;
    bandwidth = 1000;
    element_size = ((asize * 8)/(1024*1024));
    dispatch = 10;
    
    
    int h, c;
    double T,TL;
    getch(&h, &c, &T, WL_HEIGHT, WL_WIDTH, func_time, bandwidth, element_size, dispatch);

    /* GET NUMBER OF ROWS PER JOB
    int apj;
    if(c%setB.row_count == 0)
	apj = c/setB.row_count;
    else
	apj = (c/setB.row_count) + 1;
    */

    /* GET BOX SIZE PER JOB*/
    int apj = (int) sqrt(c);
    apj++; // casting on the line above truncates, incrementing "overassigns" amount of work to do per job, rather than underassigning.
	
    getchLocal(&TL, WL_HEIGHT, WL_WIDTH, func_time);

    fprintf(stderr,"H: %i\nCPJ: %i\nAPJ: %i\nRT: %.2f\nLT: %.2f\n",h,c,apj,T,TL);
    
    
    if( (LOCALorREMOTE!=1) && (TL <= T)) {
	// run local tool
	fprintf(stderr,"Local execution chosen (%.2f < %.2f)\n",TL,T);
	chdir(starting_directory);
	makeLocalCleanupScript(argv[workloadID_index],local_prefix,matrix_host,matrix_path,full_function_directory);
	char* localexecute = (char *) malloc(CHIRP_PATH_MAX*sizeof(char));
	if(localexecute == NULL) {
	    fprintf(stderr,"Allocating local execution string memory failed!\n");
	    return 1;
	}
	sprintf(localexecute,"allpairs_multicore -w %i -i %i -X %i -Y %i -p %i -q %i -r %i -s %i %s %s %s %s %s",blbcoord+WL_WIDTH,blacoord+WL_HEIGHT,blbcoord,blacoord,abase,bbase,abaseend,bbaseend,argv[setA_index],argv[setB_index],fq_func_name,matrix_host,matrix_path);
	// note: can change abase and bbase to abase+4 and bbase+4 because the benchmarking already did these squares
	retval = system(localexecute);
	exit(retval);
    }
    fprintf(stderr,"Remote execution chosen (%.2f > %.2f)\n",TL,T);


    /****************************************************************************************
      main function Section 3
      Copy the two sets onto the local chirp server
      Distribute the sets from the local chirp server to the pool
      Determine which servers have the data 
    ****************************************************************************************/

    int chirp_acl_string_len = strlen(hostname)+strlen("hostname:*.nd.edu"); // determine maximum length of an ACL identity
    char* chirp_acl_string = (char*) malloc(chirp_acl_string_len*sizeof(char)); // allocate space for the ACL identity
    if(chirp_acl_string == NULL) {
	fprintf(stderr,"Allocating chirp acl string memory failed!\n");
	return 1;
    }
    char* chirp_dirname = (char *) malloc(sizeof(char)*(strlen(pw->pw_name)+strlen(argv[workloadID_index])+6+2)); // allocate space for the chirp directory name string
    if(chirp_dirname == NULL) {
	fprintf(stderr,"Allocating chirp directory name string memory failed!\n");
	return 1;
    }
    
    sprintf(chirp_dirname,"/%s_%s",pw->pw_name,argv[workloadID_index]); // initialize the directory name string to the "parent" directory: USERID_WORKLOADID
    printf("Chirp_dirname:%s\n",chirp_dirname);
    chirp_reli_mkdir(hostname,chirp_dirname,0700,stoptime); // create the parent directory

    // now do two set subdirectories
    // first subdir
    sprintf(chirp_dirname,"/%s_%s/%s",pw->pw_name,argv[workloadID_index],"set1");
    //printf("Chirp_dirname:%s\n",chirp_dirname);
    // copy setA directory to local chirp server.
    do_put_recursive( argv[setA_index], hostname, chirp_dirname);
    // set acl for that directory on local chirp server
    sprintf(chirp_acl_string,"hostname:%s", hostname);
    chirp_reli_setacl(hostname,chirp_dirname,chirp_acl_string,"rwlda",stoptime);
    strcpy(chirp_acl_string,"hostname:*.nd.edu");
    chirp_reli_setacl(hostname,chirp_dirname,chirp_acl_string,"rl",stoptime);
    strcpy(chirp_acl_string,"system:localuser");
    chirp_reli_setacl(hostname,chirp_dirname,chirp_acl_string,"rl",stoptime);

    if(strcmp(argv[setA_index],argv[setB_index])) {
	// second subdir
	sprintf(chirp_dirname,"/%s_%s/%s",pw->pw_name,argv[workloadID_index],"set2");
	//printf("Chirp_dirname:%s\n",chirp_dirname);
	// copy setB directory to local chirp server.
	do_put_recursive( argv[setB_index], hostname, chirp_dirname);
	// set acl for that directory on local chirp server
	sprintf(chirp_acl_string,"hostname:%s", hostname);
	chirp_reli_setacl(hostname,chirp_dirname,chirp_acl_string,"rwlda",stoptime);
	strcpy(chirp_acl_string,"hostname:*.nd.edu");
	chirp_reli_setacl(hostname,chirp_dirname,chirp_acl_string,"rl",stoptime);
	strcpy(chirp_acl_string,"system:localuser");
	chirp_reli_setacl(hostname,chirp_dirname,chirp_acl_string,"rl",stoptime);
    }

    sprintf(chirp_dirname,"/%s_%s",pw->pw_name,argv[workloadID_index]);
    //printf("Chirp_dirname:%s\n",chirp_dirname);
    // set acl for the parent directory on local chirp server
    sprintf(chirp_acl_string,"hostname:%s", hostname);
    chirp_reli_setacl(hostname,chirp_dirname,chirp_acl_string,"rwlda",stoptime);
    strcpy(chirp_acl_string,"hostname:*.nd.edu");
    chirp_reli_setacl(hostname,chirp_dirname,chirp_acl_string,"rl",stoptime);
    strcpy(chirp_acl_string,"system:localuser");
    chirp_reli_setacl(hostname,chirp_dirname,chirp_acl_string,"rl",stoptime);
    

    // distribute, starting from the the parent directory, to hosts in the hostset.
    // determine the available hosts from the catalogue
    struct ragged_array available_hosts = predist_hosts(tsize);

    // prepare to allocate the string for all available hosts
    int host_set_string_length = 0;
    for(i = 0; i < available_hosts.row_count; i++)
	host_set_string_length+=(strlen(available_hosts.arr[i])+2);

    // allocate the string for all available hosts
    char *host_set_string = (char *) malloc(host_set_string_length * sizeof(char));
    if(host_set_string == NULL) {
	    fprintf(stderr,"Allocating host set string memory failed!\n");
	    return 1;
	}
    // fill the string for all available hosts (space delimited).
    strcpy(host_set_string,available_hosts.arr[0]);
    for(i = 1; i < available_hosts.row_count; i++) {
	strcat(host_set_string," ");
	strcat(host_set_string,available_hosts.arr[i]);
    }

    makeRemoteCleanupScript(argv[workloadID_index],local_prefix,matrix_host,matrix_path,chirp_dirname, host_set_string,hostname,full_function_directory);

    // set timeouts for the distribution
    int bigT = 7200;
    int smallT = 600;

    // allocate and build the distribute command.
    // command will be used as a general purpose string throughout.
    char *command = (char *) malloc(strlen("chirp_distribute -a hostname -D -Y -N ")+10+strlen(" -T NNNNN -t NNNNN ")+strlen(hostname)+1+strlen(pw->pw_name)+1+strlen(argv[workloadID_index])+1+strlen(host_set_string)+1);
    if(command == NULL) {
	fprintf(stderr,"Allocating distribute command string memory failed!\n");
	return 1;
    }
    sprintf(command,"chirp_distribute -a hostname -D -Y -N %i -T %i -t %i %s %s %s",h,bigT,smallT,hostname,chirp_dirname,host_set_string);
    printf("%s\n",command);

    // Finally actually do the distribution.
    FILE* cd_pipe=popen(command,"r");

    // determine the actual set of nodes to which the sets were distributed
    struct ragged_array goodset = postdist_hosts(cd_pipe);
    printf("Distribution Complete to %i Nodes\n",goodset.row_count);
    free(command); // free the command string
    command = NULL; // reset the command string

    if(goodset.row_count == 0)
    {
	fprintf(stderr,"Did not distribute to any nodes! Cannot build jobs!\n");
	return 12;
    }

    /****************************************************************************************
      main function Section 4
      Build strings to specify lists of prestaged hosts
      If not internal function:
      Create function tarball of all items necessary to actually complete the comparison 
    ****************************************************************************************/
    // Build string of nodes with prestaged data -- one forward, one backward. Each has a limit of 2047 characters.
    char* shortstring = (char *) malloc(2048*sizeof(char));
     if(shortstring == NULL) {
	fprintf(stderr,"Allocating MachineShortName string memory failed!\n");
	return 1;
    }
    char* goodstring1 = (char *) malloc(2048*sizeof(char)); // forward list of all prestaged hosts
    if(goodstring1 == NULL) {
	fprintf(stderr,"Allocating forward list string memory failed!\n");
	return 1;
    }
    goodstring1[0]='\0';
    for(i = 0; i < goodset.row_count; i++) {
	msn_prefix(shortstring,goodset.arr[i]);
	if((strlen(goodstring1)+strlen(shortstring)+1)<2047) {
	    strcat(goodstring1,shortstring);
	    strcat(goodstring1,",");
	}
    }
    
    char* goodstring2 = (char *) malloc(2048*sizeof(char)); // backward list of all prestaged hosts
    if(goodstring2 == NULL) {
	fprintf(stderr,"Allocating backward list string memory failed!\n");
	return 1;
    }
    goodstring2[0]='\0';
    for(i = goodset.row_count-1; i >= 0; i--) {
	msn_prefix(shortstring,goodset.arr[i]);
	if((strlen(goodstring2)+strlen(shortstring)+1)<2047) {
	    strcat(goodstring2,shortstring);
	    strcat(goodstring2,",");
	}
    }

    fprintf(stderr,"GS1: %s\n",goodstring1);
    fprintf(stderr,"GS2: %s\n",goodstring2);


    
    char* goodstring = (char *) malloc(2048*sizeof(char)); // allocate string for list of hosts (may not be full list due to space constraints)
    char* reqstring = (char *) malloc(2048*sizeof(char));  // allocate string for entire requirements clause
    char* reqclose = (char *) malloc(2048*sizeof(char));   // allocate string for post-hosts requirements
    char* tokenstring = (char *) malloc(2048*sizeof(char));   // allocate string for tokenizing 
    char* nexthost; // pointer to the next host
        
    if(goodstring == NULL || reqstring == NULL || reqclose == NULL || tokenstring == NULL) {
	fprintf(stderr,"Allocating requirements string memory failed!\n");
	return 1;
    }

    if(full_function_directory[0]) {
	// create a file called exclude.list to be used by tar to indicate items not to be included in tarball
	FILE* excludep = fopen("exclude.list","w");
	if(!excludep) {
	    fprintf(stderr,"Could not open exclusion list for writing.\n");
	    return 13;
	}
	fprintf(excludep,"exclude.list\n"); // write the exclude list into itself. Duh.
	fprintf(excludep,"%s.func.tar\n",argv[workloadID_index]); // write the function tarball itself into the exclude list.
	fclose(excludep);
	
	
	// allocate and fill strings for command to create the function tarball, then do it
	command = (char *) malloc((strlen("tar -X exclude.list -f   .func.tar -r ./* 2> /dev/null")+strlen(argv[workloadID_index]))*sizeof(char));
	if(command == NULL) {
	    fprintf(stderr,"Allocating tar command string memory failed!\n");
	    return 1;
	}
	sprintf(command,"tar -X exclude.list -f %s.func.tar -r ./* 2> /dev/null",argv[workloadID_index]);
	system(command); // actually create the function tarball
	free(command); // free the command string
	command = NULL; // reset the command string 
    }


    /****************************************************************************************
      main function Section 5
      Create a local directory for each job.
      Measure the disk requirements to include in the submit file requirements
      Create the submit file, including custom list of hosts for requirements limits
      Actually Submit Job!!!
    ****************************************************************************************/
    
    int a, b; // counters for setA (a) and setB (b)
    //int bpj = WL_WIDTH; // items in set 2 per job ... initialize as 1 full row.
    int bpj = apj; // square box.
    char* job_directory = (char *) malloc((strlen(local_prefix) + 1 + strlen(argv[workloadID_index]) + 1 + 8 + 1 + 8)*sizeof(char)); // string for the directory for a given job
    int ffdl;
    if(full_function_directory[0])
	ffdl = strlen(full_function_directory);
    else
	ffdl = 1;
    command = (char *) malloc((ffdl+strlen(argv[workloadID_index])+strlen("condor_submit .submit")+MAXFILENAME)*sizeof(char)); // general purpose string

     if(job_directory == NULL) {
	fprintf(stderr,"Allocating job directory string memory failed!\n");
	return 1;
    }

    if(command == NULL) {
	fprintf(stderr,"Allocating condor_submit command string memory failed!\n");
	return 1;
    }

    int rsl; // length of requirements string.

    int token_counter = 0;
    int jobcount = 0;
    int jobdircount = -1;
   
    // for each job, build a submit file and submit the job
    for(a=abase; a<=abaseend; a+=apj) { // for each element in set a, incrementing by the number of those elements per job
	for(b=bbase; b<=bbaseend; b+=bpj) { // for each element in set b, incrementing by the number of those elements per job

	    if(jobcount % 10000 == 0) {
		jobdircount++;
		sprintf(job_directory,"%s/%i/",local_prefix, jobdircount); // initialize the name of the job superdirectory: PREFIX/COUNT/
		mkdir(job_directory, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);       // make that directory
	    }
	    
	    sprintf(job_directory,"%s/%i/%s.%i.%i",local_prefix, jobdircount, argv[workloadID_index], a , b); // initialize the name of the job directory: PREFIX/COUNT/WORKLOADID.A.B
	    mkdir(job_directory, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);       // make that directory

	    INT64_T diskreq = 0;
	    // determine the disk requirements
	    if(full_function_directory[0]) {
		sprintf(command,"%s/%s.func.tar",full_function_directory,argv[workloadID_index]);
		stat64(command, &abuf); // measure the size of the function tarball
		diskreq = abuf.st_size; // initialize the overall disk requirement to the size of the function tarball, as this is really all that's transferred
		diskreq /= 1000; // convert to KB.
	    }
	    
	    diskreq += 100000; //100MB cushion for safety and output and what have you
	    //printf("dr:%lld ; dr2: %lld\n",diskreq,diskreq2);
	    
	    // move to the newly created job directory
	    chdir(job_directory);
	    
	    // create a file called WORKLOADID.submit and begin filling it.
	    sprintf(command,"%s.submit",argv[workloadID_index]);
	    FILE *subp = fopen(command,"w");
	    fprintf(subp,"universe = vanilla\n"); // set condor universe
	    fprintf(subp,"executable = %s/allpairs_wrapper.sh\n",starting_directory); // set executable -- it's a wrapper.
	    if(strcmp(argv[setA_index],argv[setB_index])) { // if setA != setB
		fprintf(subp,"arguments = %s %s /chirp/localhost/%s/set1 /chirp/localhost/%s/set2 %s %s %i %i %i %i %i %i %i %i\n",argv[workloadID_index],func_name,chirp_dirname,chirp_dirname,matrix_host,matrix_path,blacoord+WL_HEIGHT,blbcoord+WL_WIDTH,blacoord,blbcoord,a,b,MIN((a+apj-1),abaseend),MIN((b+bpj-1),bbaseend)); 
	    }
	    else { // setA == SetB, so we only distributed one copy.
		fprintf(subp,"arguments = %s %s /chirp/localhost/%s/set1 /chirp/localhost/%s/set1 %s %s %i %i %i %i %i %i %i %i\n",argv[workloadID_index],func_name,chirp_dirname,chirp_dirname,matrix_host,matrix_path,blacoord+WL_HEIGHT,blbcoord+WL_WIDTH,blacoord,blbcoord,a,b,MIN((a+apj-1),abaseend),MIN((b+bpj-1),bbaseend)); 
	    }
	    // set first "third" of requirements string
	    /*
	    //Lines below allows jobs to run on either VM/Slot 1 or 2; this is disabled when using Li's multicore functionality.
	    sprintf(reqstring,"Requirements = (Arch==\"INTEL\" || Arch == \"X86_64\") && (Disk > %lld) && (Memory >= 450) && (MachineGroup != \"itm\") && (Machine != \"%s\") && ( (VirtualMachineID == 1) || ((VirtualMachineID == 2) && (Disk > %lld)) ) && ( stringListIMember(MachineShortName, \"",diskreq,hostname,diskreq2);
	    INT64_T diskreq2 = diskreq*2; // double the requirement for dual-processor machines.
	    */
	    sprintf(reqstring,"Requirements = (Arch==\"INTEL\" || Arch == \"X86_64\") && (Disk > %lld) && (Memory >= 450) && (MachineGroup != \"itm\") && (Machine != \"%s\") && ( (VirtualMachineID == 1))  && ( stringListIMember(MachineShortName, \"",diskreq,hostname);

	    sprintf(reqclose,"\") )\n"); // set third "third" of requirements string
	    rsl = strlen(reqstring)+2+strlen(reqclose); // measure the length of the requirements string's 1st and 3rd parts
	    
	    if(token_counter++%2 == 0)  // if "forward" job
		strcpy(tokenstring,goodstring1);
	    else
		strcpy(tokenstring,goodstring2);
	    
	    goodstring[0]='\0';
	    nexthost = strtok(tokenstring,","); // go through "forward" full set until we exhaust the character limit.
	    while(nexthost != NULL && ((strlen(nexthost)+1+rsl)<2046)) {
		//printf("Goodstring:%s\n",goodstring);
		if(goodstring[0] != '\0')
		    strcat(goodstring,",");
		strcat(goodstring,nexthost);
		nexthost = strtok(NULL,",");
	    }
	    nexthost = NULL;
	
	    printf("Goodstring:%s\n",goodstring);
	    strcat(reqstring,goodstring); // build full reqstring from three thirds.
	    strcat(reqstring,reqclose);
	    fprintf(subp,"%s\n",reqstring); // print it to the submit file
	    
	    // finish off the submit file
	    fprintf(subp, "Rank = Memory\n");
	    if(full_function_directory[0])
		fprintf(subp, "transfer_input_files = %s/%s.func.tar, %s/allpairs_multicore\n",full_function_directory,argv[workloadID_index],starting_directory);
	    else
		fprintf(subp, "transfer_input_files = %s/allpairs_multicore\n",starting_directory);
	    fprintf(subp, "output = %s.output\n",argv[workloadID_index]);
	    fprintf(subp, "error = %s.error\n",argv[workloadID_index]);
	    fprintf(subp, "transfer_files = always\n");
	    fprintf(subp, "log = %s/%s.logfile\n",starting_directory,argv[workloadID_index]);
	    fprintf(subp, "on_exit_remove = (ExitBySignal == False) && (ExitCode == 0)\n");
	    fprintf(subp, "notification = never\n");
	    fprintf(subp, "getenv = true\n");
	    fprintf(subp, "queue\n");
	    
	    fclose(subp); // close the submit file
	    
	    sprintf(command,"condor_submit %s.submit",argv[workloadID_index]); // build job submission command
	    system(command); // FINALLY SUBMIT JOB!
	    printf("Cluster: %i:%i %i:%i\n",a,b,MIN((a+apj-1),abaseend),MIN((b+bpj-1),bbaseend)); // print job submission notification
	    jobcount++; // increment total number of jobs
	    chdir(starting_directory); // change back to starting directory -- not really necessary.
	    if(full_function_directory[0])
		chdir(full_function_directory); // change to the fully qualified function directory to go again.
	}
    }
	
    chdir(starting_directory);
    makeStatusScript(argv[workloadID_index], jobcount);
    makeWaitScript(argv[workloadID_index]);

    return 0;

}


/* # Due to a bug with condor_wait, we don't actually clean up here, rather just create a script that will do it for us when we run it manually. */
/* echo "#!/bin/bash */
/* #Cleaning up: */
/* dir=$dir */
/* execdir=$execdir */
/* fname=$fname */
/* #for((p=0; $p<$pcount; p=$p+$ppj)); do */
/* #    cat $prefix/$fname.$p.$g/$fname.error >> $dir/$fname.error */
/* #    cat $prefix/$fname.$p.$g/$fname.output >> $dir/$fname.output */
/* #done */
/* #cp $dir/$fname.output $dir/$fname.results */
/* rm -rf $prefix/$fname.* */
/* cd $dir */
/* rm $execdir/exclude.list */
/* rm $execdir/$fname.func.tar */
/* rm $execdir/$fname.gal */
/* rm -rf $gallery_location$gallery_dirname/ */
/* rm -rf $prefix/working/$fname/ */
/* echo \"Cleaning up distributed copies\" */
/* chirp_distribute -X -R `hostname` `whoami`_$fname $goodset */
/* $parrot rm -rf /chirp/`hostname`/`whoami`_$fname */
/* " > $dir/$fname.finalize */
/* chmod u+x $dir/$fname.finalize */




/*     echo "notification = never" >> $fname.submit # don't e-tase me, bro. */
/*     echo "queue" >> $fname.submit  */
    
/*     submitout=`condor_submit $fname.submit` #Go! */
/*     echo $submitout */
/*     cluster=${submitout##*cluster } */
/*     cluster=${cluster%%\.} */
/*     echo "CLUSTER $cluster $p:$g $p:$((gcount-1))" #append to the output to be a bit more descriptive for this actual application */
/*     ((jobcount=$jobcount+1)) */
/*     cd $dir */
/*     cd $execdir */
/* done */
/* #if we got here, we've submitted all the jobs; now we must just do a condor_wait and then cleanup */



/* ((cmpcount=$pcount*$gcount)) */
/* echo "$jobcount total jobs to complete $cmpcount comparisons" */
/* for((j=1; $j<=$jobcount;j=$j+1)); do */
/*     condor_wait -num $j $dir/$fname.logfile */
/* done */

/* condor_wait -num $jobcount $dir/$fname.logfile #wait again, just for the fun of it. */


/* vim: set noexpandtab tabstop=4: */
