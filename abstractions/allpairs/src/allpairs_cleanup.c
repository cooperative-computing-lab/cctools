#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "auth_all.h"
#include "debug.h"
#include "chirp_matrix.h"

void printUsage(char* cmd) {
	printf("Use: %s <options> -F [finalize file]",cmd);
        printf(" where options are:\n");
	printf(" -a <mode>      Explicit authentication mode.\n");
	printf(" -d <subsystem> Enable debugging for this subsystem.  (Try -d all to start.)\n");
	//printf(" -t <string>    Timeout, e.g. 60s\n");
	//printf(" -D             Download Results Matrix.\n");
	printf(" -R             Remove remote state.\n");
	printf(" -L             Remove local state.\n");
	printf(" -M             Remove results matrix.\n");
	printf(" -h             Show this help screen\n");
}

int main(int argc, char** argv) {
    char cl;
    int did_explicit_auth = 0;
    int download,rm_local,rm_remote,rm_mat,file_provided;
    char finalize_file[CHIRP_PATH_MAX];
    
    download=rm_local=rm_remote=rm_mat=file_provided=0;

    while((cl=getopt(argc,argv,"+a:d:hDLRMF:"))!=(char)-1) {
	switch(cl) {
	case 'a':
	    auth_register_byname(optarg);
	    did_explicit_auth = 1;
	    break;
	case 'd':
	    debug_flags_set(optarg);
	    break;
	case 'h':
		printUsage(argv[0]);
		exit(0);
		break;
	case 'D': // download matrix data to local disk
	    download=1;
	    break;
	case 'L': // force LOCAL state removal
	    rm_local=1;
	    break;
	case 'R': // force REMOTE state removal (chirp_distribute -X)
	    rm_remote=1;
	    break;
	case 'M': // force REMOTE MATRIX state removal 
	    rm_mat=1;
	    break; 
	case 'F':
	    file_provided=1;
	    strcpy(finalize_file,optarg);
	    break;
	}
    }

    if(!file_provided) {
	fprintf(stderr,"Please provide argument -F [finalize file]\n");
	printUsage(argv[0]);
	exit(1);
    }

     if(!did_explicit_auth) auth_register_all(); // if an authentication mechanism wasn't chosen, default register all.
     debug_config(argv[0]); // indicate what string to use as the executable name when printing debugging information
    // first, parse finalize file to get information.
    char* cmd;
    char* wID;
    char* local_dir;
    char* mat_host;
    char* mat_path;
    char* remote_dir;
    char* node_list;
    char* hn;
    char* fun_path;
    int strlentmp;
    FILE* fp = fopen(finalize_file,"r");
    if(!fp) {
	fprintf(stderr,"Finalize file not readable.\n");
	exit(1);
    }

    // 0th item is workload id
    if(fscanf(fp, " wID=%i ",&strlentmp) == 1) {
	wID = malloc(strlentmp*sizeof(char));
	if(!wID)
	{
	    fprintf(stderr,"Could not allocate %i bytes for workload ID\n",strlentmp);
	    exit(1);
	}
	if(fscanf(fp, " %s ", wID) != 1) {
	    fprintf(stderr,"Could not read in workload ID\n");
	    exit(2);
	}
    }
    
    
    // first item is local prefix -- remove everything.
    if(fscanf(fp, " local_dir=%i ",&strlentmp) == 1) {
	local_dir = (char*) malloc(strlentmp*sizeof(char));
	if(!local_dir)
	{
	    fprintf(stderr,"Could not allocate %i bytes for local directory\n",strlentmp);
	    exit(1);
	}
	if(fscanf(fp," %s ", local_dir) != 1) {
	    fprintf(stderr,"Could not read in local directory\n");
	    exit(2);
	}
    }

    // second item is matrix host -\ remove
    // third item is matrix path  -/ matrix
    if(fscanf(fp, " mat_host=%i ",&strlentmp) == 1) {
	mat_host = (char *) malloc(strlentmp*sizeof(char));
	if(!mat_host)
	{
	    fprintf(stderr,"Could not allocate %i bytes for matrix host\n",strlentmp);
	    exit(1);
	}
	if(fscanf(fp," %s ", mat_host) != 1) {
	    fprintf(stderr,"Could not read in matrix host\n");
	    exit(2);
	}
    }

    if(fscanf(fp, " mat_path=%i ",&strlentmp) == 1) {
	mat_path = (char *) malloc(strlentmp*sizeof(char));
	if(!mat_path)
	{
	    fprintf(stderr,"Could not allocate %i bytes for matrix path\n",strlentmp);
	    exit(1);
	}
	if(fscanf(fp," %s ", mat_path) != 1) {
	    fprintf(stderr,"Could not read in matrix path\n");
	    exit(2);
	}
    }
    

    // 4th item is chirp_dirname
    if(fscanf(fp, " remote_dir=%i ",&strlentmp) == 1) {
	remote_dir = (char *) malloc(strlentmp*sizeof(char));
	if(!remote_dir)
	{
	    fprintf(stderr,"Could not allocate %i bytes for remote path\n",strlentmp);
	    exit(1);
	}
	if(fscanf(fp," %s ", remote_dir) != 1) {
	    fprintf(stderr,"Could not read in remote path\n");
	    exit(2);
	}
    }
     
    // 7th item is full goodstring
    if(fscanf(fp, " node_list=%i ",&strlentmp) == 1) {
	node_list = (char *) malloc(strlentmp*sizeof(char));
	if(!node_list)
	{
	    fprintf(stderr,"Could not allocate %i bytes for remote hosts\n",strlentmp);
	    exit(1);
	}
	if(fread(node_list,1,strlentmp,fp) != strlentmp) {
	    fprintf(stderr,"Could not read in remote hosts\n");
	    exit(2);
	}
    }

    // 9th item is hostname
    if(fscanf(fp, " host=%i ",&strlentmp) == 1) {
	hn = malloc(strlentmp*sizeof(char));
	if(!hn)
	{
	    fprintf(stderr,"Could not allocate %i bytes for hostname\n",strlentmp);
	    exit(1);
	}
	if(fscanf(fp, " %s ", hn) != 1) {
	    fprintf(stderr,"Could not read in hostname\n");
	    exit(2);
	}
    }
    
    // 10th item is full function directory -- remove tarball, exception list
    if(fscanf(fp, " fun_path=%i ",&strlentmp) == 1) {
	fun_path = (char *) malloc(strlentmp*sizeof(char));
	if(!fun_path)
	{
	    fprintf(stderr,"Could not allocate %i bytes for function directory\n",strlentmp);
	    exit(1);
	}
	if(fscanf(fp," %s ", fun_path) != 1) {
	    fprintf(stderr,"Could not read in function directory\n");
	    exit(2);
	}
	cmd = (char *) malloc((strlen(fun_path)+strlen("rm -f ")+strlen(wID)+strlen(".func.tar")+12)*sizeof(char));
	if(!cmd)
	{
	    fprintf(stderr,"Could not allocate memory for command\n");
	    exit(1);
	}
	sprintf(cmd,"rm -f %s/%s.func.tar",fun_path,wID);
	if(system(cmd)) { fprintf(stderr,"Could not remove %s/%s.func.tar\n",fun_path,wID); exit(1);}
	sprintf(cmd,"rm -f %s/exclude.list",fun_path);
	if(system(cmd)) { fprintf(stderr,"Could not remove %s/exclude.list\n",fun_path); exit(1);}
	free(cmd);
	cmd=NULL;
	
    }
    else {
	// internal function
	fun_path = NULL;
    }
    
    
    // end parsing finalize file
    
    // next, download if desired.
    if(download) {
	fprintf(stderr,"Download Matrix Mode\n");
	
    }
    // next, delete remote state if desired.
    if(rm_remote) {
	fprintf(stderr,"Remove Remote State Mode\n");
	cmd = (char *) malloc(strlen("chirp_distribute -a hostname -X ")+10+(2*strlen(hn))+1+strlen(remote_dir)+1+strlen(node_list)+1);
	if(cmd == NULL) {
	    fprintf(stderr,"Allocating distribute command string memory failed!\n");
	    return 1;
	}
	sprintf(cmd,"chirp_distribute -a hostname -X %s %s %s",hn,remote_dir,node_list);
	debug(D_CHIRP,"%s\n",cmd);
	system(cmd);
	sprintf(cmd,"chirp_distribute -a hostname -X %s %s %s",hn,remote_dir,hn);
	debug(D_CHIRP,"%s\n",cmd);
	system(cmd);
	free(cmd);
	cmd=NULL;
    }
    // next, delete matrix if desired.
     if(rm_mat) {
	fprintf(stderr,"Remove Matrix State Mode\n");
	chirp_matrix_delete( mat_host, mat_path, time(0)+600 );
	
    }
    // next, delete local if desired.
     if(rm_local) {
	 fprintf(stderr,"Remove Local State Mode\n");
	 cmd = (char *) malloc(strlen("rm -rf ")+1+strlen(local_dir)+1);
	 if(cmd == NULL) {
	     fprintf(stderr,"Allocating distribute command string memory failed!\n");
	     return 1;
	 }
	 sprintf(cmd,"rm -rf %s",local_dir);
	 system(cmd);
	 free(cmd);
	 cmd=NULL;
     }

     return 0;
    
}
