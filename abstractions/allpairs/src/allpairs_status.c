#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "chirp_protocol.h"

void printUsage(char* cmd) {
	printf("Use: %s [options] <condor log file> <total number of jobs>\n", cmd);
}

int main(int argc, char** argv) {
    if(argc < 3) {
	printUsage(argv[0]);
	return 1;
    }
    FILE *statusPipe;
    char* command = (char *) malloc((strlen("condor_userlog_job_counter   ")+strlen(argv[1]))*sizeof(char));
    char event[256];
    int j, k;
    int tjobs = atoi(argv[2]);
        
    sprintf(command,"condor_userlog_job_counter %s",argv[1]); 
    statusPipe = popen(command, "r");
    if(statusPipe == NULL) {fprintf(stderr,"Popen failed!\n"); return 1;}
    
    while(!feof(statusPipe)) {
	//fscanf(fname,"%c",&in);
	k = fscanf(statusPipe, " Log event: %s",event);
	if(k != 1) {
	    k=fscanf(statusPipe," outcome: %s",event);
	    if(k != 1)
		printf("Failed string1: %s\n",event);
	    else
		;//printf("Successful string1: %s\n",event);
	}
	else
	    ;//printf("Successful string1: %s\n",event);
	if(strcmp(event,"ULOG_NO_EVENT") != 0) {
	    k=fscanf(statusPipe," Queued Jobs: %d",&j);
	    if(k != 1) {
		fscanf(statusPipe,"%s",event);
		printf("Failed string2: %s\n",event);
	    }
	    else {
		;//printf("Successful string2: %d\n",j);
	    }
	}
	else
	    break;
    }

    if(j < 0) {
	printf("Warning: %i more jobs have finished than started! (%.2f%% of total jobs)\n",-j,-100*((float)j/tjobs));
    }
    else {
	printf("%i (%.2f%%) jobs have not finished.\n",j,100*((float)j/tjobs));
    }
    return 0;
}
