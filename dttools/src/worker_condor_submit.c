/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MAX_HOSTNAME_LENGTH 256
#define MAX_FILENAME_LENGTH 1024


void print_usage() {
    printf("Usage:\nworker_condor_submit [options] #Workers MasterHostname MasterPort [MachineGroup] [MachineGroup] ...\n");
    printf("-x: Use only x86_64 machines for workers. (Default: use both 32-bit and 64-bit machines)\n");
    printf("-o: Include output stream file specification in Condor submit file. (Default: worker's stdout comes back in .debug file)\n");
    printf("-e: Include error stream file specification in Condor submit file. (Default: worker's stderr is discarded)\n");
    printf("-y [file]: Use only machines listed in given file for workers. (Default: do not restrict, except by given MachineGroups)\n");
}

int main(int argc, char** argv) {

    int i;
    int include_output = 0;
    int include_error = 0;
    int only_x86 = 0;
    char opt_file[MAX_FILENAME_LENGTH] = "";
    char c;
    
    

    while((c=getopt(argc,argv,"xoey:"))!=(char)-1) {
	switch(c) {
	case 'x':
	    only_x86 = 1;
	    break;
	case 'o':
	    include_output = 1;
	    break;
	case 'e':
	    include_error = 1;
	    break;
	case 'y':
	    strcpy(opt_file,optarg);
	    break;
	default:
	    print_usage();
	    exit(0);
	    break;
	}
    }

    if( (argc-optind)<3) {
	print_usage();
	exit(1);
    }
    
    int jobs;
    jobs = atoi(argv[optind]);
    
    char hostname[MAX_HOSTNAME_LENGTH];
    strcpy (hostname, argv[optind+1]);

    int port;
    port = atoi(argv[optind+2]);

    

    FILE *subp = fopen("workers.submit","w");
    fprintf(subp,"universe = vanilla\n"); // set condor universe
    fprintf(subp,"executable = worker\n"); // set executable; it's a worker 
    fprintf(subp,"arguments = -o worker.$(PROCESS).debug %s %i\n",hostname,port);

    // set architecture portion of requirements string
    char reqstring[4096];
    if(only_x86)
	sprintf(reqstring,"Requirements = (Arch == \"X86_64\") ");
    else
	sprintf(reqstring,"Requirements = (Arch==\"INTEL\" || Arch == \"X86_64\") ");
    
    // set group portion of requirements string
    if((argc-optind) > 3) {
	char reqclose[10];
	int rsl;

	char groupstring[4096] = "";
	char goodstring[4096] = "";

	strcat(reqstring,"&& ( stringListIMember(MachineGroup, \"");



	for(i = optind+3; i < argc; i++) {
	    strcat(groupstring,argv[i]);
	    strcat(groupstring,",");
	}
	groupstring[strlen(groupstring)-1] = '\0';
	sprintf(reqclose,"\") )"); // set string to close off stringListIMember
	rsl = strlen(reqstring)+2+strlen(reqclose); // measure the length of the requirements string's architecture part and stringListIMember's remaining requirement
	
	char* nextgroup = strtok(groupstring,","); // go through given groups until we exhaust the character limit.
	while(nextgroup != NULL && ((strlen(nextgroup)+1+rsl)<2046)) {
	    if(goodstring[0] != '\0')
		strcat(goodstring,",");
	    strcat(goodstring,nextgroup);
	    nextgroup = strtok(NULL,",");
	}
	nextgroup = NULL;
	
	strcat(reqstring,goodstring); // add group requirements to full requirements
	strcat(reqstring,reqclose);
    }

    // set hosts portion of requirements string
    if(strcmp(opt_file,"")) {
	char reqclose[10];
	int rsl;

	FILE* yfd;
	yfd = fopen(opt_file,"r");
	if(yfd) {
	    char MSNstring[4096] = "";
	    char goodstring[4096] = "";
	    char line[4096] = "";
	    strcat(reqstring,"&& ( stringListIMember(MachineShortName, \"");
	    
	    
	    //open file, read all items into MSNstring
	    
	    while(fscanf(yfd,"%s",line) == 1) {
		strcat(MSNstring,line);
		strcat(MSNstring,",");
	    }
	    MSNstring[strlen(MSNstring)-1] = '\0';
	    sprintf(reqclose,"\") )"); // set string to close off stringListIMember
	    rsl = strlen(reqstring)+2+strlen(reqclose); // measure the length of the requirements string's architecture and group parts and stringListIMember's remaining requirement
	    
	    char* nexthost = strtok(MSNstring,","); // go through given hosts until we exhaust the character limit.
	    while(nexthost != NULL && ((strlen(nexthost)+1+rsl)<2046)) {
		if(goodstring[0] != '\0')
		    strcat(goodstring,",");
		strcat(goodstring,nexthost);
		nexthost = strtok(NULL,",");
	    }
	    nexthost = NULL;
	    
	    strcat(reqstring,goodstring); // add host requirements to full requirements
	    strcat(reqstring,reqclose);
	}
	else {
	    printf("Could not open given host file %s\n",opt_file);
	}
    }

   
    fprintf(subp,"%s\n",reqstring); // print it to the submit file

    // finish off the submit file
    if(include_output)
	fprintf(subp, "output = worker.$(PROCESS).output\n");
    if(include_error)
	fprintf(subp, "error = worker.$(PROCESS).error\n");
    fprintf(subp, "transfer_files = always\n");
    fprintf(subp, "transfer_output_files = worker.$(PROCESS).debug\n");
    fprintf(subp, "+JobMaxSuspendTime = 10\n");
    fprintf(subp, "log = worker.$(PROCESS).logfile\n");
    fprintf(subp, "notification = never\n");
    fprintf(subp, "queue %i\n", jobs);
    
    fclose(subp); // close the submit file

    return system("condor_submit workers.submit"); // FINALLY SUBMIT JOBS!
}
