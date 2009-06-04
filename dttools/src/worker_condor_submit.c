#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_HOSTNAME_LENGTH 256

void print_usage() {
    printf("Usage:\nworker_condor_submit #Workers MasterHostname MasterPort [MachineGroup] [MachineGroup] ...\n");
}

int main(int argc, char** argv) {

    int i;
    
    if(argc < 4) {
	print_usage();
	exit(1);
    }

    int jobs;
    jobs = atoi(argv[1]);
    
    char hostname[MAX_HOSTNAME_LENGTH];
    strcpy (hostname, argv[2]);

    int port;
    port = atoi(argv[3]);

    

    FILE *subp = fopen("workers.submit","w");
    fprintf(subp,"universe = vanilla\n"); // set condor universe
    fprintf(subp,"executable = worker\n"); // set executable; it's a worker 
    fprintf(subp,"arguments = -o worker.$(PROCESS).debug %s %i\n",hostname,port);

    // set first "third" of requirements string
    char reqstring[4096];
    sprintf(reqstring,"Requirements = (Arch==\"INTEL\" || Arch == \"X86_64\") ");

    if(argc > 4) {
	char reqclose[10];
	int rsl;
  
	char groupstring[4096] = "";
	char goodstring[4096] = "";

	strcat(reqstring,"&& ( stringListIMember(MachineGroup, \"");



	for(i = 4; i < argc; i++) {
	    strcat(groupstring,argv[i]);
	    strcat(groupstring,",");
	}
	groupstring[strlen(groupstring)-1] = '\0';
	sprintf(reqclose,"\") )"); // set third "third" of requirements string
	rsl = strlen(reqstring)+2+strlen(reqclose); // measure the length of the requirements string's 1st and 3rd parts
	
	char* nextgroup = strtok(groupstring,","); // go through "forward" full set until we exhaust the character limit.
	while(nextgroup != NULL && ((strlen(nextgroup)+1+rsl)<2046)) {
	    if(goodstring[0] != '\0')
		strcat(goodstring,",");
	    strcat(goodstring,nextgroup);
	    nextgroup = strtok(NULL,",");
	}
	nextgroup = NULL;
	
	strcat(reqstring,goodstring); // build full reqstring from three thirds.
	strcat(reqstring,reqclose);
    }

    fprintf(subp,"%s\n",reqstring); // print it to the submit file

    // finish off the submit file
    fprintf(subp, "#output = worker.$(PROCESS).output\n");
    fprintf(subp, "#error = worker.$(PROCESS).error\n");
    fprintf(subp, "transfer_files = always\n");
    fprintf(subp, "transfer_output_files = worker.$(PROCESS).debug\n");
    fprintf(subp, "+JobMaxSuspendTime = 10\n");
    fprintf(subp, "log = worker.$(PROCESS).logfile\n");
    fprintf(subp, "notification = never\n");
    fprintf(subp, "queue %i\n", jobs);
    
    fclose(subp); // close the submit file

    return system("condor_submit workers.submit"); // FINALLY SUBMIT JOBS!
}
