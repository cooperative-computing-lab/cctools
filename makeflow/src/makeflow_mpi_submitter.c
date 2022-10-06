/*
 * Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 * */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "hash_table.h"
#include "jx.h"
#include "jx_parse.h"
#include "getopt.h"
#include "getopt_aux.h"
#include "stringtools.h"
#include "list.h"
#include "xxmalloc.h"
#include <unistd.h>

static const struct option long_options[] = {
    {"makeflow-arguments", required_argument, 0, 'm'},
    {"workqueue-arguments", required_argument, 0, 'q'},
    {"makeflow-port", required_argument, 0, 'p'},
    {"slots", required_argument, 0, 'w'},
    {"max-submits", required_argument, 0, 'W'},
    {"email", required_argument, 0, 'e'},
    {"queue", required_argument, 0, 'u'},
    {"help", no_argument, 0, 'h'},
    {"mpi-name", required_argument, 0, 'n'},
    {"config-file", required_argument, 0, 'C'},
    {"mpi-module", required_argument, 0, 'o'},
    {"type", required_argument, 0, 'T'},
    {"cores-per-worker", required_argument, 0, 'c'},
    {"memory", required_argument, 0, 'M'},
    {"disk", required_argument,0,'D'},
    {"disk-location",required_argument,0,'S'},
    {"time-limit",required_argument,0,'t'},
    {"copy-out",required_argument,0,'O'},
    {"makeflow", no_argument, 0, 'K'},
    {0, 0, 0, 0}
};

void print_help() {
    printf("usage: mpi_submitter [options]\n");
    printf(" -K,--makeflow                    Use Makeflow -T mpi instead of mpi_starter or mpi_worker\n");
    printf(" -m,--makeflow-arguments          Options to pass to makeflow manager\n");
    printf(" -q,--workqueue-arguments         Options to pass to work_queue_workers\n");
    printf(" -p,--makeflow-port               The port to set the makeflow manager to use\n");
    printf(" -w,--slots                       How many Slots per-submission\n");
    printf(" -W,--max-submits                 Maximum number of submissions to do\n");
    printf(" -c,--cores-per-worker            How many cores per worker on each node submitted\n");
    printf(" -M,--memory                      How much memory per worker on each node submitted\n");
    printf(" -D,--disk                        How much disk space to use on each node submitted\n");
    printf(" -S,--disk-location               Root location for scratch space\n");
    printf(" -e,--email                       Email for submitting to TORQUE or SGE\n");
    printf(" -u,--queue                       Queue name being submitted to on SGE\n");
    printf(" -n,--mpi-name                    The MPI queue being submitted to\n");
    printf(" -C,--config-file                 A JSON representation of the configurations needed, instead of needing to pass in command line options\n");
    printf(" -o,--mpi-module                  MPI module name to load before running `mpirun`\n");
    printf(" -T,--type                        sge, torque, or slurm \n");
    printf(" -t,--time-limit                  Sets a time limit for the job in the queue\n");
    printf(" -O,--copy-out                    Location for makeflow to copy out created files");


    printf("\n\n");
    printf(" -h,--help                        Prints out this list\n");
}

union mpi_submitter_ccl_guid {
    char c[8];
    unsigned int ul;
};

static unsigned gen_guid() {
    FILE* ran = fopen("/dev/urandom", "r");
    if (!ran) {
        fprintf(stderr, "Cannot open /dev/urandom");
        exit(1);
    }
    union mpi_submitter_ccl_guid guid;
    size_t k = fread(guid.c, sizeof (char), 8, ran);
    if (k < 8) {
        fprintf(stderr, "couldn't read 8 bytes from /dev/urandom/");
        exit(1);
    }
    fclose(ran);
    return guid.ul;
}

char* generate_job_name() {
    char* guid_s = string_format("%u",gen_guid());
    size_t i=0, k=0;
    char temp[256];
    memset(temp,'\0',256);
    for(i=0; i<strlen(guid_s); i++){
        temp[k++]=(guid_s[i] - '0') + 'A';
    }
    free(guid_s);
    return string_format("mpi_submitter_job_%s", temp);
}

char* random_filename() {
    return string_format("mpi_submitter_submit_file_%u", gen_guid());
}

int getnum(char* out) {
    size_t i = 0, k=0;
    char big[1000];
    memset(big,'\0',1000);
    for (i = 0; i < strlen(out); i++) {
        if (out[i] >= '0' && out[i] <= '9') {
            big[k++] = out[i];
        }
    }
    return atoi(big);

}

void create_sge_file(char* fileout, struct jx* options) {
    FILE* fout = fopen(fileout, "w+");
    if (!fout) {
        fprintf(stderr, "cannot open given file: %s", fileout);
        exit(1);
    }

    char* binary = "";
    char* makeflow_options = "";
    char* workqueue_options = "";

    if (jx_lookup_string(options, "makeflow-arguments") != NULL) {
        makeflow_options = string_format("%s", jx_lookup_string(options, "makeflow-arguments"));
        binary = "mpi_starter";
        if (jx_lookup_integer(options, "use-makeflow-mpi")) {
            binary = "makeflow -T mpi";
        }
        if (jx_lookup_string(options, "workqueue-arguments") != NULL) {
            workqueue_options = string_format("%s", jx_lookup_string(options, "workqueue-arguments"));
        }
    } else {
        binary = "mpi_worker";
        if (jx_lookup_string(options, "workqueue-arguments") != NULL) {
            workqueue_options = (char*) jx_lookup_string(options, "workqueue-arguments");
        }
    }
    
    if(jx_lookup_integer(options,"memory") != 0) {
        workqueue_options = string_format("--memory=%i %s", (int) jx_lookup_integer(options, "memory"), workqueue_options);
        if (strstr("makeflow -T mpi", binary)) {
            makeflow_options = string_format("--mpi-memory=%i %s", (int) jx_lookup_integer(options, "memory"), makeflow_options);
        }
    }
    if (jx_lookup_string(options, "disk") != NULL) {
        workqueue_options = string_format("--disk=%s %s", jx_lookup_string(options, "disk"), workqueue_options);
    }
    if(jx_lookup_string(options,"disk-location")!=NULL){
        workqueue_options = string_format("--workdir=%s %s",jx_lookup_string(options,"disk-location"),workqueue_options);
    }
    if(jx_lookup_integer(options, "cores-per-worker") != 0){
        if (strstr("makeflow -T mpi", binary)) {
            makeflow_options = string_format("--mpi-cores=%i %s", (int) jx_lookup_integer(options, "cores-per-worker"), makeflow_options);
        }
    }

    fprintf(fout, "#!/bin/csh\n\n");
    if (jx_lookup_string(options, "email") != NULL) {
        fprintf(fout, "#$ -M %s\n", jx_lookup_string(options, "email")); //email
        fprintf(fout, "#$ -m abe\n");
    }

    fprintf(fout, "#$ -pe %s %i\n", jx_lookup_string(options, "mpi-name"), (int) jx_lookup_integer(options, "slots")); //number of processes, and assume I can do this line, basically Xp
    fprintf(fout, "#$ -q %s\n", jx_lookup_string(options, "queue")); //queuename
    fprintf(fout, "#$ -N %s\n", generate_job_name()); //job name

    if (jx_lookup_string(options, "mpi-module") != NULL) {
        fprintf(fout, "module load %s\n", jx_lookup_string(options, "mpi-module")); //assume this works
    }
    
    if(strlen(workqueue_options)>0 && !strstr("mpi_worker",binary)){
        workqueue_options = string_format("-q \"%s\"",workqueue_options);
    }
    if(strlen(makeflow_options)>0 && !(strstr("mpi_worker",binary) || strstr("makeflow -T mpi",binary))){
        makeflow_options = string_format("-m \"%s\"",makeflow_options);
    }
    
    if(strstr("makeflow -T mpi",binary)){
        workqueue_options = "";
    }
    
    fprintf(stderr,"makeflow options: %s\nworkqueue options: %s\n",makeflow_options,workqueue_options);

    if (jx_lookup_integer(options, "cores-per-worker") != 0) {
        fprintf(fout, "setenv MPI_WORKER_CORES_PER %i\n", (int)jx_lookup_integer(options, "cores-per-worker"));
        fprintf(fout, "mpirun -npernode 1 %s %s %s\n", binary, makeflow_options, workqueue_options);
    } else {
        fprintf(fout, "mpirun -np $NSLOTS %s %s %s\n", binary, makeflow_options, workqueue_options);
    }

    fclose(fout);
}

void create_slurm_file(char* fileout, struct jx* options) {
    FILE* fout = fopen(fileout, "w+");
    
    char* binary = "";
    char* makeflow_options = "";
    char* workqueue_options = "";
    char* cpout = (char*)jx_lookup_string(options,"copy-out");

    if (jx_lookup_string(options, "makeflow-arguments") != NULL) {
        makeflow_options = string_format("%s", jx_lookup_string(options, "makeflow-arguments"));
        binary = "mpi_starter";
        if (jx_lookup_integer(options, "use-makeflow-mpi")) {
            binary = "makeflow -T mpi";
        }
        if (jx_lookup_string(options, "workqueue-arguments") != NULL) {
            workqueue_options = string_format("%s", jx_lookup_string(options, "workqueue-arguments"));
        }
    } else {
        binary = "mpi_worker";
        if (jx_lookup_string(options, "workqueue-arguments") != NULL) {
            workqueue_options = (char*) jx_lookup_string(options, "workqueue-arguments");
        }
    }
    
    if(jx_lookup_integer(options,"memory") != 0){
        workqueue_options = string_format("--memory=%i %s",(int)jx_lookup_integer(options,"memory"),workqueue_options);
        if (strstr("makeflow -T mpi", binary)) {
            makeflow_options = string_format("--mpi-memory=%i %s", (int) jx_lookup_integer(options, "memory"), makeflow_options);
        }
    }
    if(jx_lookup_string(options,"disk")!=NULL){
        workqueue_options = string_format("--disk=%s %s",jx_lookup_string(options,"disk"),workqueue_options);
    }
    if(jx_lookup_string(options,"disk-location")!=NULL){
        workqueue_options = string_format("--workdir=%s %s",jx_lookup_string(options,"disk-location"),workqueue_options);
    }
    
    fprintf(fout, "#!/bin/sh\n\n");
    
    fprintf(fout,"#SBATCH --job-name=%s\n",generate_job_name());
    fprintf(fout,"#SBATCH --partition=%s\n",jx_lookup_string(options,"mpi-name"));
    
    if (jx_lookup_integer(options, "cores-per-worker") != 0) {
        fprintf(fout,"#SBATCH --cpus-per-task=%i\n",(int)jx_lookup_integer(options, "cores-per-worker"));
        workqueue_options = string_format("--cores=%i %s",(int)jx_lookup_integer(options,"cores-per-worker"),workqueue_options);
        if (strstr("makeflow -T mpi", binary)) {
            makeflow_options = string_format("--mpi-cores=%i %s", (int) jx_lookup_integer(options, "cores-per-worker"), makeflow_options);
        }
    }
    
    fprintf(fout,"#SBATCH --ntasks=%i\n",(int)jx_lookup_integer(options, "slots"));
    
    if(jx_lookup_integer(options,"memory") != 0 && jx_lookup_integer(options, "cores-per-worker") != 0){
        int mem = (int)jx_lookup_integer(options,"memory")/(int)jx_lookup_integer(options, "cores-per-worker");
        fprintf(fout,"#SBATCH --mem-per-cpu=%i\n",mem);
    }
    
    if(jx_lookup_string(options,"time-limit") != NULL){
        fprintf(fout,"#SBATCH -t %s\n",jx_lookup_string(options,"time-limit"));
    }
    
    
    if (jx_lookup_string(options, "mpi-module") != NULL) {
        fprintf(fout, "module load %s\n", jx_lookup_string(options, "mpi-module")); //assume this works
    }
    
    if(strlen(workqueue_options)>0 && !strstr("mpi_worker",binary)){
        workqueue_options = string_format("-q \"%s\"",workqueue_options);
    }
    if(strlen(makeflow_options)>0 && !(strstr("mpi_worker",binary) || strstr("makeflow -T mpi",binary))){
        makeflow_options = string_format("-m \"%s\"",makeflow_options);
    }
    if(strstr("makeflow -T mpi",binary)){
        workqueue_options = "";
    }
    
    
    if(cpout != NULL){
        fprintf(fout, "mpirun %s %s %s -c \"%s\"\n", binary, makeflow_options, workqueue_options,cpout);
    }else{
        fprintf(fout, "mpirun %s %s %s\n", binary, makeflow_options, workqueue_options);
    }

    fclose(fout);
}

void create_torque_file(char* fileout, struct jx* options) {
    FILE* fout = fopen(fileout, "w+");
    
    char* binary = "";
    char* makeflow_options = "";
    char* workqueue_options = "";

   if (jx_lookup_string(options, "makeflow-arguments") != NULL) {
        makeflow_options = string_format("%s", jx_lookup_string(options, "makeflow-arguments"));
        binary = "mpi_starter";
        if (jx_lookup_integer(options, "use-makeflow-mpi")) {
            binary = "makeflow -T mpi";
        }
        if (jx_lookup_string(options, "workqueue-arguments") != NULL) {
            workqueue_options = string_format("%s", jx_lookup_string(options, "workqueue-arguments"));
        }
    } else {
        binary = "mpi_worker";
        if (jx_lookup_string(options, "workqueue-arguments") != NULL) {
            workqueue_options = (char*) jx_lookup_string(options, "workqueue-arguments");
        }
    }
    
    if(jx_lookup_integer(options,"memory") != 0){
        workqueue_options = string_format("--memory=%i %s",(int)jx_lookup_integer(options,"memory"),workqueue_options);
    }
    if(jx_lookup_string(options,"disk")!=NULL){
        workqueue_options = string_format("--disk=%s %s",jx_lookup_string(options,"disk"),workqueue_options);
    }
    if(jx_lookup_string(options,"disk-location")!=NULL){
        workqueue_options = string_format("--workdir=%s %s",jx_lookup_string(options,"disk-location"),workqueue_options);
    }
    
    fprintf(fout, "#!/bin/sh\n\n");

    if (jx_lookup_string(options, "email") != NULL) {
        fprintf(fout, "#PBS -M %s\n", jx_lookup_string(options, "email")); //email
        fprintf(fout, "#PBS -m abe\n");
    }
    fprintf(fout, "#PBS -N %s\n", generate_job_name()); //job name
    fprintf(fout, "#PBS -j oe\n"); //join stdout and stderr
    fprintf(fout, "#PBS -k o\n"); //keeps output
    fprintf(fout, "#PBS -V\n"); //pass on submitter's environment
    
    //do all the fun nodes and processes and stuff....
    fprintf(fout, "#PBS -l nodes=%i\n",(int)jx_lookup_integer(options,"slots"));
    fprintf(fout, "#PBS -l ppn=1\n"); //we're going to have to say 1, and if cores-per-worker is set, then we can send it, otherwise, set --cores=0 in the worker settings
    if(jx_lookup_integer(options,"cores-per-worker") != 0){
        workqueue_options = string_format("--cores=%i %s",(int)jx_lookup_integer(options,"cores-per-worker"),workqueue_options);
        if (strstr("makeflow -T mpi", binary)) {
            makeflow_options = string_format("--mpi-cores=%i %s", (int) jx_lookup_integer(options, "cores-per-worker"), makeflow_options);
        }
    }else{
        workqueue_options = string_format("--cores=0 %s",workqueue_options);
    }
    //if(jx_lookup_integer(options,"memory")!=0){
    //    fprintf(fout,"#PBS -l pmem=%imb\n",jx_lookup_integer(options,"memory"));
    //}
    
    if (jx_lookup_string(options, "mpi-module") != NULL) {
        fprintf(fout, "module load %s\n", jx_lookup_string(options, "mpi-module")); //assume this works
    }
    
    if(strlen(workqueue_options)>0 && !strstr("mpi_worker",binary)){
        workqueue_options = string_format("-q \"%s\"",workqueue_options);
    }
    if(strlen(makeflow_options)>0 && !(strstr("mpi_worker",binary) || strstr("makeflow -T mpi",binary))){
        makeflow_options = string_format("-m \"%s\"",makeflow_options);
    }
    if(strstr("makeflow -T mpi",binary)){
        workqueue_options = "";
    }
    fprintf(fout, "mpirun -npernode 1 -machinefile $PBS_NODEFILE %s %s %s\n", binary, makeflow_options, workqueue_options);
    
    
    
    fclose(fout);
}

enum submitter_types {
    slurm,
    sge,
    torque
};

int main(int argc, char** argv) {

    enum submitter_types type = 0;
    int c;
    struct jx* config = jx_object(NULL);
    //char* username = getlogin();
    int max_submits = 1;
    int cur_submits = 0;
    struct list* ids = list_create();

    //parse inputs for workers
    //check if makeflow options -> mpi_starter, else -> mpi_worker
    //submit with SGE or SLURM

    while ((c = getopt_long(argc, argv, "m:q:p:w:W:e:u:n:C:c:T:o:t:O:M:hK", long_options, 0)) != -1) {
        switch (c) {
            case 'm': //makeflow-options
                jx_insert_string(config, "makeflow-arguments", xxstrdup(optarg));
                break;
            case 'q': //workqueue-options
                jx_insert_string(config, "workqueue-arguments", xxstrdup(optarg));
                break;
            case 'p': //makeflow-port
                jx_insert_integer(config, "makeflow-port", atoi(optarg));
                break;
            case 'h':
                print_help();
                return 0;
                break;
            case 'w':
                jx_insert_integer(config, "slots", atoi(optarg));
                break;
            case 'W':
                max_submits = atoi(optarg);
                break;
            case 'c':
                jx_insert_integer(config, "cores-per-worker", atoi(optarg));
                break;
            case 'M':
                jx_insert_integer(config, "memory", atoi(optarg));
                break;
            case 'D':
                jx_insert_integer(config, "disk", atoi(optarg));
                break;
            case 'S':
                jx_insert_string(config, "disk-location", xxstrdup(optarg));
                break;
            case 'e':
                jx_insert_string(config, "email", xxstrdup(optarg));
                break;
            case 'u':
                jx_insert_string(config, "queue", xxstrdup(optarg));
                break;
            case 't':
                jx_insert_string(config,"time-limit",xxstrdup(optarg));
                break;
            case 'n':
                jx_insert_string(config, "mpi-name", xxstrdup(optarg));
                break;
            case 'C':
                config = jx_parse_file(optarg);
                break;
            case 'T':
                if (strstr(optarg, "slurm")) type = slurm;
                else if (strstr(optarg, "torque")) type = torque;
                else if (strstr(optarg, "sge")) type = sge;
                else{
                    fprintf(stderr, "Unknown submit type: %s\n", optarg);
                    exit(1);
                }
                break;
            case 'o':
                jx_insert_string(config, "mpi-module", xxstrdup(optarg));
                break;
            case 'O':
                jx_insert_string(config,"copy-out",xxstrdup(optarg));
                break;
            case 'K':
                jx_insert_integer(config,"use-makeflow-mpi",1);
            default: //ignore anything not wanted
                break;
        }
    }

    while (1) {
        if (cur_submits < max_submits) {
            cur_submits += 1;
            fprintf(stderr, "Submitting a new job\n");
            char* filename = random_filename();
            char* tmp = "";
            string_format("qsub %s", filename);
            switch (type) {
                case slurm:
                    tmp = string_format("sbatch %s", filename);
                    create_slurm_file(filename, config);
                    break;
                case torque:
                    tmp = string_format("qsub %s", filename);
                    create_torque_file(filename, config);
                    break;
                case sge:
                    tmp = string_format("qsub %s", filename);
                    create_sge_file(filename, config);
                    break;
                default:
                    fprintf(stderr, "You must specify a submission type\n");
                    exit(1);
                    break;
            }

            //submit job
            FILE* submitret = popen(tmp, "r");
            char outs[1024];
            fgets(outs, 1024, submitret);
            int id = getnum(outs);
            int* idp = malloc(sizeof (int)*1);
            *idp = id;
            fprintf(stderr, "Submitted job: %i\n outs: %s\n", id, outs);
            list_push_tail(ids, (void*) idp);
            free(tmp);

            //tmp = string_format("rm %s", filename);
            //system(tmp);
            //free(tmp);

        }
        //check on jobs
        list_first_item(ids);
        int* idp;
        while((idp = (int*)list_next_item(ids)) != NULL){
            fprintf(stderr,"Checking on job: %i\n",*idp);
            char* tmp;
            switch(type){
                case slurm:
                    tmp = string_format("qstat -j %i",*idp);
                    break;
                case torque:
                    tmp = string_format("qstat %i",*idp);
                    break;
                case sge:
                    tmp = string_format("qstat -j %i",*idp);
                    break;
                default:
                    tmp = string_format(" ");
                    break;
            }
            system(tmp);
            free(tmp);
        }
        

        sleep(45);
    }

    return 0;
}


/* vim: set noexpandtab tabstop=4: */
