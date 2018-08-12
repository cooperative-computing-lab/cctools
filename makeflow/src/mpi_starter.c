#include <stdlib.h>
#include <stdio.h>
#include "hash_table.h"
#include "jx.h"
#include "jx_parse.h"
#include <mpi.h>
#include "getopt.h"
#include "getopt_aux.h"
#include "stringtools.h"
#include "load_average.h"
#include <unistd.h>
#include "host_memory_info.h"
#include "int_sizes.h"

static const struct option long_options[] = {
    {"makeflow-arguments", required_argument, 0, 'm'},
    {"workqueue-arguments", required_argument, 0, 'q'},
    {"makeflow-port", required_argument, 0, 'p'},
    {"copy-out", required_argument, 0, 'c'},
    {"help", no_argument, 0, 'h'},
    {0, 0, 0, 0}
};

void print_help(){
    printf("Use: mpi_starter [options]\n");
    printf("Basic Opetions:\n");
    printf(" -m,--makeflow-arguments       Options to pass to makeflow, such as dagfile, etc\n");
    printf(" -p,--makeflow-port            The port for Makeflow to use when communicating with workers\n");
    printf(" -q,--workqueue-arguments      Options to pass to work_queue_worker\n");
    printf(" -c,--copy-out                 Where to copy out all files produced");
    printf(" -h,--help                     Print out this help\n");
}

static char* get_ipaddr() {
    FILE* ipstream = popen("hostname -i", "r");

    int one, two, three, four;
    fscanf(ipstream, "%i.%i.%i.%i", &one, &two, &three, &four);

    pclose(ipstream);

    return string_format("%i.%i.%i.%i", one, two, three, four);
}

int main(int argc, char** argv) {
    
    //show help?
    int c;
    while ((c = getopt_long(argc, argv, "m:q:p:c:h", long_options, 0)) != -1) {
        switch (c) {
            case 'm': //makeflow-options
                break;
            case 'q': //workqueue-options
                break;
            case 'p':
                break;
            case 'h':
                print_help();
                break;
            case 'c':
                break;
            default: //ignore anything not wanted
                break;
        }
    }
    
    MPI_Init(NULL, NULL);
    int mpi_world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_world_size);
    int mpi_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    char procname[MPI_MAX_PROCESSOR_NAME];
    int procnamelen;
    MPI_Get_processor_name(procname, &procnamelen);
    int rank_0_cores = 1;

    struct hash_table* comps = hash_table_create(0, 0);
    struct hash_table* sizes = hash_table_create(0, 0);

    if (mpi_rank == 0) { //Master to decide who stays and who doesn't
        int i;

        for (i = 1; i < mpi_world_size; i++) {
            unsigned len = 0;
            MPI_Recv(&len, 1, MPI_UNSIGNED, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            char* str = malloc(sizeof (char*)*len + 1);
            memset(str, '\0', sizeof (char)*len + 1);
            MPI_Recv(str, len, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            struct jx* recobj = jx_parse_string(str);
            char* name = jx_lookup_string(recobj, "name");
            int rank = jx_lookup_integer(recobj, "rank");

            if (strstr(procname, name)) { //0 will always be the master on its own comp
                rank_0_cores += 1;
                continue;
            }

            if (hash_table_lookup(comps, name) == NULL) {
                hash_table_insert(comps, name, rank);
            }
            //for partition sizing
            if(hash_table_lookup(sizes, name) == NULL){
                hash_table_insert(sizes, name, 1);
            }else{
                int val = (int) hash_table_lookup(sizes, name);
                hash_table_remove(sizes, name);
                hash_table_insert(sizes, name, val +1 );
                
            }

            jx_delete(recobj);

        }
        for (i = 1; i < mpi_world_size; i++) {
            hash_table_firstkey(comps);
            char* key;
            int value;
            int sent = 0;
            while (hash_table_nextkey(comps, &key, (void**) &value)) {
                if (value == i) {
                    fprintf(stderr, "Telling %i to live\n", value);
                    MPI_Send("LIVE", 4, MPI_CHAR, i, 0, MPI_COMM_WORLD);
                    sent = 1;
                }
            }
            if (sent == 0) {
                MPI_Send("DIE ", 4, MPI_CHAR, i, 0, MPI_COMM_WORLD);
            }
        }

    } else { //send proc name and num
        hash_table_delete(comps);
        char* sendstr = string_format("{\"name\":\"%s\",\"rank\":%i}", procname, mpi_rank);
        unsigned len = strlen(sendstr);
        MPI_Send(&len, 1, MPI_UNSIGNED, 0, 0, MPI_COMM_WORLD);
        MPI_Send(sendstr, len, MPI_CHAR, 0, 0, MPI_COMM_WORLD);

        free(sendstr);
        //get if live or die
        char livedie[10];
        MPI_Recv(livedie, 4, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (strstr(livedie, "DIE")) {
            MPI_Finalize();
            return 0;
        } else if (strstr(livedie, "LIVE")) {
            //do nothing, continue
        } else {
            fprintf(stderr, "livedie string got corrupted, wrong command sent.... %s\n", livedie);
            MPI_Finalize();
            return 1;
        }//corrupted string or something
    }

    //end major 

    char* makeflow_args = "";
    char* workqueue_args = "";
    char* port = "9000";
    char* cpout = NULL;
    while ((c = getopt_long(argc, argv, "m:q:p:c:h", long_options, 0)) != -1) {
        switch (c) {
            case 'm': //makeflow-options
                makeflow_args = xxstrdup(optarg);
                break;
            case 'q': //workqueue-options
                workqueue_args = xxstrdup(optarg);
                break;
            case 'p':
                port = xxstrdup(optarg);
                break;
            case 'h':
                print_help();
                break;
            case 'c':
                cpout = xxstrdup(optarg);
                break;
            default: //ignore anything not wanted
                break;
        }
    }

    if (mpi_rank == 0) { //we're makeflow
        char* master_ipaddr = get_ipaddr();
        unsigned mia_len = strlen(master_ipaddr);
        int value;
        char* key;
        fprintf(stderr, "master ipaddress: %s\n", master_ipaddr);
        hash_table_firstkey(comps);
        while (hash_table_nextkey(comps, &key, (void**) &value)) {
            fprintf(stderr, "sending my ip to %s rank %i\n", key, value);
            MPI_Send(&mia_len, 1, MPI_UNSIGNED, value, 0, MPI_COMM_WORLD);
            MPI_Send(master_ipaddr, mia_len, MPI_CHAR, value, 0, MPI_COMM_WORLD);
        }
        //tell the remaining how big to make themselves
        hash_table_firstkey(sizes);
        char* sizes_key;
        int sizes_cor;
        while(hash_table_nextkey(sizes,&sizes_key,(void**)&sizes_cor)){
            hash_table_firstkey(comps);
            while (hash_table_nextkey(comps, &key, (void**) &value)) {
                if(strstr(key,sizes_key) != NULL){
					if(getenv("MPI_WORKER_CORES_PER") != NULL){ //check to see if we're passing this via env-var
						sizes_cor = atoi(getenv("MPI_WORKER_CORES_PER"));
					}
                    MPI_Send(&sizes_cor,1,MPI_INT,value,0,MPI_COMM_WORLD);
                }
            }
        }

        int cores = rank_0_cores;
        int cores_total = load_average_get_cpus();
        UINT64_T memtotal;
        UINT64_T memavail;
        host_memory_info_get(&memavail,&memtotal);
        int mem = ((memtotal/(1024*1024))/cores_total)*cores;//MB

        char* sys_str = string_format("makeflow -T wq --port=%s -d all --local-cores=%i %s", port, ((cores / 2) + 1), makeflow_args);

        int k = 0;
        
        if (cores > 1) {
            pid_t fid = fork();
            if (fid < 0) {
                error(1);
            } else if (fid == 0) { //I'm the child, run worker
                sys_str = string_format("work_queue_worker --cores=%i --memory=%i %s %s %s", cores / 2, mem, master_ipaddr, port, workqueue_args);
                fprintf(stderr,"I'm the child running: %s\n",sys_str);
                k = system(sys_str);
                fprintf(stderr,"I'm the child, I'm done and returning!\n");
                return k;
            } else { //I'm the parent, run makeflow
                fprintf(stderr, "Starting Makeflow command: %s\n", sys_str);
                k = system(sys_str);
                fprintf(stderr,"Makeflow has finished! Waiting for worker to die!\n");
                int status;
                pid_t pid = waitpid(fid,&status,0);
                if(pid < 1){
                    fprintf(stderr,"Error in waiting for master's worker to die\n");
                }
            }
        } else { //only 1 core, so just have the makeflow run
            fprintf(stderr, "Starting Makeflow w/no fork command: %s\n", sys_str);
            k = system(sys_str);
            fprintf(stderr,"Makeflow has finished! No forking happened, so dying.\n");
        }
        //free(sys_str);
        //free(master_ipaddr);
        
        sys_str = string_format("pwd && ls");
        system(sys_str);
        
        if(cpout != NULL){
            sys_str = string_format("cp -r `pwd`/* %s",cpout);
            system(sys_str);
        }

        hash_table_delete(comps);
        hash_table_delete(sizes);

        MPI_Finalize();
        return k;

    } else { //we're a work_queue_worker

        fprintf(stderr, "Yay, i'm a worker starter, and my procname is: %s\n", procname);

        unsigned len = 0;
        MPI_Recv(&len, 1, MPI_UNSIGNED, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        char* master_ip = malloc(sizeof (char*)*len + 1);
        memset(master_ip, '\0', sizeof (char)*len + 1);
        MPI_Recv(master_ip, len, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        fprintf(stderr, "Here is the master_ipaddr: %s\n", master_ip);
        
        int cores;
        MPI_Recv(&cores,1,MPI_INT,0,0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        //get a fair amount of memory, for now
        int cores_total = load_average_get_cpus();
        UINT64_T memtotal;
        UINT64_T memavail;
        host_memory_info_get(&memavail,&memtotal);
        fprintf(stderr,"worker: %i memtotal: %lu\n",mpi_rank,memtotal);
        int mem = ((memtotal/(1024*1024))/cores_total)*cores;//gigabytes
        
        fprintf(stderr,"Calling printenv from worker: %i\n",mpi_rank);
        char* printenvstr = string_format("printenv > rank_%i_env.txt",mpi_rank);
        system(printenvstr);
        
        char* sys_str = string_format("work_queue_worker --cores=%i --memory=%i %s %s %s", cores, mem, master_ip, port, workqueue_args);
        fprintf(stderr,"Rank %i: Starting Worker: %s\n",mpi_rank,sys_str);
        int k = system(sys_str);
        fprintf(stderr,"Rank %i: Worker is now done!\n",mpi_rank);
        //free(sys_str);
        
        //should keep starting the worker in a loop until Rank 0 says to die.

        MPI_Finalize();
        return k;

    }

    return 0;
}


/* vim: set noexpandtab tabstop=4: */
