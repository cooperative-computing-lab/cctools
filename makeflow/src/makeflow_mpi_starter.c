/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef CCTOOLS_WITH_MPI

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
#include <string.h>
#include "xxmalloc.h"
#include <signal.h>
#include <sys/wait.h>
#include <sys/types.h>

int workqueue_pid;

void wq_handle(int sig) {
    kill(workqueue_pid, SIGTERM);
    _exit(0);
}



static const struct option long_options[] = {
    {"makeflow-arguments", required_argument, 0, 'm'},
    {"workqueue-arguments", required_argument, 0, 'q'},
    {"makeflow-port", required_argument, 0, 'p'},
    {"copy-out", required_argument, 0, 'c'},
    {"help", no_argument, 0, 'h'},
    {"debug", required_argument, 0, 'd'},
    {0, 0, 0, 0}
};

void print_help() {
    printf("Use: makeflow_mpi_starter [options]\n");
    printf("Basic Options:\n");
    printf(" -m,--makeflow-arguments       Options to pass to makeflow, such as dagfile, etc\n");
    printf(" -p,--makeflow-port            The port for Makeflow to use when communicating with workers\n");
    printf(" -q,--workqueue-arguments      Options to pass to work_queue_worker\n");
    printf(" -c,--copy-out                 Where to copy out all files produced\n");
    printf(" -d,--debug                    Base Debug file name\n");
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
    for (c = 0; c < argc; c++) {
        if (strstr(argv[c], "-h") || strstr(argv[c], "--help")) {
            print_help();
            return 0;
        }
    }
    c = 0;

	//mpi boilerplate code modified from tutorial at www.mpitutorial.com
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
            char* name = (char*) jx_lookup_string(recobj, "name");
            int rank = jx_lookup_integer(recobj, "rank");


            if (hash_table_lookup(comps, name) == NULL) {
                long long p = rank;
                hash_table_insert(comps, name, (void*) p);
            }
            //for partition sizing
            if (hash_table_lookup(sizes, name) == NULL) {
                long long p = 1;
                hash_table_insert(sizes, name, (void*) p);
            } else {
                long long val = (long long) hash_table_lookup(sizes, name);
                hash_table_remove(sizes, name);
                long long p = val + 1;
                hash_table_insert(sizes, name, (void*) p);

            }

            jx_delete(recobj);

        }
        for (i = 1; i < mpi_world_size; i++) {
            hash_table_firstkey(comps);
            char* key;
            long long value;
            int sent = 0;
            while (hash_table_nextkey(comps, &key, (void**) &value)) {
                if (value == i) {
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
            MPI_Finalize();
            return 1;
        }//corrupted string or something
    }

    //end major 

    char* makeflow_args = "";
    char* workqueue_args = "";
    char* port = "9000";
    char* cpout = NULL;
    char* debug_base = NULL;
    while ((c = getopt_long(argc, argv, "m:q:p:c:d:h", long_options, 0)) != -1) {
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
            case 'd':
                debug_base = xxstrdup(optarg);
                break;
            default: //ignore anything not wanted
                break;
        }
    }

    if (mpi_rank == 0) { //we're makeflow
        char* manager_ipaddr = get_ipaddr();
        unsigned mia_len = strlen(manager_ipaddr);
        long long value;
        char* key;
        hash_table_firstkey(comps);
        while (hash_table_nextkey(comps, &key, (void**) &value)) {
            MPI_Send(&mia_len, 1, MPI_UNSIGNED, value, 0, MPI_COMM_WORLD);
            MPI_Send(manager_ipaddr, mia_len, MPI_CHAR, value, 0, MPI_COMM_WORLD);
        }
        //tell the remaining how big to make themselves
        hash_table_firstkey(sizes);
        char* sizes_key;
        long long sizes_cor;
        while (hash_table_nextkey(sizes, &sizes_key, (void**) &sizes_cor)) {
            hash_table_firstkey(comps);
            while (hash_table_nextkey(comps, &key, (void**) &value)) {
                if (strstr(key, sizes_key) != NULL) {
                    if (getenv("MPI_WORKER_CORES_PER") != NULL) { //check to see if we're passing this via env-var
                        sizes_cor = atoi(getenv("MPI_WORKER_CORES_PER"));
                    }
                    int sizes_cor_int = sizes_cor;
                    MPI_Send(&sizes_cor_int, 1, MPI_INT, value, 0, MPI_COMM_WORLD);
                }
            }
        }

        int cores = rank_0_cores;
        int cores_total = load_average_get_cpus();
        UINT64_T memtotal;
        UINT64_T memavail;
        host_memory_info_get(&memavail, &memtotal);
        int mem = ((memtotal / (1024 * 1024)) / cores_total) * cores; //MB

        char* sys_str = string_format("makeflow -T wq --port=%s --local-cores=%i --local-memory=%i %s", port, 1, mem, makeflow_args);
        if (debug_base != NULL) {
            sys_str = string_format("makeflow -T wq --port=%s -dall --debug-file=%s.makeflow --local-cores=%i --local-memory=%i %s", port, debug_base, 1, mem, makeflow_args);
        }

        int k = 0;
        k = system(sys_str);
        hash_table_firstkey(comps);
        //char* key1;
        //long long value1;
        while (hash_table_nextkey(comps, &key, (void**) &value)) {
            unsigned die = 10;
            MPI_Send(&die, 1, MPI_UNSIGNED, value, 0, MPI_COMM_WORLD);
        }

        if (cpout != NULL) {
            sys_str = string_format("cp -r `pwd`/* %s", cpout);
            system(sys_str);
        }

        hash_table_delete(comps);
        hash_table_delete(sizes);

        MPI_Finalize();
        return k;

    } else { //we're a work_queue_worker

        unsigned len = 0;
        MPI_Recv(&len, 1, MPI_UNSIGNED, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        char* manager_ip = malloc(sizeof (char*)*len + 1);
        memset(manager_ip, '\0', sizeof (char)*len + 1);
        MPI_Recv(manager_ip, len, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        int cores;
        MPI_Recv(&cores, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        //get a fair amount of memory, for now
        int cores_total = load_average_get_cpus();
        UINT64_T memtotal;
        UINT64_T memavail;
        host_memory_info_get(&memavail, &memtotal);
        int mem = ((memtotal / (1024 * 1024)) / cores_total) * cores; //gigabytes

        char* sys_str = string_format("work_queue_worker --timeout=86400 --cores=%i --memory=%i %s %s %s", cores, mem, manager_ip, port, workqueue_args);
        if (debug_base != NULL) {
            sys_str = string_format("work_queue_worker --timeout=86400 -d all -o %s.workqueue.%i --cores=%i --memory=%i %s %s %s", debug_base, mpi_rank, cores, mem, manager_ip, port, workqueue_args);
        }
        int pid = fork();
        int k = 0;
        if (pid == 0) {
            signal(SIGTERM, wq_handle);
            for (;;) {
                workqueue_pid = fork();
                if (workqueue_pid == 0) {
                    int argc;
                    char **argv;
                    string_split_quotes(sys_str, &argc, &argv);
                    execvp(argv[0], argv);
                    fprintf(stderr, "Oh no, workqueue execvp didn't work...");
					exit(1);
                }
                int status_wq;
                waitpid(workqueue_pid, &status_wq, 0);
            }
        } else {
            unsigned die;
            MPI_Recv(&die, 1, MPI_UNSIGNED, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            kill(pid, SIGTERM);
        }

        MPI_Finalize();
        return k;

    }

    return 0;
}
#else
#include <stdio.h>
int main(int argc, char** argv){
    fprintf(stdout,"To use this Program, please configure and compile cctools with MPI.\n");
}

#endif


/* vim: set noexpandtab tabstop=4: */
