/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "process.h"
#include "macros.h"
#include "stringtools.h"

#include "hash_table.h"
#include "jx.h"
#include "jx_parse.h"
#include "load_average.h"
#include <unistd.h>
#include "host_memory_info.h"
#include "int_sizes.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>

#ifdef MPI

#include <mpi.h>

static struct hash_table* name_rank;
static struct hash_table* name_size;

static int gotten_resources = 0;



void batch_job_mpi_give_ranks_sizes(struct hash_table* nr, struct hash_table* ns){
    name_rank = nr;
    name_size = ns;
}

static void get_resources(){
    gotten_resources = 1;
    char* key;
    int value;
    char* worker_cmd;
    unsigned len;

    hash_table_firstkey(name_rank);
    while (hash_table_nextkey(name_rank, &key, (void**) &value)) {
        //ask for resources
        worker_cmd = string_format("{\"Orders\":\"Send-Resources\"}");
        len = strlen(worker_cmd);
        MPI_Send(&len, 1, MPI_UNSIGNED, value, 0, MPI_COMM_WORLD);
        MPI_Send(worker_cmd, len, MPI_CHAR, value, 0, MPI_COMM_WORLD);

        //recieve response
        MPI_Recv(&len, 1, MPI_UNSIGNED, value, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        char* str = malloc(sizeof (char*)*len + 1);
        memset(str, '\0', sizeof (char)*len + 1);
        MPI_Recv(str, len, MPI_CHAR, value, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        //parse response
        struct jx* recobj = jx_parse_string(str);



        free(str);
        jx_delete(recobj);
    }
    
}

static batch_job_id_t batch_job_mpi_submit (struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources ){
//We're only here because we're rank 0.
    //Step 1: find a comp not currently in use that can fit this command.
    //Step 2: if yes send the command to the comp
    //       if no return -1 I think....
    
    int i;
    char* key;
    int value;
    char* worker_cmd;
    unsigned len;
    
    //some init stuff
    if(!gotten_resources) get_resources();
    
    int cores_req = resources->cores;
    int mem_req = resources->memory;
}


static batch_job_id_t batch_job_mpi_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime){
    //See if we have any msgs from any of the comps
}

static int batch_job_mpi_remove (struct batch_queue *q, batch_job_id_t jobid){
    //tell a comp to kill this job
}

void batch_job_mpi_kill_workers(){
    char* key;
    int value;
    char* worker_cmd;
    hash_table_firstkey(name_rank);
    while (hash_table_nextkey(name_rank, &key, (void**) &value)) {
        worker_cmd = string_format("{\"Orders\":\"Terminate\"}");
        unsigned len = strlen(worker_cmd);
        MPI_Send(&len, 1, MPI_UNSIGNED, value, 0, MPI_COMM_WORLD);
        MPI_Send(worker_cmd, len, MPI_CHAR, value, 0, MPI_COMM_WORLD);
    }
}

static int batch_queue_mpi_create (struct batch_queue *q)
{
	batch_queue_set_feature(q, "mpi_job_queue", NULL);
	return 0;
}

int batch_job_mpi_worker_function(int worldsize, int rank, char* procname, int procnamelen){
    
    if(worldsize < 2){
        debug(D_BATCH,"Soemthing went terribly wrong.....");
    }
    
    //first, send name and rank to MPI master
    char* sendstr = string_format("{\"name\":\"%s\",\"rank\":%i}", procname, rank);
    unsigned len = strlen(sendstr);
    MPI_Send(&len, 1, MPI_UNSIGNED, 0, 0, MPI_COMM_WORLD);
    MPI_Send(sendstr, len, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    
    MPI_Recv(&len,1,MPI_UNSIGNED,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    char* str = malloc(sizeof (char*)*len + 1);
    memset(str, '\0', sizeof (char)*len + 1);
    MPI_Recv(str, len, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    
    struct jx* recobj = jx_parse_string(str);
    int cores;
    if((cores = jx_lookup_integer(recobj, "LIVE")) == 0){ //meaning we should die
        return 0;
    }
    
    while(1){
        MPI_Recv(&len, 1, MPI_UNSIGNED,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        free(str);
        str = malloc(sizeof (char*)*len + 1);
        memset(str, '\0', sizeof (char)*len + 1);
        MPI_Recv(str, len, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        jx_delete(recobj);
        recobj = jx_parse_string(str);
        
        if(strstr(jx_lookup_string(recobj,"Orders"),"Terminate")){
            break; //terminating, meaning we're done!
        }
        
        if(strstr(jx_lookup_string(recobj,"Orders"),"Send-Resources")){
            //need to send resources json object
        }
        
        if(strstr(jx_lookup_string(recobj,"Orders"),"Execute")){
            
        }
        
        //otherwise, probably start a job or something
    }
    
    
    MPI_Finalize();
    
    return 0;
}

batch_queue_stub_free(mpi);
batch_queue_stub_port(mpi);
batch_queue_stub_option_update(mpi);

batch_fs_stub_chdir(mpi);
batch_fs_stub_getcwd(mpi);
batch_fs_stub_mkdir(mpi);
batch_fs_stub_putfile(mpi);
batch_fs_stub_rename(mpi);
batch_fs_stub_stat(mpi);
batch_fs_stub_unlink(mpi);

const struct batch_queue_module batch_queue_mpi = {
	BATCH_QUEUE_TYPE_LOCAL,
	"mpi",

	batch_queue_mpi_create,
	batch_queue_mpi_free,
	batch_queue_mpi_port,
	batch_queue_mpi_option_update,

	{
		batch_job_mpi_submit,
		batch_job_mpi_wait,
		batch_job_mpi_remove,
	},

	{
		batch_fs_mpi_chdir,
		batch_fs_mpi_getcwd,
		batch_fs_mpi_mkdir,
		batch_fs_mpi_putfile,
		batch_fs_mpi_rename,
		batch_fs_mpi_stat,
		batch_fs_mpi_unlink,
	},
};

#endif

/* vim: set noexpandtab tabstop=4: */
