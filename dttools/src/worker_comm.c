#include "debug.h"
#include "itable.h"
#include "list.h"
#include "link.h"
#include "worker_comm.h"

#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <sys/stat.h>

#define WORKER_COMM_TAG_OP	0x00
#define WORKER_COMM_TAG_ROLE	0x01

int worker_comm_accept_connections(int interface, struct link *master_link, struct list *active_workers, int active_timeout, int short_timeout)
{
	static struct worker *worker_info = NULL;
	struct worker_comm *comm;
	int sleeptime, stoptime, mpi_init = 0;

	stoptime = time(0) + short_timeout;
	switch(interface) {
		case WORKER_COMM_MPI:
			MPI_Initialized(&mpi_init);
			if(!mpi_init)
				return -1;
			break;
		case WORKER_COMM_TCP:
			if(!master_link)
				return -1;
			break;
	}

	switch(interface) {
		case WORKER_COMM_MPI:
			while(time(0) < stoptime) {
				static struct worker *mpi_worker_info = NULL;
				static MPI_Request mpi_current_request = MPI_REQUEST_NULL;
				
				int complete = 0;
				MPI_Status mpi_stat;
			 
				if(!mpi_worker_info) {
					mpi_worker_info = malloc(sizeof(*mpi_worker_info));
					memset(worker_info, 0, sizeof(*mpi_worker_info));
				}
				if(mpi_current_request == MPI_REQUEST_NULL) {
					MPI_Irecv(mpi_worker_info, sizeof(*mpi_worker_info), MPI_BYTE, MPI_ANY_SOURCE, WORKER_COMM_TAG_ROLE, &mpi_current_request);
				}
		
				MPI_Test(&mpi_current_request, &complete, &mpi_stat);
				if(complete) {
					worker_info = mpi_worker_info;
					mpi_worker_info = NULL;
					
					comm = malloc(sizeof(*comm));
					comm->type = WORKER_COMM_MPI;
					comm->mpi_rank = mpi_stat.MPI_SOURCE;
					comm->active_timeout = active_timeout;
					comm->short_timeout = short_timeout;
					comm->results = 0;
					comm->hostname = NULL;
					comm->lnk = NULL;
					comm->mpi_req = MPI_REQUEST_NULL;
					
					worker_info->comm = comm;
					list_push_tail(available_workers, worker_info);
					worker_info = NULL;
					comm = NULL;
				}
			}
		break;
		
		case WORKER_COMM_TCP:
			// If the master link was awake, then accept as many workers as possible.
			sleeptime = (stoptime - time(0)) * 1000000;
			if(link_usleep(master_link, sleeptime, 1, 0)) {
				do {
					worker_info = malloc(sizeof(*worker_info));
					comm = malloc(sizeof(*comm));
					
					comm->lnk = link_accept(master_link, stoptime);
					if(comm->lnk) {
						char line[WQ_LINE_MAX];
						int result;
						link_tune(comm->lnk, LINK_TUNE_INTERACTIVE);
						result = link_readline(comm->lnk, line, WQ_LINE_MAX, stoptime);

						comm->type = WORKER_COMM_TCP;
						comm->mpi_rank = -1;
						comm->active_timeout = active_timeout;
						comm->short_timeout = short_timeout;
						comm->results = 0;
						comm->mpi_req = MPI_REQUEST_NULL;
						
						if(result > 0) {
							sscanf(line, "%s %d %d %d\n", &worker_info->hostname, &worker_info->cores, &worker_info->ram, &worker_info->disk);
							comm->hostname = strdup(worker_info->hostname);
							worker_info->comm = comm;
							list_push_tail(available_workers, worker_info);
							continue;
						}
						
					}
					free(worker_info);
					free(comm);
					return -1;
				
				} while(link_usleep(master_link, 0, 1, 0));
			}
		break;
	}
	
	return 0;
}

struct worker_comm * worker_comm_connect(struct worker_comm *comm, int interface, const char *hostname, int port_id, int active_timeout, int short_timeout)
{
	int comm_create = 0, mpi_init = 0;
	
	MPI_Initialized(&mpi_init);
	if(interface == WORKER_COMM_MPI && !mpi_init)
		return NULL;

	if(!comm) {
		comm = malloc(sizeof(*comm));
		memset(comm, 0, sizeof(*comm));
		comm_create = 1;
	}
	comm->active_timeout = active_timeout;
	comm->short_timeout = short_timeout;

	switch(interface) {
		case WORKER_COMM_MPI:
			comm->type = WORKER_COMM_MPI;
			comm->mpi_rank = port_id;
			comm->lnk = NULL;
		break;
		
		case WORKER_COMM_TCP:
			comm->type = WORKER_COMM_TCP;
			comm->mpi_rank = -1;
			comm->lnk = link_connect(hostname, port_id, active_timeout);
			if(!comm->lnk) {
				if(comm_create)
					free(comm);
				return NULL;
			}
		break;
	}
	
	return comm;
}

void worker_comm_disconnect(struct worker_comm *comm)
{
	if(comm->lnk) {
		link_close(comm->lnk);
		comm->lnk = NULL;
	}
	
	if(comm->mpi_rank >= 0 ) {
		comm->mpi_rank = -1;
	}

	if(comm->mpi_req != MPI_REQUEST_NULL) {
		MPI_Cancel(&comm->mpi_req);
		MPI_Request_free(&comm->mpi_req);
	}
}

void worker_comm_delete(struct worker_comm *comm)
{
	worker_comm_disconnect(comm);
	free(comm);
}

int worker_comm_send_worker(struct worker_comm *comm, struct worker *workerdata)
{
	int mpi_init, stoptime;
	switch(comm->type) {
		case WORKER_COMM_MPI:
			MPI_Initialized(&mpi_init);
			if(!mpi_init)
				return -1;
			MPI_Send(workerdata, sizeof(*workerdata), MPI_BYTES, comm->mpi_rank, 0, MPI_COMM_WORLD);
			break;
		case WORKER_COMM_TCP:
			if(!comm->lnk)
				return -1;
			stoptime = time(0) + comm->active_timeout;
			link_putfstring(comm->lnk, "%s %d %d %d\n", stoptime, workerdata->hostname, workerdata->cores, workerdata->ram, workerdata->disk);
			break;
	}
	return 0;
}

int worker_comm_send_array(struct worker_comm *comm, int datatype, void* buf, int length)
{
	int i, mpi_init, stoptime;
	
	switch(comm->type) {
		case WORKER_COMM_MPI:
			MPI_Initialized(&mpi_init);
			if(!mpi_init)
				return -1;
			break;
		case WORKER_COMM_TCP:	
			stoptime = time(0) + comm->active_timeout;
			if(!comm->lnk)
				return -1;
			break;
	}
	
	switch(datatype) {
		case WORKER_COMM_ARRAY_INT:
			if(comm->type == WORKER_COMM_MPI) {
				MPI_Send(buf, length, MPI_INT, comm->mpi_rank, 0, MPI_COMM_WORLD);
			}
			if(comm->type == WORKER_COMM_TCP) {
				for(i = 0; i < length; i++)
					link_putfstring(comm->lnk, "%d ", stoptime, ((int*)buf)[i]);
				link_putfstring(comm->lnk, "\n", stoptime);
			}
			break;

		case WORKER_COMM_ARRAY_CHAR:
			if(comm->type == WORKER_COMM_MPI) {
				MPI_Send(buf, length, MPI_CHAR, comm->mpi_rank, 0, MPI_COMM_WORLD);
			}
			if(comm->type == WORKER_COMM_TCP) {
				link_write(comm->lnk, (char *)buf, sizeof(char)*length, stoptime);
			}
			break;

		case WORKER_COMM_ARRAY_FLOAT:
			if(comm->type == WORKER_COMM_MPI) {
				MPI_Send(buf, length, MPI_FLOAT, comm->mpi_rank, 0, MPI_COMM_WORLD);
			}
			if(comm->type == WORKER_COMM_TCP) {
				for(i = 0; i < length; i++)
					link_putfstring(comm->lnk, "%f ", stoptime, ((float*)buf)[i]);
				link_putfstring(comm->lnk, "\n", stoptime);
			}
			break;

		case WORKER_COMM_ARRAY_DOUBLE:
			if(comm->type == WORKER_COMM_MPI) {
				MPI_Send(buf, length, MPI_DOUBLE, comm->mpi_rank, 0, MPI_COMM_WORLD);
			}
			if(comm->type == WORKER_COMM_TCP) {
				for(i = 0; i < length; i++)
					link_putfstring(comm->lnk, "%f ", stoptime, ((double*)buf)[i]);
				link_putfstring(comm->lnk, "\n", stoptime);
			}
			break;
	}
	
	return 0;
}

int worker_comm_recv_array(struct worker_comm *comm, int datatype, void* buf, int length)
{
	int i, mpi_init, stoptime;
	char line[WQ_LINE_MAX];
	char *tokens, *token;
	
	switch(comm->type) {
		case WORKER_COMM_MPI:
			MPI_Initialized(&mpi_init);
			if(!mpi_init)
				return -1;
			break;
		case WORKER_COMM_TCP:	
			if(!comm->lnk)
				return -1;
			stoptime = time(0) + comm->active_timeout;
			link_readline(comm->lnk, line, WQ_LINE_MAX, stoptime);
			break;
	}
	
	switch(datatype) {
		case WORKER_COMM_ARRAY_INT:
			if(comm->type == WORKER_COMM_MPI) {
				MPI_Recv(buf, length, MPI_INT, comm->mpi_rank, 0, MPI_COMM_WORLD, &comm->mpi_stat);
			}
			if(comm->type == WORKER_COMM_TCP) {
				token = strtok_r(line, " ", tokens);
				i = 0;
				do{
					sscanf(token, "%d", ((int*)buf)+i);
					i++;
				}while((token = strtok_r(NULL, " ", tokens)));
			}
			break;

		case WORKER_COMM_ARRAY_CHAR:
			if(comm->type == WORKER_COMM_MPI) {
				MPI_Recv(buf, length, MPI_CHAR, comm->mpi_rank, 0, MPI_COMM_WORLD, &comm->mpi_stat);
			}
			if(comm->type == WORKER_COMM_TCP) {
				memcpy((char *)buf, line, length);
			}
			break;

		case WORKER_COMM_ARRAY_FLOAT:
			if(comm->type == WORKER_COMM_MPI) {
				MPI_Recv(buf, length, MPI_FLOAT, comm->mpi_rank, 0, MPI_COMM_WORLD, &comm->mpi_stat);
			}
			if(comm->type == WORKER_COMM_TCP) {
				token = strtok_r(line, " ", tokens);
				i = 0;
				do{
					sscanf(token, "%f", ((float*)buf)+i);
					i++;
				}while((token = strtok_r(NULL, " ", tokens)));
			}
			break;

		case WORKER_COMM_ARRAY_DOUBLE:
			if(comm->type == WORKER_COMM_MPI) {
				MPI_Recv(buf, length, MPI_DOUBLE, comm->mpi_rank, 0, MPI_COMM_WORLD, &comm->mpi_stat);
			}
			if(comm->type == WORKER_COMM_TCP) {
				token = strtok_r(line, " ", tokens);
				i = 0;
				do{
					sscanf(token, "%f", ((double*)buf)+i);
					i++;
				}while((token = strtok_r(NULL, " ", tokens)));
			}
			break;
	}
	return 0;
}

int worker_comm_send_buffer(struct worker_comm *comm, const char *buffer, int length, char header)
{
	int mpi_init, stoptime;
	switch(comm->type) {
		case WORKER_COMM_MPI:
			MPI_Initialized(&mpi_init);
			if(!mpi_init)
				return -1;
			if(header)
				MPI_Send(&length, 1, MPI_INT, comm->mpi_rank, 0, MPI_COMM_WORLD);
			if(length > 0)
				MPI_Send(buffer, length, MPI_BYTES, comm->mpi_rank, 0, MPI_COMM_WORLD);
			break;
		case WORKER_COMM_TCP:
			stoptime = time(0) + comm->active_timeout;
			if(!comm->lnk)
				return -1;
			if(header)
				link_putfstring(comm->lnk, "%d\n", stoptime, length);
			if(length > 0)
				link_write(comm->lnk, buffer, length, stoptime);
			break;
	}
	return 0;
}

int worker_comm_send_file(struct worker_comm *comm, const char *filename, int length, char header)
{
	int mpi_init, result, stoptime;
	FILE *source;
	
	if(length <= 0) {
		struct stat st;
		result = stat(filename, &st);
		length = st.st_size;
	}
	if(result < 0 || length <= 0)
		return -1;
	source = fopen(filename, "r");
	if(!source)
		return -1;
	
	switch(comm->type) {
		case WORKER_COMM_MPI:
			MPI_Initialized(&mpi_init);
			if(!mpi_init)
				return -1;
			if(length > 0) {
				char *buffer;
				int bufferlength;
				bufferlength = copy_stream_to_buffer(source, &buffer);
				fclose(source);
				if(bufferlength < length)
					length = bufferlength;
				if(length <= 0) {
					free(buffer);
					return -1;
				}
				if(header) {
					MPI_Send(&length, 1, MPI_INT, comm->mpi_rank, 0, MPI_COMM_WORLD);
				}
				MPI_Send(buffer, length, MPI_BYTES, comm->mpi_rank, 0, MPI_COMM_WORLD);
				free(buffer);
			break;
		case WORKER_COMM_TCP:
			if(!comm->lnk)
				return -1;
			stoptime = time(0)+comm->active_timeout;
			if(header)
				link_putfstring(comm->lnk, "%d\n", stoptime, length);
			if(length > 0)
				link_stream_from_file(comm->lnk, source, length, stoptime);
			fclose(source);
			break;
	}
	return 0;
}

int worker_comm_recv_buffer(struct worker_comm *comm, char **buffer, int *bufferlength, char header)
{
	int mpi_init, stoptime;
	char line[WQ_LINE_MAX];
	
	switch(comm->type) {
		case WORKER_COMM_MPI:
			MPI_Initialized(&mpi_init);
			if(!mpi_init)
				return -1;
			if(header)
				MPI_Recv(bufferlength, 1, MPI_INT, comm->mpi_rank, 0, MPI_COMM_WORLD, comm->mpi_stat);
			if(*bufferlength > 0) {
				*buffer = malloc(*bufferlength);
				memset(*buffer, *bufferlength, 0);
				MPI_Recv(*buffer, *bufferlength, MPI_BYTES, comm->mpi_rank, 0, MPI_COMM_WORLD, comm->mpi_stat);
			} else {
				*buffer = NULL;
			}
			break;
		case WORKER_COMM_TCP:
			if(!comm->lnk)
				return -1;
			stoptime = time(0)+comm->active_timeout;
			if(header) {
				link_readline(comm->lnk, line, WQ_LINE_MAX, stoptime);
				sscanf(line, "%d\n", bufferlength);
			}
			if(*bufferlength > 0) {
				*buffer = malloc(*bufferlength);
				memset(*buffer, *bufferlength, 0);
				link_read(comm->lnk, *buffer, *bufferlength, stoptime);
			} else {
				*buffer = NULL;
			}
			break;
	}
	return 0;
}

int worker_comm_send_op(struct worker_comm *comm, struct worker_op *op)
{
	int mpi_init, stoptime;
	switch(comm->type) {
		case WORKER_COMM_MPI:
			MPI_Initialized(&mpi_init);
			if(!mpi_init)
				return -1;
			MPI_Send(op, sizeof(*op), MPI_BYTES, comm->mpi_rank, 0, MPI_COMM_WORLD);
			if(op->payload)
				MPI_Send(op->payload, op->payloadsize, MPI_BYTES, comm->mpi_rank, 0, MPI_COMM_WORLD);
			break;
		case WORKER_COMM_TCP:
			stoptime = time(0)+comm->active_timeout;
			if(!comm->lnk)
				return -1;
			link_putfstring(comm->lnk, "%d %d %d %d %d %d %s\n", stoptime, op->type, op->jobid, op->id, op->options, op->flags, op->payloadsize, op->name);
			if(op->payload)
				link_write(comm->lnk, op->payload, op->payloadsize, stoptime);
			break;
	}
	return 0;
}

int worker_comm_receive_op(struct worker_comm *comm, struct worker_op *op)
{
	int mpi_init, complete, stoptime;
	char line[WQ_LINE_MAX];
	stoptime = time(0)+comm->active_timeout;
	
	switch(comm->type) {
		case WORKER_COMM_MPI:
			MPI_Initialized(&mpi_init);
			if(!mpi_init)
				return -1;
			if(comm->mpi_req == MPI_REQUEST_NULL) {
				MPI_Irecv(op, sizeof(*op), MPI_BYTES, comm->mpi_rank, 0, MPI_COMM_WORLD, &comm->mpi_req);
			}
			while(time(0) < stoptime && !complete) {
				MPI_Test(&comm->mpi_req, &complete, &comm->mpi_stat);
			}
			
			if(complete) {
				if(op->payloadsize) {
					op->payload = malloc(op->payloadsize);
					memset(op->payload, 0, op->payloadsize);
					MPI_Recv(op->payload, op->payloadsize, MPI_BYTES, comm->mpi_rank, 0, MPI_COMM_WORLD, &comm->mpi_stat);
				} else {
					op->payload = NULL;
				}
			}
			break;
		case WORKER_COMM_TCP:
			if(!comm->lnk)
				return -1;
			complete = link_readline(comm->lnk, line, WQ_LINE_MAX, stoptime);
			
			if(complete) {
				sscanf(line, "%d %d %d %d %d %d %s", &op->type, &op->jobid, &op->id, &op->options, &op->flags, &op->payloadsize, op->name);
				if(op->payloadsize) {
					op->payload = malloc(op->payloadsize);
					memset(op->payload, 0, op->payloadsize);
					link_read(comm->lnk, op->payload, op->payloadsize, stoptime);
				} else {
					op->payload = NULL;
				}
			}
			break;
	}
	if(complete)
		return 0;
	else
		return -1;
}

int worker_comm_test_results(struct worker_comm *comm)
{
	int stoptime, complete = 0;
	char line[WQ_LINE_MAX];
	stoptime = time(0) + comm->active_timeout;
	
	switch(comm->type) {
		case WORKER_COMM_MPI:
			if(comm->mpi_req == MPI_REQUEST_NULL) {
				MPI_Irecv(&comm->results, 1, MPI_INT, comm->mpi_rank, 0, MPI_COMM_WORLD, &comm->mpi_req);
			}
			while(time(0) < stoptime && !complete) {
				MPI_Test(&comm->mpi_req, &complete, &comm->mpi_stat);
			}
			break;
		case WORKER_COMM_TCP:
			complete = work_queue_readline(comm->lnk, line, WQ_LINE_MAX, stoptime);
			if(complete) {
				complete = sscanf(line, "%d", &comm->results);
			}
			break;
	}


	if(!complete)
		return -1;
	
	return comm->results;
}


