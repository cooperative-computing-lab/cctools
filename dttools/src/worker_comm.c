

struct worker * worker_comm_accept_connection(int type, int port, MPI_Comm communicator, int timeout, int stoptime)
{
	static struct list *available_workers = NULL;
	static struct link *master_link = NULL;
	static int master_port = -1;
	
	static MPI_Request mpi_current_request = MPI_REQUEST_NULL;
	static MPI_Comm mpi_communicator = MPI_COMM_WORLD;
	static int mpi_num_workers = -1;

	static struct worker *worker_info = NULL;
	struct worker_comm *comm;
	int mpi_init = 0, sleeptime;

	switch(type) {
		case WORKER_COMM_MPI:
			MPI_Initialized(&mpi_init);
			if(!mpi_init)
				return NULL;
			break;
		case WORKER_COMM_TCP:
			if(!comm->lnk)
				return -1;
			break;
	}


	if(!available_workers) {
		available_workers = list_create();
	}
	if(list_size(available_workers)) {
		return list_pop_head(available_workers);
	}
	if(mpi_init && num_mpi_workers < 0) {
		MPI_Comm_size(mpi_communicator, &mpi_num_workers);
	}

	switch(type) {
		case WORKER_COMM_MPI: {
			
			while(time(0) < stoptime) {
				int complete = 0;
				MPI_Status mpi_stat;
				static struct worker *mpi_worker_info = NULL;
			 
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
					comm->timeout = timeout;
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
		}
		break;
		
		case WORKER_COMM_TCP:
			if(port != master_port) {
				link_close(master_link);
				master_link = NULL;
			}
			
			if(!master_link) {
				if(!port || port < 0)
					return NULL;
				master_port = port;
				master_link = link_serve(port);
			}
			
			// If the master link was awake, then accept as many workers as possible.
			sleeptime = (stoptime - time(0)) * 1000000;
			if(link_usleep(master_link, sleeptime, 1, 0)) {
				do {
					worker_info = malloc(sizeof(*worker_info));
					comm = malloc(sizeof(*comm));
					
					comm->lnk = link_accept(master_link, stoptime);
					if(comm->lnk) {
						link_tune(comm->lnk, LINK_TUNE_INTERACTIVE);
						
						/* Recieve worker description */
					} else {
						free(worker_info);
						free(comm);
						return NULL;
					}
					
					list_push_tail(available_workers, worker_info);
				} while(link_usleep(master_link, 0, 1, 0));
			}
		break;
	}
	
	if(list_size(available_workers)) {
		return list_pop_head(available_workers);
	} else {
		return NULL;
	}

}

int worker_comm_connect(struct worker_comm *comm, struct worker_op *op, int timeout)
{
	int mpi_init = 0;
	
	MPI_Initialized(&mpi_initi);
	if(op->payloadsize <= 0 && !mpi_init)
		return -1;
	
	comm->timeout = timeout;

	if(op->payloadsize <= 0) {
		// Use MPI
		comm->type = WORKER_COMM_MPI;
		comm->mpi_rank = op->id;
		comm->lnk = NULL;
	} else {
		// Use LINK
		comm->type = WORKER_COMM_TCP;
		comm->mpi_rank = -1;
		comm->lnk = link_connect(op->payload, op->id, timeout);
		if(!comm->lnk)
			return -1;
	}
	
	return 0;
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

int worker_comm_send_worker(struct worker_comm *comm, struct worker *worker)
{
	int mpi_init, stoptime;
	switch(comm->type) {
		case WORKER_COMM_MPI:
			MPI_Initialized(&mpi_init);
			if(!mpi_init)
				return -1;
			MPI_Send(worker, sizeof(*worker), MPI_BYTES, comm->mpi_rank, 0, MPI_COMM_WORLD);
			break;
		case WORKER_COMM_TCP:
			if(!comm->lnk)
				return -1;
			stoptime = time(0) + comm->timeout;
			link_putfstring(comm->lnk, "%s %d %d %d\n", stoptime, worker->hostname, worker->cores, worker->ram, worker->disk);
			
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
			if(!comm->lnk)
				return -1;
			stoptime = time(0) + comm->timeout;
			break;
	}
	
	switch(datatype) {
		case WORKER_COMM_ARRAY_INT:
			if(comm->type == WORKER_COMM_MPI) {
				MPI_Send(&length, 1, MPI_INT, comm->mpi_rank, 0, MPI_COMM_WORLD);
				MPI_Send(buf, length, MPI_INT, comm->mpi_rank, 0, MPI_COMM_WORLD);
			}
			if(comm->type == WORKER_COMM_TCP) {
				link_putfstring(comm->lnk, "%d\n", stoptime, length);
				for(i = 0; i < length; i++)
					link_putfstring(comm->lnk, "%d ", stoptime, ((int*)buf)[i]);
				link_putfstring(comm->lnk, "\n", stoptime);
			}
			break;

		case WORKER_COMM_ARRAY_CHAR:
			if(comm->type == WORKER_COMM_MPI) {
				MPI_Send(&length, 1, MPI_INT, comm->mpi_rank, 0, MPI_COMM_WORLD);
				MPI_Send(buf, length, MPI_CHAR, comm->mpi_rank, 0, MPI_COMM_WORLD);
			}
			if(comm->type == WORKER_COMM_TCP) {
				link_putfstring(comm->lnk, "%d\n", stoptime, length);
				link_write(comm->lnk, (char *)buf, sizeof(char)*length, stoptime);
			}
			break;

		case WORKER_COMM_ARRAY_FLOAT:
			if(comm->type == WORKER_COMM_MPI) {
				MPI_Send(&length, 1, MPI_INT, comm->mpi_rank, 0, MPI_COMM_WORLD);
				MPI_Send(buf, length, MPI_FLOAT, comm->mpi_rank, 0, MPI_COMM_WORLD);
			}
			if(comm->type == WORKER_COMM_TCP) {
				link_putfstring(comm->lnk, "%d\n", stoptime, length);
				for(i = 0; i < length; i++)
					link_putfstring(comm->lnk, "%f ", stoptime, ((float*)buf)[i]);
				link_putfstring(comm->lnk, "\n", stoptime);
			}
			break;

		case WORKER_COMM_ARRAY_DOUBLE:
			if(comm->type == WORKER_COMM_MPI) {
				MPI_Send(&length, 1, MPI_INT, comm->mpi_rank, 0, MPI_COMM_WORLD);
				MPI_Send(buf, length, MPI_DOUBLE, comm->mpi_rank, 0, MPI_COMM_WORLD);
			}
			if(comm->type == WORKER_COMM_TCP) {
				link_putfstring(comm->lnk, "%d\n", stoptime, length);
				for(i = 0; i < length; i++)
					link_putfstring(comm->lnk, "%f ", stoptime, ((double*)buf)[i]);
				link_putfstring(comm->lnk, "\n", stoptime);
			}
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
			stoptime = time(0) + comm->timeout;
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
			stoptime = time(0) + comm->timeout;
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
			stoptime = time(0) + comm->timeout;
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

int worker_comm_send_file(struct worker_comm *comm, const char *filename, int length, char header) {
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
			stoptime = time(0)+comm->timeout;
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
			stoptime = time(0)+comm->timeout;
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
			stoptime = time(0)+comm->timeout;
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
	stoptime = time(0)+comm->timeout;
	
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
			stoptime = time(0) + comm->timeout;
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

int worker_comm_send_result(struct worker_comm *comm, struct worker_job *job)
{
	int results[3];
	
	results[0] = job->id;
	results[1] = job->status;
	results[2] = job->result;
	worker_comm_send_array(comm, WORK_COMM_ARRAY_INT, results, 3);
	worker_comm_send_buffer(comm, job->stdout_buffer, job->stdout_buffersize, 1);
	worker_comm_send_buffer(comm, job->stderr_buffer, job->stderr_buffersize, 1);
	return 0;
}

void worker_comm_receive_result(struct worker_comm *comm, int* results_buffer, char **stdout_buf, int *stdout_bufsize, char **stderr_buf, int *stderr_bufsize)
{
	worker_comm_recv_array(comm, WORKER_COMM_ARRAY_INT, results_buffer, 3);
	worker_comm_recv_buffer(comm, stdout_buf, stdout_bufsize, 1);
	worker_comm_recv_buffer(comm, stderr_buf, stderr_bufsize, 1);
}

int worker_comm_handle_files(struct worker_comm *comm, struct list *files, int direction)
{
	struct worker_file *file;
	struct worker_op op;
	struct stat st;

	list_first_item(files);
	while((fileptr = list_next_item(files))) {

		int file_status[3];
		char cachename[WQ_FILENAME_MAX];
		char *buffer;
		int buffersize;
		
		memset(&op, 0, sizeof(op));
		op.type = WORKER_OP_FILE_CHECK;
		op.id = fileptr->id;
		worker_comm_send_op(comm, &op);
		worker_comm_recv_array(comm, WORKER_COMM_ARRAY_INT, file_status, 3);
		
		if(fileptr->type == WORKER_FILE_NORMAL) {
			file_cache_contains(file_store, fileptr->label, cachename);
		} else if(fileptr->type == WORKER_FILE_REMOTE) {
			sprintf(cachename, "%s", fileptr->payload);
		}
		if(stat(cachename, &st))
			return -1;

		switch(direction) {
			case WORKER_FILES_INPUT:
				
				if(file_status[0] < 0) {
					memset(&op, 0, sizeof(op));
					op.type = WORKER_OP_FILE;
					op.id = fileptr->id;
					op.options = fileptr->options;
					sprintf(op.name, "%s", fileptr->filename);
					op.payload = fileptr->payload;
					worker_comm_send_op(comm, &op);
					
					if(fileptr->type == WORKER_FILE_REMOTE) {
						memset(&op, 0, sizeof(op));
						op.type = WORKER_OP_FILE_CHECK;
						op.id = fileptr->id;
						worker_comm_send_op(comm, &op);
						worker_comm_recv_array(comm, WORKER_COMM_ARRAY_INT, file_status, 3);
					}
				}
				
				
				if(file_status[0] != st.st_size || file_status[2] <= st.st_mtime) {
					memset(&op, 0, sizeof(op));
					op.type = WORKER_OP_FILE_PUT;
					op.id = fileptr->id;
					if(file_status[0] <= 0)
						op.type = WORKER_FILE_NORMAL;
					else
						op.type = fileptr->type;
					
					op.payloadsize = st.st_size;
					op.payload = NULL;
					worker_comm_send_op(comm, &op);
					worker_comm_send_file(comm, cachename, st.st_size, 0);
				}
			break;
			
			case WORKER_FILES_OUTPUT:
			
				if(fileptr->flags & WORKER_FILE_IGNORE)
					continue;
				if(file_status[0] <= 0) {
					if(!fileptr->flags & WORKER_FILE_OPTIONAL)
						return -1;
				}
				if(file_status[2] <= st.st_mtime && file_status[0] == st.st_size)
					continue;
				memset(&op, 0, sizeof(op));
				op.type = WORKER_OP_FILE_GET;
				op.id = fileptr->id;
				worker_comm_send_op(comm, &op);
				worker_comm_recv_buffer(comm, &buffer, &buffersize, 1);
				if(buffersize) {
					FILE *outfile;
					outfile = fopen(cachename, "w");
					if(!outfile) {
						free(buffer);
						return -1;
					}
					full_fwrite(outfile, buffer, buffersize);
					fclose(outfile);
					free(buffer);
				} else {
					return -1;
				}
			break;
		}
	}
}

