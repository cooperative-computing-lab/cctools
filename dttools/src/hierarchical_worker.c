



#define FILE_CACHE_DIR "/tmp/wqh_fc"

struct worker_comm {
	int type;
	int mpi_rank;
	int timeout;
	int results;
	char *hostname;
	struct link *lnk;
	MPI_Request mpi_req;
	MPI_Status mpi_stat;
}

struct worker_file {
	int id;
	int type;
	char *filename;
	int flags;
	int options;
	char *payload;
	int size;
	char label[WQ_FILENAME_MAX];
}

struct worker_op {
	int type;
	int jobid;
	int id;
	int options;
	int flags;
	int payloadsize;
	
	char name[WQ_FILENAME_MAX];
	char *payload;
}

struct worker_job {
	char *command;
	int commandlength;
	char *dirmap
	int dirmaplength;
	int options;

	int status;
	int result;

	char *stdout_buffer;
	int stdout_buffersize;
	char *stderr_buffer;
	int stderr_buffersize;

	struct list *input_files;
	struct list *output_files;
	
	FILE *out;
	int out_fd;
	FILE *err;
	int err_fd;
}

struct worker {
	int workerid;
	char hostname[WQ_FILENAME_MAX];
	int cores;
	int ram;
	int disk;
	
	int role;
	int *jobids;
	struct worker_comm *comm;
}

struct worker_job * worker_job_lookup(struct itable *waiting_jobs, int jobid)
{
	struct worker_job *job = NULL;

	job = itable_lookup(waiting_jobs, jobid);
	if(!job) {
		job = malloc(sizeof(*job));
		memset(job, 0, sizeof(*job));
		job->input_files = list_create();
		job->output_files = list_create();
		itable_insert(waiting_jobs, jobid, job);
	}
	return job;
}

void worker_job_check_files(struct worker_job *job, int filetype) {
	struct worker_file *fileptr;
	struct list *files;
	
	if(filetype == WORKER_FILES_INPUT) {
		files = job->input_files;
	} else {
		files = job->output_files;
	}
	
	list_first_item(files);
	while((fileptr = list_next_item(files))) {
		char filename[WQ_FILENAME_MAX];
		struct stat st;
		
		if(fileptr->type == WORKER_FILE_NORMAL) {
			if(filetype == WORKER_FILES_INPUT) {
				file_cache_contains(file_store, fileptr->label, filename);
			} else {
				sprintf(filename, "%s", fileptr->filename);
			}
		} else if(fileptr->type == WORKER_FILE_REMOTE) {
			sprintf(filename, "%s", fileptr->payload);
		}
		
		if(stat(filename, &st)) {
			if(fileptr->options & WORKER_FILE_OPTIONAL)
				continue;
			job->status = WORKER_JOB_MISSING_FILE;
			break;
		}
		
		
		if(fileptr->type == WORKER_FILES_INPUT) {
			if(!strcmp(fileptr->filename, filename)) {
				continue;
			}
			
			if(stat(fileptr->filename, &st) != 0 && symlink(filename, fileptr->filename) != 0) {
				job->stderr_buffer = strdup(strerror(errno));
				job->stderr_buffersize = strlen(job->stderr_buffer);
				job->status = WORKER_JOB_FAILED_SYMLINK;
				break;
			}
		} else {
			if(fileptr->type == WORKER_FILE_REMOTE)
				continue;
				
			lstat(fileptr->filename, &st);
			if(st.st_mode & S_IFREG) {
				FILE *reg;
				int cache_fd;
				file_cache_contains(file_store, fileptr->label, filename);
				reg = fopen(fileptr->filename, "r");
				cache_fd = open64(filename, O_WRONLY | O_CREAT | O_SYNC | O_TRUNC, fileptr->flags);
				copy_stream_to_fd(reg, cache_fd);
				fclose(reg);
				close(cache_fd);
			}
		}
	}
}

void handle_op(struct worker_comm *super_comm, struct worker_op *op) {
	struct stat st;

	switch(op->type) {
		case WORKER_OP_ROLE:
			worker_comm_disconnect(super_comm);
			worker_comm_connect(super_comm, op, timeout);
			workerdata->role = op->flags;
			worker_comm_send_worker(super_comm, workerdata);
			return;
	
		case WORKER_OP_WORKDIR:
			if(stat(op->name, &st)) {
				debug(D_WQ, "Working directory (%s) does not exist\n", op->name);
				exit(1);
			}
			chdir(op->name);
			break;
	
		case WORKER_OP_CLEAR_CACHE:
			file_cache_cleanup(file_store);
			itable_firstkey(file_table);
			while(itable_nextkey(file_table, &index, &fileptr)) {
				free(fileptr);file
				itable_remove(file_table, index);
			}
			break;
	
		case WORKER_OP_FILE:
			fileptr = itable_remove(file_table, op->id);
			if(fileptr && op->options & WORKER_FILE_FLAG_NOCLOBBER)
				break;
			if(!fileptr) {
				fileptr = malloc(sizeof(*fileptr));
				memset(fileptr, 0, sizeof(*fileptr));
			}
			else
				free(fileptr->filename);
		
			fileptr->id = op->id;
			if(op->options & WORKER_FILE_FLAG_CACHEABLE)
				fileptr->cacheable = 1;
			else
				fileptr->cacheable = 0;
			else if(op->options & WORKER_FILE_FLAG_REMOTEFS)
				fileptr->type = WORKER_FILE_REMOTE;
			else
				fileptr->type = WORKER_FILE_NORMAL;

			fileptr->options = op->options;
			fileptr->filename = strdup(op->name);
			fileptr->flags = op->flags;
			sprintf(fileptr->label, "%d.%s", fileptr->id, fileptr->filename);
		
			if(op->payload)
				fileptr->payload = strdup(op->payload);
			
			itable_insert(file_table, op->id, fileptr);
			break;
	
		case WORKER_OP_FILE_CHECK:
			INT64_T stats[4];

			memset(stats, 0, sizeof(stats));
			stats[0] = -1;
		
			fileptr = itable_lookup(file_table, op->id);
			if(fileptr) {
				char label[WQ_FILENAME_MAX];
				char cachename[WQ_FILENAME_MAX];
				struct stat64 st;

				if(fileptr->cacheable)
					stats[1] &= WORKER_FILE_FLAG_CACHEABLE;
		
				if(fileptr->type == WORKER_FILE_REMOTE) {
					stats[1] &= WORKER_FILE_FLAG_REMOTEFS;
					sprintf(cachename, "%s", fileptr->payload);
				} else {
					sprintf(label, "%d.%s", fileptr->id, fileptr->filename);
					file_cache_contains(file_store, label, cachename);
				}
			
				if(!stat64(cachename, &st)) {
					stats[0] = st.st_size;
					stats[2] = st.st_mtime;
					stats[3] = st.st_mode;
				}
		
			} else {
				stats[1] = WORKER_FILE_FLAG_MISSING;
			}
		
			worker_comm_send_array(super_comm, WORK_COMM_ARRAY_INT64, stats, 4);
			break;
	
		case WORKER_OP_FILE_PUT:
			fileptr = itable_lookup(file_table, op->id);
			if(fileptr) {
				char cachename[WQ_FILENAME_MAX];
				int fd;
			
				if(fileptr->type == WORKER_FILE_NORMAL) {
					file_cache_contains(file_store, fileptr->label, cachename);
				} else if(fileptr->type == WORKER_FILE_REMOTE) {
					sprintf(cachename, "%s", fileptr->payload);
				}
				fd = open64(cachename, O_WRONLY | O_CREAT | O_SYNC | O_TRUNC, op->flags);
				full_write(fd, op->payload, op->payloadsize);
				close(fd);
			}
			break;
	
		case WORKER_OP_FILE_GET:
			fileptr = itable_lookup(file_table, op->id);
			if(fileptr) {
				char label[WQ_FILENAME_MAX];
				char cachename[WQ_FILENAME_MAX];
				struct stat st;
			
				if(fileptr->type == WORKER_FILE_NORMAL) {
					file_cache_contains(file_store, fileptr->label, cachename);
				} else if(fileptr->type == WORKER_FILE_REMOTE) {
					sprintf(cachename, "%s", fileptr->payload);
				}
			
				if(!stat(cachename, &st)) {
					worker_comm_send_file(super_comm, cachename, 1);
				} else {
					worker_comm_send_buffer(super_comm, NULL, 0, 1);
				}
			} else {
				worker_comm_send_buffer(super_comm, NULL, -1, 1);
			}
			break;
	
		case WORKER_OP_RESULTS:
		
			INT64_T num_results = list_size(complete_jobs);
			worker_comm_send_array(super_comm, WORK_COMM_ARRAY_INT64, &num_results, 1);

			while(list_size(complete_jobs)) {
				job = list_pop_head(complete_jobs);
				worker_comm_send_result(super_comm, job);
				worker_job_delete(job);
				job = NULL;
			}
			break;

		case WORKER_OP_JOB_DIRMAP:
			job = worker_job_lookup(unfinished_jobs, op->jobid);
			job->dirmap = malloc(op->payloadsize);
			job->dirmaplength = op->payloadsize;
			memcpy(job->dirmap, op->payload, op->payloadsize);
			break;
	
		case WORKER_OP_JOB_REQUIRES:
			job = worker_job_lookup(unfinished_jobs, op->jobid);
			fileptr = itable_lookup(file_table, op->id);
			if(fileptr)
				list_push_tail(job->input_files, fileptr);
			break;
		
		case WORKER_OP_JOB_GENERATES:
			job = worker_job_lookup(unfinished_jobs, op->jobid);
			fileptr = itable_lookup(file_table, op->id);
			if(fileptr)
				list_push_tail(job->output_files, fileptr);
			break;
	
		case WORKER_OP_JOB_CMD:
			job = worker_job_lookup(unfinished_jobs, op->jobid);
			if(job->command)
				free(job->command);
			job->command = strdup(op->payload);
			job->commandlength = op->payloadsize;
			job->options = op->options;
			break;
	
		case WORKER_OP_JOB_CLOSE:
			job = itable_remove(unfinished_jobs, op->jobid);
			if(job) {
				job->status = WORKER_JOB_STATUS_READY;
				list_push_tail(waiting_jobs, job);
			}
			break;
	}

}

void foreman_main() {
	struct list* tmp_workers;
	struct worker_op results_request;

	memset(&results_request, 0, sizeof(results_request));
	results_request.type = WORKER_OP_RESULTS;

	/* Wait for new connections */
	while(stoptime > time(0)) {
		struct worker *new_worker = worker_comm_accept_connections(/*TYPE*/, stoptime);
		if(new_worker) {
			list_push_tail(active_workers, new_worker);
		}
	}

	/* Service each worker */
	num_waiting_jobs = list_size(waiting_jobs);
	while(worker = list_pop_head(active_workers)) {
		/* If the worker is full, see if has anything done */
		if(!worker->open_cores) {
			int num_results;
			num_results = worker_comm_test_results(worker->comm);
			/* If it has available results, fetch them */
			if(num_results > 0) {
				worker->state = WORKER_STATE_AVAILABLE;
				worker->open_cores += num_results;
				for(i = 0; i < num_results; i++) {
					int jobid, stdout_buffersize, stderr_buffersize;
					int result_status[3];
					char *stdout_buffer, *stderr_buffer;
					
					jobid = worker_comm_receive_result(worker->comm, result_status, &stdout_buffer, &stdout_buffersize, &stderr_buffer, &stderr_buffersize);
					job = itable_remove(active_jobs, jobid);
					job->status = result_status[1];
					job->result = result_status[2];
					job->stdout_buffer = stdout_buffer;
					job->stdout_buffersize = stdout_buffersize;
					job->stderr_buffer = stderr_buffer;
					job->stderr_buffersize = stderr_buffersize;
					
					worker_comm_handle_files(worker->comm, job->output_files, WORKER_FILES_OUTPUT);
					list_push_tail(complete_jobs, job);
				}
			}
			/* If there was an actual response, but nothing was ready, ask it again for results */
			if(num_results == 0) {
				worker_comm_send_op(worker->comm, &request_results);
			}
		}
		/* If there are available jobs and the worker has any open spaces, give it a job */
		if(num_waiting_jobs && worker->open_cores) {
			/* May want to implement a more complicated (read: cache-aware) job selection algorithm */
			job = list_pop_head(waiting_jobs);
			num_waiting_jobs--;
			worker_comm_handle_files(worker->comm, job->input_files, WORKER_FILES_INPUT);
			if(job->dirmap) {
				memset(op, 0, sizeof(*op));
				op->type = WORKER_OP_JOB_DIRMAP;
				op->jobid = job->id;
				op->payloadsize = job->dirmaplength;
				op->payload = job->dirmap;
				worker_comm_send_op(worker->comm, op);
			}
			list_first_item(job->input_files)
			memset(op, 0, sizeof(*op));
			while((fileptr = list_next_item(job->input_files)) {
				op->type = WORKER_OP_JOB_REQUIRES;
				op->jobid = job->id;
				op->id = fileptr->id;
				worker_comm_send_op(worker->comm, op);
			}
			
			list_first_item(job->output_files);
			memset(op, 0, sizeof(*op));
			while((fileptr = list_next_item(job->output_files)) {
				op->type = WORKER_OP_JOB_GENERATES;
				op->jobid = job->id;
				op->id = fileptr->id;
				worker_comm_send_op(worker->comm, op);
			}
			memset(op, 0, sizeof(*op));
			op->type = WORKER_OP_JOB_CMD;
			op->jobid = job->id;
			op->options = job->options;
			op->payload = job->command;
			op->payloadsize = job->commandlength;
			worker_comm_send_op(worker->comm, op);
			
			memset(op, 0, sizeof(*op));
			op->type = WORKER_OP_JOB_CLOSE;
			op->jobid = job->id;
			worker_comm_send_op(worker->comm, op);
			
			worker_comm_send_op(worker->comm, &request_results);
			worker->open_cores--;
		}
		/* If the worker still has open cores, and there are any jobs available, send it to the end of the queue */
		if(num_waiting_jobs && worker->open_cores)
			list_push_tail(active_workers);
		else /* Otherwise, mark it as visited and full */
			list_push_tail(checked_workers);
		
		
	}
	
	/* Swap "checked" and "ready" lists */
	tmp_workers = active_workers;
	active_workers = checked_workers;
	checked_workers = active_workers;
}

void worker_main() {
	int maxfd = 0;
	fd_set worker_fds;
	struct timeval short_timeout_st;
	short_timeout_st.tv_sec = short_timeout;
	short_timeout_st.tv_usec = 0;
	
	
	/* run through the list of active jobs, figure out which ones have output available */

	FD_ZERO(&worker_fds);
	itable_firstkey(active_jobs);
	while(itable_nextkey(active_jobs, &jobid, &job)) {
		if(job->options & WORKER_JOB_STDOUT || job->options & WORKER_JOB_COMBINED) {
			FD_SET(&worker_fds, job->out_fd);
			maxfd = MAX(job->out_fd, maxfd);
		}
		if(job->options & WORKER_JOB_STDERR) {
			FD_SET(&worker_fds, job->err_fd);
			maxfd = MAX(job->err_fd, maxfd);
		}
	}
	num_ready_procs = select(maxfd, worker_fds, NULL, NULL, &short_timeout_st);


	/* for each process with any output, copy the output into the job's stdout or stderr buffer, and check for end of file */
	if(num_ready_procs > 0) {
		itable_firstkey(active_jobs);
		while(itable_nextkey(active_jobs, &jobid, &job)) {
			int len;
			char *buffer;
			if(FD_ISSET(&worker_fds, job->err_fd)) {
				len = copy_stream_to_buffer(job->err, &buffer);
				job->stderr_buffer = realloc(job->stderr_buffer, job->stderr_buffersize+len);
				memcpy(job->stderr_buffer + job->stderr_buffersize, buffer, len);
				job->stderr_buffersize += len;
				free(buffer);
			}
			if(FD_ISSET(&worker_fds, job->out_fd)) {
				len = copy_stream_to_buffer(job->out, &buffer);
				job->stdout_buffer = realloc(job->stdout_buffer, job->stdout_buffersize+len);
				memcpy(job->stdout_buffer + job->stdout_buffersize, buffer, len);
				job->stdout_buffersize += len;
				free(buffer);
			}
			if( (!job->err || feof(job->err)) && (!job->out || feof(job->out)) ) {
				job = itable_remove(active_jobs, jobid);
				worker_job_check_files(job, WORKER_FILES_OUTPUT);
				list_push_tail(complete_jobs, job);
			}
		}
	}
	
	/* If cores are open, start running new jobs */
	
	while(itable_size(active_jobs) < available_cores && list_size(waiting_jobs)) {
		job = list_pop_head(waiting_jobs);
		if(job) {
			FILE **out = NULL, **err = NULL;
			
			if(job->dirmap) {
				char *tok, *dir;
				dir = strtok_r(job->dirmap, ";", &tok);
				if(dir) do {
					create_dir(dir, 0700);
				} while((dir = strtok_r(NULL, ";", &tok)));
			}
			
			worker_job_check_files(job, WORKER_FILES_INPUT);
			if(job->status != WORKER_JOB_READY) {
				list_push_tail(complete_jobs, job);
				continue;
			}
			if(job->options & WORKER_JOB_STDOUT) {
				out = &job->out;
			}
			if(job->options & WORKER_JOB_STDERR) {
				err = &job->err;
			}
			if(job->options & WORKER_JOB_COMBINED) {
				out = err = &job->out;
			}
			multi_popen(job->command, NULL, out, err);
			if(job->out)
				job->out_fd = fileno(job->out);
			if(job->err)
				job->err_fd = fileno(job->err);
		}
	}
}

void task_main() {
	int role;
	struct worker_op op;
	struct worker_comm super_comm;
	
	struct itable *unfinished_jobs = itable_create(0);
	struct list   *waiting_jobs = list_create();
	struct itable *active_jobs = itable_create(0);
	struct list   *complete_jobs = list_create();
	
	struct itable *file_table = itable_create(0);
	struct file_cache *file_store = file_cache_init(FILE_CACHE_DIR);
	
	struct worker_job *job = NULL;
	struct worker_file *fileptr = NULL;
	
	
	
	
	while(1) {
		int result;
		
		//wait for a new op from current supervisor and handle it if there is one
		memset(&op, 0, sizeof(op));
		result = worker_comm_receive_op(&super_comm, &op);
		if(result >= 0)
			result = handle_op(&super_comm, &op);

		// Proceed with role-based operation
		if(role == FOREMAN)
			foreman_main();
		else
			worker_main();
	}

}







int main(int argc, char *argv[])
{
	const char *host = NULL;
	int port = MPI_QUEUE_DEFAULT_PORT;
	char addr[LINK_ADDRESS_MAX];
	char c;
	int w, rank;

	signal(SIGTERM, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGINT, handle_abort);


	while((c = getopt(argc, argv, "d:ho:t:w:v")) != (char) -1) {
		switch (c) {
		case 'd':
			debug_flags_set(optarg);
			break;
		case 't':
			idle_timeout = string_time_parse(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'v':
			show_version(argv[0]);
			return 0;
		case 'w':
			w = string_metric_parse(optarg);
			link_window_set(w, w);
			break;
		case 'h':
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	host = argv[optind];
	port = atoi(argv[optind + 1]);


	MPI_Init(&argc, &argv);

	debug_config(argv[0]);


	if(!domain_name_cache_lookup(host, addr)) {
		fprintf(stderr, "couldn't lookup address of host %s\n", host);
		exit(1);
	}

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	if(rank) {
		return worker_main();
	} else {
		return master_main(host, port, addr);
	}
	
}

