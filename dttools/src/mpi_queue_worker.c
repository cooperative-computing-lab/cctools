/*
Copyright (C) 2011- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "cctools.h"
#include "mpi_queue.h"
#include "copy_stream.h"
#include "domain_name_cache.h"
#include "link.h"
#include "list.h"
#include "stringtools.h"
#include "itable.h"
#include "debug.h"
#include "create_dir.h"

#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <math.h>
#include <signal.h>
#include <dirent.h>

#include <mpi.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/signal.h>


#define MPI_QUEUE_LINE_MAX 256

// MPI_QUEUE op codes must be even, as they may be bitwise ORed with
// MPI_QUEUE_JOB_FAILED (0x01) to indicate which operation type failed
#define MPI_QUEUE_OP_WORK     2
#define MPI_QUEUE_OP_STAT     4
#define MPI_QUEUE_OP_UNLINK   6
#define MPI_QUEUE_OP_MKDIR    8
#define MPI_QUEUE_OP_CLOSE   10
#define MPI_QUEUE_OP_EXIT    12

#define MPI_QUEUE_JOB_WAITING  0
#define MPI_QUEUE_JOB_FAILED   1
#define MPI_QUEUE_JOB_BUSY     2
#define MPI_QUEUE_JOB_READY    4
#define MPI_QUEUE_JOB_COMPLETE 8

// Maximum time to wait before aborting if there is no connection to the master.
static int idle_timeout = 900;

// Maximum time to wait for a new command from the Makeflow process.
static int short_timeout = 10;

// Maximum time to wait when actively communicating with the master.
static int active_timeout = 3600;

// Flag gets set on receipt of a terminal signal.
static int abort_flag = 0;


static void handle_abort(int sig)
{
	abort_flag = 1;
}

struct mpi_queue_job {
	int jobid;
	int worker_rank;
	int status;
	
	int result;
	int output_length;
	char *output;
	struct list *operations;

	MPI_Request request;
	MPI_Status mpi_status;
};

struct mpi_queue_operation {
	int type;
	char args[MPI_QUEUE_LINE_MAX];
	int jobid;
	int result;
	int buffer_length;
	int output_length;
	char *buffer;
	char *output_buffer;
};


void mpi_queue_job_delete(struct mpi_queue_job *job) {

	if(job->output_length) {
		free(job->output);
	}
	
	while(list_size(job->operations)) {
		struct mpi_queue_operation *tmp = list_pop_head(job->operations);
		if(tmp->buffer_length)
			free(tmp->buffer);
		if(tmp->output_length)
			free(tmp->output_buffer);
		free(tmp);
	}
	list_delete(job->operations);
	free(job);
}

int master_main(const char *host, int port, const char *addr) {
	time_t idle_stoptime;
	struct link *master = NULL;
	int num_workers, i;
	struct mpi_queue_job **workers;

	struct itable *active_jobs = itable_create(0);
	struct itable *waiting_jobs = itable_create(0);
	struct list   *complete_jobs = list_create();

	MPI_Comm_size(MPI_COMM_WORLD, &num_workers);

	workers = malloc(num_workers * sizeof(*workers));
	memset(workers, 0, num_workers * sizeof(*workers));	
	
	idle_stoptime = time(0) + idle_timeout;

	while(!abort_flag) {
		char line[MPI_QUEUE_LINE_MAX];

		if(time(0) > idle_stoptime) {
			if(master) {
				printf("mpi master: gave up after waiting %ds to receive a task.\n", idle_timeout);
			} else {
				printf("mpi master: gave up after waiting %ds to connect to %s port %d.\n", idle_timeout, host, port);
			}
			break;
		}


		if(!master) {
			char working_dir[MPI_QUEUE_LINE_MAX];
			master = link_connect(addr, port, idle_stoptime);
			if(!master) {
				sleep(5);
				continue;
			}

			link_tune(master, LINK_TUNE_INTERACTIVE);
			
			link_readline(master, line, sizeof(line), time(0) + active_timeout);

			memset(working_dir, 0, MPI_QUEUE_LINE_MAX);
			if(sscanf(line, "workdir %s", working_dir) == 1) {
				MPI_Bcast(working_dir, MPI_QUEUE_LINE_MAX, MPI_CHAR, 0, MPI_COMM_WORLD);
			} else {
				link_close(master);
				master = NULL;
				continue;
			}
		}
		
		if(link_readline(master, line, sizeof(line), time(0) + short_timeout)) {
			struct mpi_queue_operation *op;
			int jobid, mode;
			INT64_T length;
			char path[MPI_QUEUE_LINE_MAX];
			op = NULL;
			
			debug(D_MPI, "received: %s\n", line);

			if(!strcmp(line, "get results")) {
				struct mpi_queue_job *job;
				debug(D_MPI, "results requested: %d available\n", list_size(complete_jobs));
				link_putfstring(master, "num results %d\n", time(0) + active_timeout, list_size(complete_jobs));
				while(list_size(complete_jobs)) {
					job = list_pop_head(complete_jobs);
					link_putfstring(master, "result %d %d %d %lld\n", time(0) + active_timeout, job->jobid, job->status, job->result, job->output_length);
					if(job->output_length) {
						link_write(master, job->output, job->output_length, time(0)+active_timeout);
					}
					mpi_queue_job_delete(job);
				}

			} else if(sscanf(line, "work %d %lld", &jobid, &length)) {
				op = malloc(sizeof(*op));
				memset(op, 0, sizeof(*op));
				op->type = MPI_QUEUE_OP_WORK;
				op->buffer_length = length+1;
				op->buffer = malloc(length+1);
				op->buffer[op->buffer_length] = 0;
				link_read(master, op->buffer, length, time(0) + active_timeout);
				op->result = -1;
				
			} else if(sscanf(line, "stat %d %s", &jobid, path) == 2) {
				op = malloc(sizeof(*op));
				memset(op, 0, sizeof(*op));
				op->type = MPI_QUEUE_OP_STAT;
				sprintf(op->args, "%s", path);
				op->result = -1;
				
			} else if(sscanf(line, "unlink %d %s", &jobid, path) == 2) {
				op = malloc(sizeof(*op));
				memset(op, 0, sizeof(*op));
				op->type = MPI_QUEUE_OP_UNLINK;
				sprintf(op->args, "%s", path);
				op->result = -1;
				
			} else if(sscanf(line, "mkdir %d %s %o", &jobid, path, &mode) == 3) {
				op = malloc(sizeof(*op));
				memset(op, 0, sizeof(*op));
				op->type = MPI_QUEUE_OP_MKDIR;
				sprintf(op->args, "%s %o", path, mode);
				op->result = -1;
				
			} else if(sscanf(line, "close %d", &jobid) == 1) {
				op = malloc(sizeof(*op));
				memset(op, 0, sizeof(*op));
				op->type = MPI_QUEUE_OP_CLOSE;
				op->result = -1;
				
//			} else if(sscanf(line, "symlink %d %s %s", &jobid, path, filename) == 3) {
//			} else if(sscanf(line, "put %d %s %lld %o", &jobid, filename, &length, &mode) == 4) {
//			} else if(sscanf(line, "rget %d %s", &jobid, filename) == 2) {
//			} else if(sscanf(line, "get %d %s", &jobid, filename) == 2) {
//			} else if(sscanf(line, "thirdget %d %d %s %[^\n]", &jobid, &mode, filename, path) == 4) {
//			} else if(sscanf(line, "thirdput %d %d %s %[^\n]", &jobid, &mode, filename, path) == 4) {
			} else if(!strcmp(line, "exit")) {
				break;
			} else {
				abort_flag = 1;
				continue;
			}
			if(op) {
				struct mpi_queue_job *job;
					job = itable_lookup(active_jobs, jobid);
				if(!job) {
					job = itable_lookup(waiting_jobs, jobid);
				}
				if(!job) {
					job = malloc(sizeof(*job));
					memset(job, 0, sizeof(*job));
					job->jobid = jobid;
					job->operations = list_create();
					job->status = MPI_QUEUE_JOB_WAITING;
					job->worker_rank = -1;
					itable_insert(waiting_jobs, jobid, job);
				}
				list_push_tail(job->operations, op);
			}
			idle_stoptime = time(0) + idle_timeout;
		} else {
			link_close(master);
			master = 0;
			sleep(5);
		}
		
		int num_waiting_jobs = itable_size(waiting_jobs);
		int num_unvisited_jobs = itable_size(active_jobs);
		for(i = 1; i < num_workers && (num_unvisited_jobs > 0 || num_waiting_jobs > 0); i++) {
			struct mpi_queue_job *job;
			struct mpi_queue_operation *op;
			int flag = 0;
			UINT64_T jobid;

			if(!workers[i]) {
				if(num_waiting_jobs) {
					itable_firstkey(waiting_jobs);
					itable_nextkey(waiting_jobs, &jobid, (void **)&job);
					itable_remove(waiting_jobs, jobid);
					itable_insert(active_jobs, jobid, job);
					workers[i] = job;
					num_waiting_jobs--;
					job->worker_rank = i;
					job->status = MPI_QUEUE_JOB_READY;
				} else {
					continue;
				}
			} else {
				num_unvisited_jobs--;
				if(workers[i]->status == MPI_QUEUE_JOB_BUSY) {
					MPI_Test(&workers[i]->request, &flag, &workers[i]->mpi_status);
					if(flag) {
						op = list_pop_head(workers[i]->operations);
						if(op->output_length) {
							op->output_buffer = malloc(op->output_length);
							MPI_Recv(op->output_buffer, op->output_length, MPI_BYTE, workers[i]->worker_rank, 0, MPI_COMM_WORLD, &workers[i]->mpi_status);
						}
						
						workers[i]->status = MPI_QUEUE_JOB_READY;

						if(op->type == MPI_QUEUE_OP_WORK || op->result < 0) {
							if(workers[i]->output)
								free(workers[i]->output);
							workers[i]->output = op->output_buffer;
							op->output_buffer = NULL;
							workers[i]->output_length = op->output_length;
							workers[i]->result = op->result;
							if(op->result < 0) {
								workers[i]->status = MPI_QUEUE_JOB_FAILED | op->type;
								op->type = MPI_QUEUE_OP_CLOSE;
								list_push_head(workers[i]->operations, op);
								op = NULL;
							}
						}
						if(op) {
							if(op->buffer)
								free(op->buffer);
							if(op->output_buffer)
								free(op->output_buffer);
							free(op);
						}
					}
				}
			}
			
			if( workers[i]->status != MPI_QUEUE_JOB_BUSY && list_size(workers[i]->operations)) {
				op = list_peek_head(workers[i]->operations);
				
				if(op->type == MPI_QUEUE_OP_CLOSE) {
					itable_remove(active_jobs, workers[i]->jobid);
					list_push_tail(complete_jobs, workers[i]);
					if(!(workers[i]->status & MPI_QUEUE_JOB_FAILED))
						workers[i]->status = MPI_QUEUE_JOB_COMPLETE;
					workers[i] = NULL;
					i--;
					continue;
				}
				
				MPI_Send(op, sizeof(*op), MPI_BYTE, workers[i]->worker_rank, 0, MPI_COMM_WORLD);
				if(op->buffer_length) {
					MPI_Send(op->buffer, op->buffer_length, MPI_BYTE, workers[i]->worker_rank, 0, MPI_COMM_WORLD);
					free(op->buffer);
					op->buffer_length = 0;
					op->buffer = NULL;
				}
				MPI_Irecv(op, sizeof(*op), MPI_BYTE, workers[i]->worker_rank, 0, MPI_COMM_WORLD, &workers[i]->request);
				workers[i]->status = MPI_QUEUE_JOB_BUSY;
			}
		}
	}


	/** Clean up waiting & complete jobs, send Exit commands to each worker */
	if(!master) {
		// If the master link hasn't been set up yet
		// the workers will be waiting for the working directory
		char line[MPI_QUEUE_LINE_MAX];
		memset(line, 0, MPI_QUEUE_LINE_MAX);
		MPI_Bcast(line, MPI_QUEUE_LINE_MAX, MPI_CHAR, 0, MPI_COMM_WORLD);
	} else {
		link_close(master);
	}

	for(i = 1; i < num_workers; i++) {
		struct mpi_queue_operation *op, close;
		memset(&close, 0, sizeof(close));
		close.type = MPI_QUEUE_OP_EXIT;
		
		if(workers[i]) {
			if(workers[i]->status == MPI_QUEUE_JOB_BUSY) {
				MPI_Wait(&workers[i]->request, &workers[i]->mpi_status);
				op = list_peek_head(workers[i]->operations);
				
				if(op->output_length) {
					op->output_buffer = malloc(op->output_length);
					MPI_Recv(op->output_buffer, op->output_length, MPI_BYTE, workers[i]->worker_rank, 0, MPI_COMM_WORLD, &workers[i]->mpi_status);
				}
			}
			itable_remove(active_jobs, workers[i]->jobid);
			list_push_tail(complete_jobs, workers[i]);
		}
		MPI_Send(&close, sizeof(close), MPI_BYTE, i, 0, MPI_COMM_WORLD);
	}

	itable_firstkey(waiting_jobs);
	while(itable_size(waiting_jobs)) {
		struct mpi_queue_job *job;
		UINT64_T jobid;

		itable_nextkey(waiting_jobs, &jobid, (void **)&job);
		itable_remove(waiting_jobs, jobid);
		list_push_tail(complete_jobs, job);
	}

	while(list_size(complete_jobs)) {
		mpi_queue_job_delete(list_pop_head(complete_jobs));
	}

	MPI_Finalize();
	return abort_flag;
}


int worker_main() {
	char line[MPI_QUEUE_LINE_MAX];
	struct mpi_queue_operation *op;
	struct stat st;

	op = malloc(sizeof(*op));
	
	MPI_Bcast(line, MPI_QUEUE_LINE_MAX, MPI_CHAR, 0, MPI_COMM_WORLD);
	
	if(strlen(line)) {
		if(stat(line, &st)) {
			debug(D_MPI, "Working directory (%s) does not exist\n", line);
			exit(1);
		}
		chdir(line);
	}

	while(!abort_flag) {
		char filename[MPI_QUEUE_LINE_MAX];
		int mode, result;
		FILE *stream;
		MPI_Status mpi_status;

		memset(op, 0, sizeof(*op));
		MPI_Recv(op, sizeof(*op), MPI_BYTE, 0, 0, MPI_COMM_WORLD, &mpi_status);
		
		switch(op->type) {
			case MPI_QUEUE_OP_WORK:

			op->buffer = malloc(op->buffer_length + 10);
			MPI_Recv(op->buffer, op->buffer_length, MPI_BYTE, 0, 0, MPI_COMM_WORLD, &mpi_status);
			op->buffer[op->buffer_length] = 0;
			strcat(op->buffer, " 2>&1");
			debug(D_MPI, "%s", op->buffer);
			stream = popen(op->buffer, "r");
			if(stream) {
				op->output_length = copy_stream_to_buffer(stream, &op->output_buffer);
				if(op->output_length < 0)
					op->output_length = 0;
				op->result = pclose(stream);
			} else {
				op->result = -1;
				op->output_length = 0;
				op->output_buffer = 0;
			}
			
			break;
			case MPI_QUEUE_OP_STAT:
			
			if(!stat(op->args, &st)) {
				op->result = 1;
				op->output_length = MPI_QUEUE_LINE_MAX;
				op->output_buffer = malloc(op->output_length);
				sprintf(op->output_buffer, "%lu %lu", (unsigned long int) st.st_size, (unsigned long int) st.st_mtime);
			} else {
				op->result = -1;
				op->output_length = MPI_QUEUE_LINE_MAX;
				op->output_buffer = malloc(op->output_length);
				sprintf(op->output_buffer, "0 0");
			}
		
			break;
			case MPI_QUEUE_OP_UNLINK:

			result = remove(op->args);
			if(result != 0) {	// 0 - succeeded; otherwise, failed
				op->result = -1;
			} else {
				op->result = 1;
			}

			break;
			case MPI_QUEUE_OP_MKDIR:
			
			if(sscanf(op->args, "%s %o", filename, &mode) == 2 && create_dir(filename, mode | 0700)) {
				op->result = 1;
			} else {
				op->result = -1;
			}
			
			break;
//		} else if(sscanf(line, "symlink %s %s", path, filename) == 2) {
//		} else if(sscanf(line, "put %s %lld %o", filename, &length, &mode) == 3) {
//		} else if(sscanf(line, "rget %s", filename) == 1) {
//		} else if(sscanf(line, "get %s", filename) == 1) {
//		} else if(sscanf(line, "thirdget %d %s %[^\n]", &mode, filename, path) == 3) {
//		} else if(sscanf(line, "thirdput %d %s %[^\n]", &mode, filename, path) == 3) {
			case MPI_QUEUE_OP_EXIT:

			free(op);
			MPI_Finalize();
			return 0;
			
			break;
			default:

			abort_flag = 1;
		}
		if(abort_flag) break;
		
		if(op->buffer_length) {
			free(op->buffer);
			op->buffer = NULL;
			op->buffer_length = 0;
		}

		MPI_Send(op, sizeof(*op), MPI_BYTE, 0, 0, MPI_COMM_WORLD);
	
		if(op->output_length) {
			MPI_Send(op->output_buffer, op->output_length, MPI_BYTE, 0, 0, MPI_COMM_WORLD);
			free(op->output_buffer);
		}
		
		memset(op, 0, sizeof(*op));

	}
	
	free(op);
	MPI_Finalize();
	return 1;
}

static void show_help(const char *cmd)
{
	printf("Use: %s <masterhost> <port>\n", cmd);
	printf("where options are:\n");
	printf(" -d <subsystem> Enable debugging for this subsystem.\n");
	printf(" -t <time>      Abort after this amount of idle time. (default=%ds)\n", idle_timeout);
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -v             Show version string\n");
	printf(" -w <size>      Set TCP window size.\n");
	printf(" -h             Show this help screen\n");
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

	MPI_Init(&argc, &argv);

	debug_config(argv[0]);

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
			cctools_version_print(stdout, argv[0]);
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

	cctools_version_debug(D_DEBUG, argv[0]);

	if ((argc - optind) != 2) {
	    show_help(argv[0]);
	    return 1;
	}

	host = argv[optind];
	port = atoi(argv[optind + 1]);

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

