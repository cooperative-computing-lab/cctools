/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_job.h"
#include "work_queue.h"
#include "itable.h"
#include "debug.h"
#include "macros.h"
#include "mpi_queue.h"
#include "process.h"
#include "stringtools.h"
#include "timestamp.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>

#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/signal.h>

#define BATCH_JOB_LINE_MAX 8192

struct batch_queue {
	batch_queue_type_t type;
	char *logfile;
	char *options_text;
	struct itable *job_table;
	struct itable *output_table;
	struct itable *hadoop_jobs;
	struct work_queue *work_queue;
	struct mpi_queue *mpi_queue;
};

/***************************************************************************************/

static int batch_job_submit_condor(struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
{
	FILE *file;
	char line[BATCH_JOB_LINE_MAX];
	int njobs;
	int jobid;

	file = fopen("condor.submit", "w");
	if(!file) {
		debug(D_DEBUG, "could not create condor.submit: %s", strerror(errno));
		return -1;
	}

	fprintf(file, "universe = vanilla\n");
	fprintf(file, "executable = %s\n", cmd);
	fprintf(file, "getenv = true\n");
	if(args)
		fprintf(file, "arguments = %s\n", args);
	if(infile)
		fprintf(file, "input = %s\n", infile);
	if(outfile)
		fprintf(file, "output = %s\n", outfile);
	if(errfile)
		fprintf(file, "error = %s\n", errfile);
	if(extra_input_files)
		fprintf(file, "transfer_input_files = %s\n", extra_input_files);
	// Note that we do not use transfer_output_files, because that causes the job
	// to get stuck in a system hold if the files are not created.
	fprintf(file, "should_transfer_files = yes\n");
	fprintf(file, "when_to_transfer_output = on_exit\n");
	fprintf(file, "notification = never\n");
	fprintf(file, "copy_to_spool = true\n");
	fprintf(file, "transfer_executable = true\n");
	fprintf(file, "log = %s\n", q->logfile);
	if(q->options_text)
		fprintf(file, "%s\n", q->options_text);
	fprintf(file, "queue\n");
	fclose(file);

	file = popen("condor_submit condor.submit", "r");
	if(!file)
		return -1;

	while(fgets(line, sizeof(line), file)) {
		if(sscanf(line, "%d job(s) submitted to cluster %d", &njobs, &jobid) == 2) {
			pclose(file);
			debug(D_DEBUG, "job %d submitted to condor", jobid);
			struct batch_job_info *info;
			info = malloc(sizeof(*info));
			memset(info, 0, sizeof(*info));
			info->submitted = time(0);
			itable_insert(q->job_table, jobid, info);
			return jobid;
		}
	}

	pclose(file);
	debug(D_DEBUG, "failed to submit job to condor!");
	return -1;
}

static int setup_condor_wrapper(const char *wrapperfile)
{
	FILE *file;

	if(access(wrapperfile, R_OK | X_OK) == 0)
		return 0;

	file = fopen(wrapperfile, "w");
	if(!file)
		return -1;

	fprintf(file, "#!/bin/sh\n");
	fprintf(file, "eval \"$@\"\n");
	fprintf(file, "exit $?\n");
	fclose(file);

	chmod(wrapperfile, 0755);

	return 0;
}


static int batch_job_submit_simple_condor(struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	if(setup_condor_wrapper("condor.sh") < 0) {
		debug(D_DEBUG, "could not create condor.sh: %s", strerror(errno));
		return -1;
	}

	return batch_job_submit_condor(q, "condor.sh", cmd, 0, 0, 0, extra_input_files, extra_output_files);
}

batch_job_id_t batch_job_wait_condor(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{
	static FILE *logfile = 0;
	char line[BATCH_JOB_LINE_MAX];

	if(!logfile) {
		logfile = fopen(q->logfile, "r");
		if(!logfile) {
			debug(D_NOTICE, "couldn't open logfile %s: %s\n", q->logfile, strerror(errno));
			return -1;
		}
	}

	while(1) {
		/*
		   Note: clearerr is necessary to clear any cached end-of-file condition,
		   otherwise some implementations of fgets (i.e. darwin) will read to end
		   of file once and then never look for any more data.
		 */

		clearerr(logfile);

		while(fgets(line, sizeof(line), logfile)) {
			int type, proc, subproc;
			batch_job_id_t jobid;
			time_t current;
			struct tm tm;

			struct batch_job_info *info;
			int logcode, exitcode;

			if(sscanf(line, "%d (%d.%d.%d) %d/%d %d:%d:%d", &type, &jobid, &proc, &subproc, &tm.tm_mon, &tm.tm_mday, &tm.tm_hour, &tm.tm_min, &tm.tm_sec) == 9) {
				tm.tm_year = 2008 - 1900;
				tm.tm_isdst = 0;

				current = mktime(&tm);

				info = itable_lookup(q->job_table, jobid);
				if(!info) {
					info = malloc(sizeof(*info));
					memset(info, 0, sizeof(*info));
					itable_insert(q->job_table, jobid, info);
				}


				debug(D_DEBUG, "line: %s", line);

				if(type == 0) {
					info->submitted = current;
				} else if(type == 1) {
					info->started = current;
					debug(D_DEBUG, "job %d running now", jobid);
				} else if(type == 9) {
					itable_remove(q->job_table, jobid);

					info->finished = current;
					info->exited_normally = 0;
					info->exit_signal = SIGKILL;

					debug(D_DEBUG, "job %d was removed", jobid);

					memcpy(info_out, info, sizeof(*info));

					return jobid;
				} else if(type == 5) {
					itable_remove(q->job_table, jobid);

					info->finished = current;

					fgets(line, sizeof(line), logfile);
					if(sscanf(line, " (%d) Normal termination (return value %d)", &logcode, &exitcode) == 2) {
						debug(D_DEBUG, "job %d completed normally with status %d.", jobid, exitcode);
						info->exited_normally = 1;
						info->exit_code = exitcode;
					} else if(sscanf(line, " (%d) Abnormal termination (signal %d)", &logcode, &exitcode) == 2) {
						debug(D_DEBUG, "job %d completed abnormally with signal %d.", jobid, exitcode);
						info->exited_normally = 0;
						info->exit_signal = exitcode;
					} else {
						debug(D_DEBUG, "job %d completed with unknown status.", jobid);
						info->exited_normally = 0;
						info->exit_signal = 0;
					}

					memcpy(info_out, info, sizeof(*info));
					return jobid;
				}
			}
		}


		if(itable_size(q->job_table) <= 0)
			return 0;

		if(stoptime != 0 && time(0) >= stoptime)
			return -1;

		if(process_pending())
			return -1;

		sleep(1);
	}

	return -1;
}

int batch_job_remove_condor(struct batch_queue *q, batch_job_id_t jobid)
{
	char line[256];
	FILE *file;

	sprintf(line, "condor_rm %d", jobid);

	file = popen(line, "r");
	if(!file) {
		debug(D_DEBUG, "couldn't run %s", line);
		return 0;
	} else {
		while(fgets(line, sizeof(line), file)) {
			/* nothing */
		}
		pclose(file);
		return 1;
	}
}

/***************************************************************************************/

static int setup_batch_wrapper(const char *sysname, const char *wrapperfile)
{
	FILE *file;

	if(access(wrapperfile, R_OK | X_OK) == 0)
		return 0;

	file = fopen(wrapperfile, "w");
	if(!file)
		return -1;

	fprintf(file, "#!/bin/sh\n");
	fprintf(file, "logfile=%s.status.${JOB_ID}\n", sysname);
	fprintf(file, "starttime=`date +%%s`\n");
	fprintf(file, "cat > $logfile <<EOF\n");
	fprintf(file, "start $starttime\n");
	fprintf(file, "EOF\n\n");
	fprintf(file, "eval \"$@\"\n\n");
	fprintf(file, "status=$?\n");
	fprintf(file, "stoptime=`date +%%s`\n");
	fprintf(file, "cat >> $logfile <<EOF\n");
	fprintf(file, "stop $status $stoptime\n");
	fprintf(file, "EOF\n");
	fclose(file);

	chmod(wrapperfile, 0755);

	return 0;
}

batch_job_id_t batch_job_submit_simple_sge(struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	char line[BATCH_JOB_LINE_MAX];
	char name[BATCH_JOB_LINE_MAX];
	batch_job_id_t jobid;
	struct batch_job_info *info;

	FILE *file;

	if(setup_batch_wrapper("sge", "sge.wrapper") < 0)
		return -1;

	strcpy(name, cmd);
	char *s = strchr(name, ' ');
	if(s)
		*s = 0;

	sprintf(line, "qsub -cwd -o /dev/null -j y -N '%s' %s sge.wrapper \"%s\"", string_basename(name), q->options_text ? q->options_text : "", cmd);

	debug(D_DEBUG, "%s", line);

	file = popen(line, "r");
	if(!file) {
		debug(D_DEBUG, "couldn't submit job: %s", strerror(errno));
		return -1;
	}

	line[0] = 0;
	while(fgets(line, sizeof(line), file)) {
		if(sscanf(line, "Your job %d", &jobid) == 1) {
			debug(D_DEBUG, "job %d submitted", jobid);
			pclose(file);
			info = malloc(sizeof(*info));
			memset(info, 0, sizeof(*info));
			info->submitted = time(0);
			itable_insert(q->job_table, jobid, info);
			return jobid;
		}
	}

	if(strlen(line)) {
		debug(D_NOTICE, "job submission failed: %s", line);
	} else {
		debug(D_NOTICE, "job submission failed: no output from qsub");
	}
	pclose(file);
	return -1;
}

batch_job_id_t batch_job_submit_sge(struct batch_queue * q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
{
	char command[BATCH_JOB_LINE_MAX];

	sprintf(command, "%s %s", cmd, args);

	if(infile)
		sprintf(&command[strlen(command)], " <%s", infile);
	if(outfile)
		sprintf(&command[strlen(command)], " >%s", outfile);
	if(errfile)
		sprintf(&command[strlen(command)], " 2>%s", errfile);

	return batch_job_submit_simple_sge(q, command, extra_input_files, extra_output_files);
}

batch_job_id_t batch_job_wait_sge(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{
	struct batch_job_info *info;
	batch_job_id_t jobid;
	FILE *file;
	char statusfile[BATCH_JOB_LINE_MAX];
	char line[BATCH_JOB_LINE_MAX];
	int t, c;

	while(1) {
		UINT64_T ujobid;
		itable_firstkey(q->job_table);
		while(itable_nextkey(q->job_table, &ujobid, (void **) &info)) {
			jobid = ujobid;
			sprintf(statusfile, "sge.status.%d", jobid);
			file = fopen(statusfile, "r");
			if(file) {
				while(fgets(line, sizeof(line), file)) {
					if(sscanf(line, "start %d", &t)) {
						info->started = t;
					} else if(sscanf(line, "stop %d %d", &c, &t) == 2) {
						debug(D_DEBUG, "job %d complete", jobid);
						if(!info->started)
							info->started = t;
						info->finished = t;
						info->exited_normally = 1;
						info->exit_code = c;
					}
				}
				fclose(file);

				if(info->finished != 0) {
					unlink(statusfile);
					info = itable_remove(q->job_table, jobid);
					*info_out = *info;
					free(info);
					return jobid;
				}
			}
		}

		if(itable_size(q->job_table) <= 0)
			return 0;

		if(stoptime != 0 && time(0) >= stoptime)
			return -1;

		if(process_pending())
			return -1;

		sleep(1);
	}

	return -1;
}

int batch_job_remove_sge(struct batch_queue *q, batch_job_id_t jobid)
{
	char line[BATCH_JOB_LINE_MAX];
	struct batch_job_info *info;

	info = itable_lookup(q->job_table, jobid);
	if(!info)
		return 0;

	if(!info->started)
		info->started = time(0);

	info->finished = time(0);
	info->exited_normally = 0;
	info->exit_signal = 1;

	sprintf(line, "qdel %d", jobid);
	system(line);

	return 1;
}

/***************************************************************************************/

batch_job_id_t batch_job_submit_simple_moab( struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files )
{
	char line[BATCH_JOB_LINE_MAX];
	char name[BATCH_JOB_LINE_MAX];
	batch_job_id_t jobid;
	struct batch_job_info *info;

	FILE *file;

	if(setup_batch_wrapper("moab", "moab.wrapper")<0) return -1;

	strcpy(name,cmd);
	char *s = strchr(name,' ');
	if(s) *s = 0;

	sprintf(line,"msub -d $CWD -o /dev/null -j oe -N '%s' %s moab.wrapper \"%s\"",string_basename(name),q->options_text ? q->options_text : "",cmd);

	debug(D_DEBUG,"%s",line);

	file = popen(line,"r");
	if(!file) {
		debug(D_DEBUG,"couldn't submit job: %s",strerror(errno));
		return -1;
	}

	line[0] = 0;
	while(fgets(line,sizeof(line),file)) {
		if(sscanf(line,"Your job %d",&jobid)==1) {
			debug(D_DEBUG,"job %d submitted",jobid);
			pclose(file);
			info = malloc(sizeof(*info));
			memset(info,0,sizeof(*info));
			info->submitted = time(0);
			itable_insert(q->job_table,jobid,info);
			return jobid;
		}
	}
	
	if (strlen(line)) {
		debug(D_NOTICE,"job submission failed: %s",line);
	} else {
		debug(D_NOTICE,"job submission failed: no output from msub");
	}
	pclose(file);
	return -1;
}

batch_job_id_t batch_job_submit_moab( struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files )
{
	char command[BATCH_JOB_LINE_MAX];

	sprintf(command,"%s %s",cmd,args);

	if(infile) sprintf(&command[strlen(command)]," <%s",infile);
	if(outfile) sprintf(&command[strlen(command)]," >%s",outfile);
	if(errfile) sprintf(&command[strlen(command)]," 2>%s",errfile);
	
	return batch_job_submit_simple_sge(q,command,extra_input_files,extra_output_files);
}

batch_job_id_t batch_job_wait_moab( struct batch_queue *q, struct batch_job_info *info_out, time_t stoptime )
{
	struct batch_job_info *info;
	batch_job_id_t jobid;
	FILE *file;
	char statusfile[BATCH_JOB_LINE_MAX];
	char line[BATCH_JOB_LINE_MAX];
	int t,c;

	while(1) {
		UINT64_T ujobid;
		itable_firstkey(q->job_table);
		while(itable_nextkey(q->job_table,&ujobid,(void**)&info)) {
			jobid = ujobid;
			sprintf(statusfile,"moab.status.%d",jobid);
			file = fopen(statusfile,"r");
			if(file) {
				while(fgets(line,sizeof(line),file)) {
					if(sscanf(line,"start %d",&t)) {
						info->started = t;
					} else if(sscanf(line,"stop %d %d",&c,&t)==2) {
						debug(D_DEBUG,"job %d complete",jobid);
						if(!info->started) info->started = t;
						info->finished = t;
						info->exited_normally = 1;
						info->exit_code = c;
					}
				}
				fclose(file);

				if(info->finished!=0) {
					unlink(statusfile);
					info = itable_remove(q->job_table,jobid);
					*info_out = *info;
					free(info);
					return jobid;
				}
			}
		}

		if(itable_size(q->job_table)<=0) return 0;

		if(stoptime!=0 && time(0)>=stoptime) return -1;

		if(process_pending()) return -1;

		sleep(1);
	}

	return -1;
}

int batch_job_remove_moab( struct batch_queue *q, batch_job_id_t jobid )
{
	char line[BATCH_JOB_LINE_MAX];
	struct batch_job_info *info;

	info = itable_lookup(q->job_table,jobid);
	if(!info) return 0;

	if(!info->started) info->started = time(0);

	info->finished = time(0);
	info->exited_normally = 0;
	info->exit_signal = 1;

	sprintf(line,"mdel %d",jobid);
	system(line);

	return 1;
}



/***************************************************************************************/

void specify_mpi_queue_task_files(struct mpi_queue_task *t, const char *input_files, const char *output_files)
{
	char *f, *p, *files;

	if(input_files) {
		files = strdup(input_files);
		f = strtok(files, " \t,");
		while(f) {
			p = strchr(f, '=');
			if(p) {
				*p = 0;
				mpi_queue_task_specify_file(t, f, MPI_QUEUE_INPUT);
				*p = '=';
			} else {
				mpi_queue_task_specify_file(t, f, MPI_QUEUE_INPUT);
			}
			f = strtok(0, " \t,");
		}
		free(files);
	}

	if(output_files) {
		files = strdup(output_files);
		f = strtok(files, " \t,");
		while(f) {
			p = strchr(f, '=');
			if(p) {
				*p = 0;
				mpi_queue_task_specify_file(t, f, MPI_QUEUE_OUTPUT);
				*p = '=';
			} else {
				mpi_queue_task_specify_file(t, f, MPI_QUEUE_OUTPUT);
			}
			f = strtok(0, " \t,");
		}
		free(files);
	}
}

batch_job_id_t batch_job_submit_simple_mpi_queue( struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files )
{
	struct mpi_queue_task *t;

	t = mpi_queue_task_create(cmd);

	specify_mpi_queue_task_files(t, extra_input_files, extra_output_files);

	mpi_queue_submit(q->mpi_queue, t);

	return t->taskid;
}

batch_job_id_t batch_job_submit_mpi_queue( struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files )
{
	struct mpi_queue_task *t;
	char *full_command;

	if(infile)
		full_command = (char *) malloc((strlen(cmd) + strlen(args) + strlen(infile) + 5) * sizeof(char));
	else
		full_command = (char *) malloc((strlen(cmd) + strlen(args) + 2) * sizeof(char));

	if(!full_command) {
		debug(D_DEBUG, "couldn't create new work_queue task: out of memory\n");
		return -1;
	}

	if(infile)
		sprintf(full_command, "%s %s < %s", cmd, args, infile);
	else
		sprintf(full_command, "%s %s", cmd, args);

	t = mpi_queue_task_create(full_command);

	free(full_command);

	if(infile)
		mpi_queue_task_specify_file(t, infile, MPI_QUEUE_INPUT);
	if(cmd)
		mpi_queue_task_specify_file(t, cmd, MPI_QUEUE_INPUT);

	specify_mpi_queue_task_files(t, extra_input_files, extra_output_files);

	mpi_queue_submit(q->mpi_queue, t);

	if(outfile) {
		itable_insert(q->output_table, t->taskid, strdup(outfile));
	}

	return t->taskid;
}

batch_job_id_t batch_job_wait_mpi_queue( struct batch_queue *q, struct batch_job_info *info, time_t stoptime )
{
	static FILE *logfile = 0;
//	struct work_queue_stats s;

	int timeout, taskid = -1;

	if(!logfile) {
		logfile = fopen(q->logfile, "a");
		if(!logfile) {
			debug(D_NOTICE, "couldn't open logfile %s: %s\n", q->logfile, strerror(errno));
			return -1;
		}
	}

	if(stoptime == 0) {
		timeout = MPI_QUEUE_WAITFORTASK;
	} else {
		timeout = MAX(0, stoptime - time(0));
	}

	struct mpi_queue_task *t = mpi_queue_wait(q->mpi_queue, timeout);
	if(t) {
		info->submitted = t->submit_time / 1000000;
		info->started = t->start_time / 1000000;
		info->finished = t->finish_time / 1000000;
		info->exited_normally = 1;
		info->exit_code = t->return_status;
		info->exit_signal = 0;

		/*
		   If the standard ouput of the job is not empty,
		   then print it, because this is analogous to a Unix
		   job, and would otherwise be lost.  Important for
		   capturing errors from the program.
		 */

		if(t->output && t->output[0]) {
			if(t->output[1] || t->output[0] != '\n') {
				string_chomp(t->output);
				printf("%s\n", t->output);
			}
		}

		char *outfile = itable_remove(q->output_table, t->taskid);
		if(outfile) {
			FILE *file = fopen(outfile, "w");
			if(file) {
				fwrite(t->output, strlen(t->output), 1, file);
				fclose(file);
			}
			free(outfile);
		}
		fprintf(logfile, "TASK %llu %d %d %d %llu %llu \"%s\" \"%s\"\n", timestamp_get(), t->taskid, t->result, t->return_status, t->submit_time, t->finish_time, t->tag ? t->tag : "", t->command_line);

		taskid = t->taskid;
		mpi_queue_task_delete(t);
	}
	// Print to work queue log since status has been changed.
//	mpi_queue_get_stats(q->mpi_queue, &s);
//	fprintf(logfile, "QUEUE %llu %d %d %d %d %d\n", timestamp_get(), s.tasks_running, s.tasks_waiting, s.tasks_complete, s.total_tasks_dispatched, s.total_tasks_complete);
//	fflush(logfile);
//	fsync(fileno(logfile));

	if(taskid >= 0) {
		return taskid;
	}

	if(mpi_queue_empty(q->mpi_queue)) {
		return 0;
	} else {
		return -1;
	}
}

int batch_job_remove_mpi_queue(struct batch_queue *q, batch_job_id_t jobid)
{
	return 0;
}


/***************************************************************************************/

void specify_work_queue_task_files(struct work_queue_task *t, const char *input_files, const char *output_files)
{
	char *f, *p, *files;

	if(input_files) {
		files = strdup(input_files);
		f = strtok(files, " \t,");
		while(f) {
			p = strchr(f, '=');
			if(p) {
				*p = 0;
				work_queue_task_specify_input_file(t, f, p + 1);
				debug(D_DEBUG, "local file %s is %s on remote system:", f, p + 1);
				*p = '=';
			} else {
				work_queue_task_specify_input_file(t, f, f);
			}
			f = strtok(0, " \t,");
		}
		free(files);
	}

	if(output_files) {
		files = strdup(output_files);
		f = strtok(files, " \t,");
		while(f) {
			p = strchr(f, '=');
			if(p) {
				*p = 0;
				work_queue_task_specify_output_file(t, f, p + 1);
				debug(D_DEBUG, "remote file %s is %s on local system:", f, p + 1);
				*p = '=';
			} else {
				work_queue_task_specify_output_file(t, f, f);
			}
			f = strtok(0, " \t,");
		}
		free(files);
	}
}

void specify_work_queue_task_shared_files(struct work_queue_task *t, const char *input_files, const char *output_files)
{
	char *f, *p, *files;

	if(input_files) {
		files = strdup(input_files);
		f = strtok(files, " \t,");
		while(f) {
			char fname[WORK_QUEUE_LINE_MAX];
			p = strchr(f, '=');
			if(p) {
				*p = 0;
			}

			if(f[0] != '/') {
				char tmp[WORK_QUEUE_LINE_MAX];
				getcwd(tmp, WORK_QUEUE_LINE_MAX);
				strcat(tmp, "/");
				strcat(tmp, f);
				string_collapse_path(tmp, fname, 1);
			} else {
				strcpy(fname, f);
			}

			if(p) {	
				work_queue_task_specify_file(t, f, p + 1, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE|WORK_QUEUE_THIRDGET);
				debug(D_DEBUG, "shared file %s is %s on remote system:", f, p + 1);
				*p = '=';
			} else {
				work_queue_task_specify_file(t, fname, fname, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE|WORK_QUEUE_THIRDGET);
			}
			f = strtok(0, " \t,");
		}
		free(files);
	}

	if(output_files) {
		files = strdup(output_files);
		f = strtok(files, " \t,");
		while(f) {
			char fname[WORK_QUEUE_LINE_MAX];
			p = strchr(f, '=');
			if(p) {
				*p = 0;
			}

			if(f[0] != '/') {
				char tmp[WORK_QUEUE_LINE_MAX];
				getcwd(tmp, WORK_QUEUE_LINE_MAX);
				strcat(tmp, "/");
				strcat(tmp, f);
				string_collapse_path(tmp, fname, 1);
			} else {
				strcpy(fname, f);
			}

			if(p) {	
				work_queue_task_specify_file(t, fname, p + 1, WORK_QUEUE_OUTPUT, WORK_QUEUE_THIRDPUT);
				debug(D_DEBUG, "shared file %s is %s on remote system:", f, p + 1);
				*p = '=';
			} else {
				work_queue_task_specify_file(t, fname, fname, WORK_QUEUE_OUTPUT, WORK_QUEUE_THIRDPUT);
			}
			f = strtok(0, " \t,");
		}
		free(files);
	}
}

int batch_job_submit_work_queue(struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
{
	struct work_queue_task *t;
	char *full_command;

	if(infile)
		full_command = (char *) malloc((strlen(cmd) + strlen(args) + strlen(infile) + 5) * sizeof(char));
	else
		full_command = (char *) malloc((strlen(cmd) + strlen(args) + 2) * sizeof(char));

	if(!full_command) {
		debug(D_DEBUG, "couldn't create new work_queue task: out of memory\n");
		return -1;
	}

	if(infile)
		sprintf(full_command, "%s %s < %s", cmd, args, infile);
	else
		sprintf(full_command, "%s %s", cmd, args);

	t = work_queue_task_create(full_command);

	free(full_command);

	if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
		if(infile)
			work_queue_task_specify_file(t, infile, infile, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE|WORK_QUEUE_THIRDGET);
		if(cmd)
			work_queue_task_specify_file(t, cmd, cmd, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE|WORK_QUEUE_THIRDGET);

		specify_work_queue_task_shared_files(t, extra_input_files, extra_output_files);
	} else {
		if(infile)
			work_queue_task_specify_input_file(t, infile, infile);
		if(cmd)
			work_queue_task_specify_input_file(t, cmd, cmd);

		specify_work_queue_task_files(t, extra_input_files, extra_output_files);
	}

	work_queue_submit(q->work_queue, t);

	if(outfile) {
		itable_insert(q->output_table, t->taskid, strdup(outfile));
	}

	return t->taskid;
}

int batch_job_submit_simple_work_queue(struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	struct work_queue_task *t;

	t = work_queue_task_create(cmd);

	if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
		specify_work_queue_task_shared_files(t, extra_input_files, extra_output_files);
	} else {
		specify_work_queue_task_files(t, extra_input_files, extra_output_files);
	}

	work_queue_submit(q->work_queue, t);

	return t->taskid;
}

batch_job_id_t batch_job_wait_work_queue(struct batch_queue * q, struct batch_job_info * info, time_t stoptime)
{
	static FILE *logfile = 0;
	struct work_queue_stats s;

	int timeout, taskid = -1;

	if(!logfile) {
		logfile = fopen(q->logfile, "a");
		if(!logfile) {
			debug(D_NOTICE, "couldn't open logfile %s: %s\n", q->logfile, strerror(errno));
			return -1;
		}
	}

	if(stoptime == 0) {
		timeout = WORK_QUEUE_WAITFORTASK;
	} else {
		timeout = MAX(0, stoptime - time(0));
	}

	struct work_queue_task *t = work_queue_wait(q->work_queue, timeout);
	if(t) {
		info->submitted = t->submit_time / 1000000;
		info->started = t->start_time / 1000000;
		info->finished = t->finish_time / 1000000;
		info->exited_normally = 1;
		info->exit_code = t->return_status;
		info->exit_signal = 0;

		/*
		   If the standard ouput of the job is not empty,
		   then print it, because this is analogous to a Unix
		   job, and would otherwise be lost.  Important for
		   capturing errors from the program.
		 */

		if(t->output && t->output[0]) {
			if(t->output[1] || t->output[0] != '\n') {
				string_chomp(t->output);
				printf("%s\n", t->output);
			}
		}

		char *outfile = itable_remove(q->output_table, t->taskid);
		if(outfile) {
			FILE *file = fopen(outfile, "w");
			if(file) {
				fwrite(t->output, strlen(t->output), 1, file);
				fclose(file);
			}
			free(outfile);
		}
		fprintf(logfile, "TASK %llu %d %d %d %d %llu %llu %llu %llu %llu %s \"%s\" \"%s\"\n", timestamp_get(), t->taskid, t->result, t->return_status, t->worker_selection_algorithm, t->submit_time, t->transfer_start_time, t->finish_time,
			t->total_bytes_transferred, t->total_transfer_time, t->host, t->tag ? t->tag : "", t->command_line);

		taskid = t->taskid;
		work_queue_task_delete(t);
	}
	// Print to work queue log since status has been changed.
	work_queue_get_stats(q->work_queue, &s);
	fprintf(logfile, "QUEUE %llu %d %d %d %d %d %d %d %d %d %d %lld %lld\n", timestamp_get(), s.workers_init, s.workers_ready, s.workers_busy, s.tasks_running, s.tasks_waiting, s.tasks_complete, s.total_tasks_dispatched, s.total_tasks_complete,
		s.total_workers_joined, s.total_workers_removed, s.total_bytes_sent, s.total_bytes_received);
	fflush(logfile);
	fsync(fileno(logfile));

	if(taskid >= 0) {
		return taskid;
	}

	if(work_queue_empty(q->work_queue)) {
		return 0;
	} else {
		return -1;
	}
}

int batch_job_remove_work_queue(struct batch_queue *q, batch_job_id_t jobid)
{
	return 0;
}

/***************************************************************************************/

struct batch_job_hadoop_job {
	char jobname[BATCH_JOB_LINE_MAX];
	int jobid;
};

struct batch_job_hadoop_job *batch_job_hadoop_job_create(char *jname)
{
	static int jobid = 1;
	struct batch_job_hadoop_job *job;
	job = malloc(sizeof(*job));
	if(!job)
		return NULL;
	snprintf(job->jobname, BATCH_JOB_LINE_MAX, "%s", jname);
	job->jobid = jobid++;
	return job;
}

static int setup_hadoop_wrapper(const char *wrapperfile, const char *cmd)
{
	FILE *file;

	//if(access(wrapperfile,R_OK|X_OK)==0) return 0;

	file = fopen(wrapperfile, "w");
	if(!file)
		return -1;

	fprintf(file, "#!/bin/sh\n");
	if(cmd)
		fprintf(file, "exec %s\n\n", cmd);
	fclose(file);

	chmod(wrapperfile, 0755);

	return 0;
}

static char *get_hadoop_target_file(const char *input_files)
{
	static char result[BATCH_JOB_LINE_MAX];
	static char match_string[BATCH_JOB_LINE_MAX];
	char *hdfs_root = getenv("HDFS_ROOT_DIR");
	sprintf(match_string, "%s%%[^,]", hdfs_root);
	sscanf(input_files, match_string, result);
	return result;
}

int batch_job_fork_hadoop(struct batch_queue *q, const char *cmd)
{
	int fd_pipe[2];
	int childpid;

	if(pipe(fd_pipe) < 0)
		return -1;

	fflush(NULL);
	debug(D_HDFS, "forking hadoop_status_wrapper\n");
	if((childpid = fork()) < 0) {
		return -1;
	} else if(!childpid) {
		// CHILD
		char line[BATCH_JOB_LINE_MAX];
		char outname[BATCH_JOB_LINE_MAX];
		FILE *cmd_pipe, *parent;
		struct flock lock; {
			lock.l_whence = SEEK_SET;
			lock.l_start = lock.l_len = 0;
			lock.l_pid = getpid();
		}

		close(fd_pipe[0]);
		parent = fdopen(fd_pipe[1], "w");
		setvbuf(parent, NULL, _IOLBF, 0);

		cmd_pipe = popen(cmd, "r");
		if(!cmd_pipe) {
			debug(D_DEBUG, "hadoop_status_wrapper: couldn't submit job: %s", strerror(errno));
			fclose(parent);
			_exit(-1);
		}

		outname[0] = 0;
		while(fgets(line, sizeof(line), cmd_pipe)) {
			char jobname[BATCH_JOB_LINE_MAX];
			char error_string[BATCH_JOB_LINE_MAX];
			int mapdone, reddone;
			if(!strlen(outname) && sscanf(line, "%*s %*s INFO streaming.StreamJob: Running job: %s", jobname) == 1) {
				fprintf(parent, "%s\n", jobname);
				fclose(parent);
				sprintf(outname, "%s.status", jobname);
			} else if(strlen(outname) && sscanf(line, "%*s %*s INFO streaming.StreamJob:\tmap %d%%\treduce %d%%", &mapdone, &reddone) == 2) {
				FILE *output = NULL;
				sprintf(line, "%ld\tM%03d\tR%03d\n", (long int) time(0), mapdone, reddone);

				output = fopen(outname, "w");
				lock.l_type = F_WRLCK;
				fcntl(fileno(output), F_SETLKW, &lock);
				fprintf(output, "%s", line);
				lock.l_type = F_UNLCK;
				fcntl(fileno(output), F_SETLKW, &lock);
				fclose(output);

				debug(D_HDFS, "hadoop_status_wrapper: %s", line);

			} else if(strlen(outname) && sscanf(line, "%*s %*s INFO streaming.StreamJob: Job complete: %s", jobname) == 1) {
				FILE *output = NULL;
				sprintf(line, "%ld\tSUCCESS\t%s\n", (long int) time(0), jobname);

				output = fopen(outname, "w");
				lock.l_type = F_WRLCK;
				fcntl(fileno(output), F_SETLKW, &lock);
				fprintf(output, "%s", line);
				lock.l_type = F_UNLCK;
				fcntl(fileno(output), F_SETLKW, &lock);
				fclose(output);

				debug(D_HDFS, "hadoop_status_wrapper: %s", line);
				_exit(0);
			} else if(strlen(outname) && sscanf(line, "%*s %*s ERROR streaming.StreamJob: %s", error_string) == 1) {
				FILE *output = NULL;
				sprintf(line, "%ld\tFAILURE\t%s\n", (long int) time(0), error_string);

				output = fopen(outname, "w");
				lock.l_type = F_WRLCK;
				fcntl(fileno(output), F_SETLKW, &lock);
				fprintf(output, "%s", line);
				lock.l_type = F_UNLCK;
				fcntl(fileno(output), F_SETLKW, &lock);
				fclose(output);

				debug(D_HDFS, "hadoop_status_wrapper: %s", line);
				_exit(0);
			}
		}
		_exit(0);

	} else {
		//PARENT
		char line[BATCH_JOB_LINE_MAX];
		char jobname[BATCH_JOB_LINE_MAX];
		FILE *child;
		struct batch_job_info *info;
		struct batch_job_hadoop_job *hadoop_job;

		close(fd_pipe[1]);
		child = fdopen(fd_pipe[0], "r");
		setvbuf(child, NULL, _IOLBF, 0);

		jobname[0] = 0;
		while(fgets(line, sizeof(line), child)) {
			if(sscanf(line, "%s", jobname) == 1) {
				break;
			}
		}

		fclose(child);
		if(!strlen(jobname))
			return -1;

		debug(D_HDFS, "jobname received: %s\n", jobname);
		hadoop_job = batch_job_hadoop_job_create(jobname);

		info = malloc(sizeof(*info));
		memset(info, 0, sizeof(*info));
		info->submitted = time(0);
		itable_insert(q->job_table, hadoop_job->jobid, info);
		itable_insert(q->hadoop_jobs, hadoop_job->jobid, hadoop_job);
		debug(D_DEBUG, "job %d (%s) submitted", hadoop_job->jobid, jobname);
		return (hadoop_job->jobid);
	}


}


int batch_job_submit_simple_hadoop(struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	char line[BATCH_JOB_LINE_MAX];
	char *target_file;

	target_file = get_hadoop_target_file(extra_input_files);
	if(!target_file) {
		debug(D_DEBUG, "couldn't create new hadoop task: no input file specified\n");
		return -1;
	} else
		debug(D_HDFS, "input file %s specified\n", target_file);

	setup_hadoop_wrapper("hadoop.wrapper", cmd);

	sprintf(line,
		"$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*-streaming.jar -D mapred.min.split.size=100000000 -input %s -mapper \"$HADOOP_PARROT_PATH ./hadoop.wrapper\" -file hadoop.wrapper -numReduceTasks 0 -output /makeflow_tmp/job-%010d 2>&1",
		target_file, (int) time(0));

	debug(D_HDFS, "%s\n", line);

	return batch_job_fork_hadoop(q, line);

}

int batch_job_submit_hadoop(struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
{
	char command[BATCH_JOB_LINE_MAX];

	sprintf(command, "%s %s", cmd, args);

	if(infile)
		sprintf(&command[strlen(command)], " <%s", infile);
	if(outfile)
		sprintf(&command[strlen(command)], " >%s", outfile);
	if(errfile)
		sprintf(&command[strlen(command)], " 2>%s", errfile);

	return batch_job_submit_simple_hadoop(q, command, extra_input_files, extra_output_files);
}


batch_job_id_t batch_job_wait_hadoop(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{
	struct batch_job_info *info;
	batch_job_id_t jobid;
	char line[BATCH_JOB_LINE_MAX];
	char statusfile[BATCH_JOB_LINE_MAX];
	struct batch_job_hadoop_job *hadoop_job;

	while(1) {
		UINT64_T ujobid;
		itable_firstkey(q->job_table);
		while(itable_nextkey(q->job_table, &ujobid, (void **) &info)) {
			FILE *status;
			struct flock lock; {
				lock.l_whence = SEEK_SET;
				lock.l_start = lock.l_len = 0;
				lock.l_pid = getpid();
			}
			jobid = ujobid;
			hadoop_job = (struct batch_job_hadoop_job *) itable_lookup(q->hadoop_jobs, jobid);

			sprintf(statusfile, "%s.status", hadoop_job->jobname);
			status = fopen(statusfile, "r");

			if(status) {
				int map, red;
				time_t logtime;
				char result[BATCH_JOB_LINE_MAX];
				char message[BATCH_JOB_LINE_MAX];

				line[0] = 0;
				lock.l_type = F_RDLCK;
				fcntl(fileno(status), F_SETLKW, &lock);
				fgets(line, sizeof(line), status);
				lock.l_type = F_UNLCK;
				fcntl(fileno(status), F_SETLKW, &lock);
				fclose(status);

				result[0] = message[0] = 0;
				sscanf(line, "%ld\t%s\t%s", &logtime, result, message);

				if(!strncmp(result, "SUCCESS", 7)) {
					debug(D_DEBUG, "job %d success", jobid);
					info->finished = logtime;
					info->exited_normally = 1;
				} else if(!strncmp(result, "FAILURE", 7)) {
					debug(D_DEBUG, "hadoop execution failed: %s", message);
					info->finished = logtime;
					info->exited_normally = 0;
				}

				if(info->finished != 0) {
					info = itable_remove(q->job_table, jobid);
					*info_out = *info;
					free(info);
					unlink(statusfile);
					return jobid;
				}

				if(sscanf(line, "%ld\tM%d\tR%d", &logtime, &map, &red) == 3) {
					if(map && !info->started)
						info->started = logtime;
					if(red == 100)
						debug(D_DEBUG, "job %d end execution", jobid);
				}

			}
		}

		if(itable_size(q->job_table) <= 0)
			return 0;

		if(stoptime != 0 && time(0) >= stoptime)
			return -1;

		if(process_pending())
			return -1;

		sleep(1);
	}

	return -1;
}


int batch_job_remove_hadoop(struct batch_queue *q, batch_job_id_t jobid)
{
	return 0;
}

/***************************************************************************************/

int batch_job_submit_simple_local(struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	batch_job_id_t jobid;

	fflush(NULL);
	jobid = fork();
	if(jobid > 0) {
		debug(D_DEBUG, "started process %d: %s", jobid, cmd);
		struct batch_job_info *info = malloc(sizeof(*info));
		memset(info, 0, sizeof(*info));
		info->submitted = time(0);
		info->started = time(0);
		itable_insert(q->job_table, jobid, info);
		return jobid;
	} else if(jobid < 0) {
		debug(D_DEBUG, "couldn't create new process: %s\n", strerror(errno));
		return -1;
	} else {
		int result = system(cmd);
		if(WIFEXITED(result)) {
			_exit(WEXITSTATUS(result));
		} else {
			_exit(1);
		}
	}

}

int batch_job_submit_local(struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
{
	char line[BATCH_JOB_LINE_MAX];

	if(!cmd)
		return -1;

	if(!args)
		args = "";
	if(!infile)
		infile = "/dev/null";
	if(!outfile)
		outfile = "/dev/null";
	if(!errfile)
		errfile = "/dev/null";

	sprintf(line, "%s %s <%s >%s 2>%s", cmd, args, infile, outfile, errfile);

	return batch_job_submit_simple_local(q, line, extra_input_files, extra_output_files);
}

batch_job_id_t batch_job_wait_local(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{
	while(1) {
		int timeout;

		if(stoptime > 0) {
			timeout = MAX(0, stoptime - time(0));
		} else {
			timeout = 5;
		}

		struct process_info *p = process_wait(timeout);
		if(p) {
			struct batch_job_info *info = itable_remove(q->job_table, p->pid);
			if(!info) {
				process_putback(p);
				return -1;
			}

			info->finished = time(0);
			if(WIFEXITED(p->status)) {
				info->exited_normally = 1;
				info->exit_code = WEXITSTATUS(p->status);
			} else {
				info->exited_normally = 0;
				info->exit_signal = WTERMSIG(p->status);
			}

			memcpy(info_out, info, sizeof(*info));

			int jobid = p->pid;
			free(p);
			free(info);
			return jobid;

		} else if(errno == ESRCH || errno == ECHILD) {
			return 0;
		}

		if(stoptime != 0 && time(0) >= stoptime)
			return -1;
	}
}

int batch_job_remove_local(struct batch_queue *q, batch_job_id_t jobid)
{
	if(itable_lookup(q->job_table, jobid)) {
		if(kill(jobid, SIGTERM) == 0) {
			debug(D_DEBUG, "signalled process %d", jobid);
			return 1;
		} else {
			debug(D_DEBUG, "could not signal process %d: %s\n", jobid, strerror(errno));
			return 0;
		}
	} else {
		debug(D_DEBUG, "process %d is not under my control.\n", jobid);
		return 0;
	}
}

/***************************************************************************************/

int batch_job_submit_simple_xgrid(struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	batch_job_id_t jobid;
	char line[BATCH_JOB_LINE_MAX];

	fflush(NULL);
	jobid = fork();
	if(jobid > 0) {
		debug(D_DEBUG, "started process %d: xgrid -job run %s", jobid, cmd);
		struct batch_job_info *info = malloc(sizeof(*info));
		memset(info, 0, sizeof(*info));
		info->submitted = time(0);
		info->started = time(0);
		itable_insert(q->job_table, jobid, info);
		return jobid;
	} else if(jobid < 0) {
		debug(D_DEBUG, "couldn't create new process: %s\n", strerror(errno));
		return -1;
	} else {
		sprintf(line, "xgrid -h thirtytwo1.cse.nd.edu -p cse-xgrid -in . -job run %s", cmd);
		int result = system(line);
		if(WIFEXITED(result)) {
			_exit(WEXITSTATUS(result));
		} else {
			_exit(1);
		}
	}

}

int batch_job_submit_xgrid(struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
{
	char line[BATCH_JOB_LINE_MAX];

	if(!cmd)
		return -1;

	if(!args)
		args = "";
	if(!infile)
		infile = "/dev/null";
	if(!outfile)
		outfile = "/dev/null";
	if(!errfile)
		errfile = "/dev/null";

	sprintf(line, "%s %s <%s >%s 2>%s", cmd, args, infile, outfile, errfile);

	return batch_job_submit_simple_local(q, line, extra_input_files, extra_output_files);
}

batch_job_id_t batch_job_wait_xgrid(struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{
	while(1) {
		int timeout;

		if(stoptime > 0) {
			timeout = MAX(0, stoptime - time(0));
		} else {
			timeout = 5;
		}

		struct process_info *p = process_wait(timeout);
		if(p) {
			struct batch_job_info *info = itable_remove(q->job_table, p->pid);
			if(!info) {
				process_putback(p);
				return -1;
			}

			info->finished = time(0);
			if(WIFEXITED(p->status)) {
				info->exited_normally = 1;
				info->exit_code = WEXITSTATUS(p->status);
			} else {
				info->exited_normally = 0;
				info->exit_signal = WTERMSIG(p->status);
			}

			memcpy(info_out, info, sizeof(*info));

			int jobid = p->pid;
			free(p);
			return jobid;

		} else if(errno == ESRCH || errno == ECHILD) {
			return 0;
		}

		if(stoptime != 0 && time(0) >= stoptime)
			return -1;
	}
}

int batch_job_remove_xgrid(struct batch_queue *q, batch_job_id_t jobid)
{
	if(itable_lookup(q->job_table, jobid)) {
		if(kill(jobid, SIGTERM) == 0) {
			debug(D_DEBUG, "signalled process %d", jobid);
			return 1;
		} else {
			debug(D_DEBUG, "could not signal process %d: %s\n", jobid, strerror(errno));
			return 0;
		}
	} else {
		debug(D_DEBUG, "process %d is not under my control.\n", jobid);
		return 0;
	}
}

/***************************************************************************************/

const char *batch_queue_type_string()
{
	return "local, condor, sge, wq, hadoop, mpi-queue";
}

batch_queue_type_t batch_queue_type_from_string(const char *str)
{
	if(!strcmp(str, "condor"))
		return BATCH_QUEUE_TYPE_CONDOR;
	if(!strcmp(str, "sge"))
		return BATCH_QUEUE_TYPE_SGE;
	if(!strcmp(str, "local"))
		return BATCH_QUEUE_TYPE_LOCAL;
	if(!strcmp(str, "unix"))
		return BATCH_QUEUE_TYPE_LOCAL;
	if(!strcmp(str, "wq"))
		return BATCH_QUEUE_TYPE_WORK_QUEUE;
	if(!strcmp(str, "workqueue"))
		return BATCH_QUEUE_TYPE_WORK_QUEUE;
	if(!strcmp(str, "wq-sharedfs"))
		return BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS;
	if(!strcmp(str, "workqueue-sharedfs"))
		return BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS;
	if(!strcmp(str, "xgrid"))
		return BATCH_QUEUE_TYPE_XGRID;
	if(!strcmp(str, "hadoop"))
		return BATCH_QUEUE_TYPE_HADOOP;
	if(!strcmp(str, "mpi"))
		return BATCH_QUEUE_TYPE_MPI_QUEUE;
	if(!strcmp(str, "mpi-queue"))
		return BATCH_QUEUE_TYPE_MPI_QUEUE;
	return BATCH_QUEUE_TYPE_UNKNOWN;
}

const char *batch_queue_type_to_string(batch_queue_type_t t)
{
	switch (t) {
	case BATCH_QUEUE_TYPE_LOCAL:
		return "local";
	case BATCH_QUEUE_TYPE_CONDOR:
		return "condor";
	case BATCH_QUEUE_TYPE_SGE:
		return "sge";
	case BATCH_QUEUE_TYPE_WORK_QUEUE:
		return "wq";
	case BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS:
		return "wq-sharedfs";
	case BATCH_QUEUE_TYPE_XGRID:
		return "xgrid";
	case BATCH_QUEUE_TYPE_HADOOP:
		return "hadoop";
	case BATCH_QUEUE_TYPE_MPI_QUEUE:
		return "mpi-queue";
	default:
		return "unknown";
	}
}

struct batch_queue *batch_queue_create(batch_queue_type_t type)
{
	struct batch_queue *q;

	if(type == BATCH_QUEUE_TYPE_UNKNOWN)
		return 0;

	q = malloc(sizeof(*q));
	q->type = type;
	q->options_text = 0;
	q->job_table = itable_create(0);
	q->output_table = itable_create(0);
	q->hadoop_jobs = NULL;

	if(type == BATCH_QUEUE_TYPE_CONDOR)
		q->logfile = strdup("condor.logfile");
	else if(type == BATCH_QUEUE_TYPE_WORK_QUEUE || type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS)
		q->logfile = strdup("wq.log");
	else
		q->logfile = NULL;

	if(type == BATCH_QUEUE_TYPE_WORK_QUEUE || type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
		q->work_queue = work_queue_create(0);
		if(!q->work_queue) {
			batch_queue_delete(q);
			return 0;
		}
	} else {
		q->work_queue = 0;
	}
	
	if(type == BATCH_QUEUE_TYPE_MPI_QUEUE) {
		q->mpi_queue = mpi_queue_create(0);
		if(!q->mpi_queue) {
			batch_queue_delete(q);
			return 0;
		}
	} else {
		q->mpi_queue = 0;
	}

	if(type == BATCH_QUEUE_TYPE_HADOOP) {
		int fail = 0;

	   if(!getenv("HADOOP_HOME")) {
				debug(D_NOTICE, "error: environment variable HADOOP_HOME not set\n");
				fail = 1;
			}
		if(!getenv("HDFS_ROOT_DIR")) {
				debug(D_NOTICE, "error: environment variable HDFS_ROOT_DIR not set\n");
				fail = 1;
		}
		if(!getenv("HADOOP_PARROT_PATH")) {
			/* Note: HADOOP_PARROT_PATH is the path to Parrot on the remote node, not on the local machine. */
			debug(D_NOTICE, "error: environment variable HADOOP_PARROT_PATH not set\n");
			fail = 1;
		}
	
		if(fail) {
			batch_queue_delete(q);
			return 0;
		}

		q->hadoop_jobs = itable_create(0);
	} else {
		q->hadoop_jobs = NULL;
	}

	return q;
}

void batch_queue_delete(struct batch_queue *q)
{
	if(q) {
		if(q->options_text)
			free(q->options_text);
		if(q->job_table)
			itable_delete(q->job_table);
		if(q->output_table)
			itable_delete(q->output_table);
		if(q->logfile)
			free(q->logfile);
		if(q->work_queue)
			work_queue_delete(q->work_queue);
		if(q->hadoop_jobs)
			itable_delete(q->hadoop_jobs);
		free(q);
	}
}

void batch_queue_set_logfile(struct batch_queue *q, const char *logfile)
{
	if(q->logfile)
		free(q->logfile);
	q->logfile = strdup(logfile);
}

void batch_queue_set_options(struct batch_queue *q, const char *options_text)
{
	if(q->options_text) {
		free(q->options_text);
	}

	if(options_text) {
		q->options_text = strdup(options_text);
	} else {
		q->options_text = 0;
	}
}

batch_job_id_t batch_job_submit(struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files)
{
	if(!q->job_table)
		q->job_table = itable_create(0);

	if(q->type == BATCH_QUEUE_TYPE_LOCAL) {
		return batch_job_submit_local(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_CONDOR) {
		return batch_job_submit_condor(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_SGE) {
		return batch_job_submit_sge(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_MOAB) {
		return batch_job_submit_moab(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
		return batch_job_submit_work_queue(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
		return batch_job_submit_work_queue(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_XGRID) {
		return batch_job_submit_xgrid(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_HADOOP) {
		return batch_job_submit_hadoop(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_MPI_QUEUE) {
		return batch_job_submit_mpi_queue(q, cmd, args, infile, outfile, errfile, extra_input_files, extra_output_files);
	} else {
		errno = EINVAL;
		return -1;
	}
}

batch_job_id_t batch_job_submit_simple(struct batch_queue * q, const char *cmd, const char *extra_input_files, const char *extra_output_files)
{
	if(!q->job_table)
		q->job_table = itable_create(0);

	if(q->type == BATCH_QUEUE_TYPE_LOCAL) {
		return batch_job_submit_simple_local(q, cmd, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_CONDOR) {
		return batch_job_submit_simple_condor(q, cmd, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_SGE) {
		return batch_job_submit_simple_sge(q, cmd, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_MOAB) {
		return batch_job_submit_simple_moab(q, cmd, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
		return batch_job_submit_simple_work_queue(q, cmd, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
		return batch_job_submit_simple_work_queue(q, cmd, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_XGRID) {
		return batch_job_submit_simple_xgrid(q, cmd, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_HADOOP) {
		return batch_job_submit_simple_hadoop(q, cmd, extra_input_files, extra_output_files);
	} else if(q->type == BATCH_QUEUE_TYPE_MPI_QUEUE) {
		return batch_job_submit_simple_mpi_queue(q, cmd, extra_input_files, extra_output_files);
	} else {
		errno = EINVAL;
		return -1;
	}
}

batch_job_id_t batch_job_wait(struct batch_queue * q, struct batch_job_info * info)
{
	return batch_job_wait_timeout(q, info, 0);
}

batch_job_id_t batch_job_wait_timeout(struct batch_queue * q, struct batch_job_info * info, time_t stoptime)
{
	if(!q->job_table)
		q->job_table = itable_create(0);

	if(q->type == BATCH_QUEUE_TYPE_LOCAL) {
		return batch_job_wait_local(q, info, stoptime);
	} else if(q->type == BATCH_QUEUE_TYPE_CONDOR) {
		return batch_job_wait_condor(q, info, stoptime);
	} else if(q->type == BATCH_QUEUE_TYPE_SGE) {
		return batch_job_wait_sge(q, info, stoptime);
	} else if(q->type == BATCH_QUEUE_TYPE_MOAB) {
		return batch_job_wait_moab(q, info, stoptime);
	} else if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
		return batch_job_wait_work_queue(q, info, stoptime);
	} else if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
		return batch_job_wait_work_queue(q, info, stoptime);
	} else if(q->type == BATCH_QUEUE_TYPE_XGRID) {
		return batch_job_wait_xgrid(q, info, stoptime);
	} else if(q->type == BATCH_QUEUE_TYPE_HADOOP) {
		return batch_job_wait_hadoop(q, info, stoptime);
	} else if(q->type == BATCH_QUEUE_TYPE_MPI_QUEUE) {
		return batch_job_wait_mpi_queue(q, info, stoptime);
	} else {
		errno = EINVAL;
		return -1;
	}
}

int batch_job_remove(struct batch_queue *q, batch_job_id_t jobid)
{
	if(!q->job_table)
		q->job_table = itable_create(0);

	if(q->type == BATCH_QUEUE_TYPE_LOCAL) {
		return batch_job_remove_local(q, jobid);
	} else if(q->type == BATCH_QUEUE_TYPE_CONDOR) {
		return batch_job_remove_condor(q, jobid);
	} else if(q->type == BATCH_QUEUE_TYPE_SGE) {
		return batch_job_remove_sge(q, jobid);
	} else if(q->type == BATCH_QUEUE_TYPE_MOAB) {
		return batch_job_remove_moab(q, jobid);
	} else if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
		return batch_job_remove_work_queue(q, jobid);
	} else if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
		return batch_job_remove_work_queue(q, jobid);
	} else if(q->type == BATCH_QUEUE_TYPE_XGRID) {
		return batch_job_remove_xgrid(q, jobid);
	} else if(q->type == BATCH_QUEUE_TYPE_HADOOP) {
		return batch_job_remove_hadoop(q, jobid);
	} else if(q->type == BATCH_QUEUE_TYPE_MPI_QUEUE) {
		return batch_job_remove_mpi_queue(q, jobid);
	} else {
		errno = EINVAL;
		return -1;
	}
}

int batch_queue_port(struct batch_queue *q)
{
	if(q->type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
		return work_queue_port(q->work_queue);
	} else if(q->type == BATCH_QUEUE_TYPE_MPI_QUEUE) {
		return mpi_queue_port(q->mpi_queue);
	} else {
		return 0;
	}
}
