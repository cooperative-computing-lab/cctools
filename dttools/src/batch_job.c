/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "batch_job.h"
#include "work_queue.h"
#include "itable.h"
#include "debug.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/signal.h>

#define BATCH_JOB_LINE_MAX 1024

struct batch_queue {
	batch_queue_type_t type;
	struct itable *job_table;
	struct itable *output_table;
	struct work_queue *work_queue;
};

static int batch_job_submit_condor( struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files )
{
	FILE *file;
	char line[BATCH_JOB_LINE_MAX];
	int njobs;
	int jobid;

	file = fopen("condor.submit","w");
	if(!file) {
		debug(D_DEBUG,"could not create condor.submit: %s",strerror(errno));
		return -1;
	}

	fprintf(file,"universe = vanilla\n");
	fprintf(file,"executable = %s\n",cmd);
	if(args) fprintf(file,"arguments = %s\n",args);
	if(infile) fprintf(file,"input = %s\n",infile);
	if(outfile) fprintf(file,"output = %s\n",outfile);
	if(errfile) fprintf(file,"error = %s\n",errfile);
	if(extra_input_files) fprintf(file,"transfer_input_files = %s\n",extra_input_files);
	if(extra_output_files) fprintf(file,"transfer_output_files = %s\n",extra_output_files);
	fprintf(file,"should_transfer_files = yes\n");
	fprintf(file,"when_to_transfer_output = on_exit\n");
	fprintf(file,"log = condor.logfile\n");
	fprintf(file,"queue\n");

	fclose(file);

	file = popen("condor_submit condor.submit","r");
	if(!file) return -1;

	while(fgets(line,sizeof(line),file)) {
		if(sscanf(line,"%d job(s) submitted to cluster %d",&njobs,&jobid)==2) {
			pclose(file);
			debug(D_DEBUG,"job %d submitted to condor",jobid);
			struct batch_job_info *info;
			info = malloc(sizeof(*info));
			memset(info,0,sizeof(*info));
			info->submitted = time(0);
			itable_insert(q->job_table,jobid,info);
			return jobid;
		}
	}

	pclose(file);
	debug(D_DEBUG,"failed to submit job to condor!");
	return -1;
}

batch_job_id_t batch_job_wait_condor( struct batch_queue *q, struct batch_job_info *info_out )
{
	static FILE * logfile = 0;
	char line[BATCH_JOB_LINE_MAX];

	if(!logfile) {
		logfile = fopen("condor.logfile","r");
		if(!logfile) {
			debug(D_NOTICE,"couldn't open logfile %s: %s\n","condor.logfile",strerror(errno));
			return -1;
		}
	}

	while(1) {
		if(itable_size(q->job_table)<=0) return 0;

		while(fgets(line,sizeof(line),logfile)) {
			int type, proc, subproc;
			batch_job_id_t jobid;
			time_t current;
			struct tm tm;	

			struct batch_job_info *info;
			int logcode, exitcode;

			if(sscanf(line,"%d (%d.%d.%d) %d/%d %d:%d:%d",
				&type,&jobid,&proc,&subproc,&tm.tm_mon,&tm.tm_mday,&tm.tm_hour,&tm.tm_min,&tm.tm_sec)==9) {
				tm.tm_year = 2008-1900;
				tm.tm_isdst = 0;

				current = mktime(&tm);

				if(type==0) {
					info = itable_lookup(q->job_table,jobid);
					if(info) info->submitted = current;
				} else if(type==1) {
					info = itable_lookup(q->job_table,jobid);
					if(info) {
						 info->started = current;				
						 debug(D_DEBUG,"job %d running now",jobid);
					}
				} else if(type==9) {
					info = itable_remove(q->job_table,jobid);
					if(!info) continue;

					info->finished = current;
					info->exited_normally = 0;
					info->exit_signal = SIGKILL;

					debug(D_DEBUG,"job %d was removed",jobid);
			
					memcpy(info_out,info,sizeof(*info));

					return jobid;
				} else if(type==5) {
					info = itable_remove(q->job_table,jobid);
					if(!info) continue;

					info->finished = current;

					fgets(line,sizeof(line),logfile);
					if(sscanf(line," (%d) Normal termination (return value %d)",&logcode,&exitcode)==2) {
						debug(D_DEBUG,"job %d completed normally with status %d.",jobid,exitcode);
						info->exited_normally = 1;
						info->exit_code = exitcode;
					} else if(sscanf(line," (%d) Abnormal termination (signal %d)",&logcode,&exitcode)==2) {
						debug(D_DEBUG,"job %d completed abnormally with signal %d.",jobid,exitcode);
						info->exited_normally = 0;
						info->exit_signal = exitcode;
					} else {
						debug(D_DEBUG,"job %d completed with unknown status.",jobid);
						info->exited_normally = 0;
						info->exit_signal = 0;
					}

					memcpy(info_out,info,sizeof(*info));
					return jobid;
				}
			}
		}
		sleep(1);
	}

	return -1;
}

int batch_job_remove_condor( struct batch_queue *q, batch_job_id_t jobid )
{
	char line[256];
	FILE *file;

	sprintf(line,"condor_rm %d",jobid);

	file = popen(line,"r");
	if(!file) {
		debug(D_DEBUG,"couldn't run %s",line);
		return 0;
	} else {
		while(fgets(line,sizeof(line),file)) {
			/* nothing */
		}
		pclose(file);
		return 1;
	}
}

static int setup_sge_wrapper( const char *wrapperfile )
{
	FILE *file;

	if(access(wrapperfile,R_OK|X_OK)==0) return 0;

	file = fopen(wrapperfile,"w");
	if(!file) return -1;

	fprintf(file,"#!/bin/sh\n");
	fprintf(file,"logfile=status.${JOB_ID}\n");
	fprintf(file,"starttime=`date +%%s`\n");
	fprintf(file,"$@\n");
	fprintf(file,"stoptime=`date +%%s`\n");
	fprintf(file,"cat > $logfile <<EOF\n");
	fprintf(file,"start $starttime\n");
	fprintf(file,"stop $? $stoptime\n");
	fclose(file);

	chmod(wrapperfile,0755);

	return 0;
}

batch_job_id_t batch_job_submit_sge( struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files )
{
	char line[BATCH_JOB_LINE_MAX];
	batch_job_id_t jobid;
	struct batch_job_info *info;

	FILE *file;

	if(setup_sge_wrapper("sge_wrapper")<0) return -1;

	sprintf(line,"qsub -N wavefront ");
	if(outfile) sprintf(&line[strlen(line)],"-o %s ",outfile);
	if(errfile) sprintf(&line[strlen(line)],"-e %s ",errfile);
	sprintf(&line[strlen(line)],"sge_wrapper %s %s",cmd,args);

	debug(D_DEBUG,"submitting sge job: %s",line);

	file = popen(line,"r");
	if(!file) {
		debug(D_DEBUG,"couldn't submit job: %s",strerror(errno));
		return -1;
	}

	if(fgets(line,sizeof(line),file)) {
		if(sscanf(line,"your job %d",&jobid)==1) {
			debug(D_DEBUG,"job %d submitted",jobid);
			pclose(file);
			info = malloc(sizeof(*info));
			memset(info,0,sizeof(info));
			info->submitted = time(0);
			itable_insert(q->job_table,jobid,info);
			return jobid;
		} else {
			debug(D_DEBUG,"job submission failed: %s",line);
			pclose(file);
			return -1;
		}
	} else {
		debug(D_DEBUG,"job submission failed: no output");
		pclose(file);
		return -1;
	}
}


batch_job_id_t batch_job_wait_sge( struct batch_queue *q, struct batch_job_info *info_out )
{
	struct batch_job_info *info;
	batch_job_id_t jobid;
	FILE *file;
	char statusfile[BATCH_JOB_LINE_MAX];
	char line[BATCH_JOB_LINE_MAX];
	int t,c;

	while(itable_size(q->job_table)) {
		itable_firstkey(q->job_table);
		while(itable_nextkey(q->job_table,&jobid,(void**)&info)) {
			sprintf(statusfile,"status.%d",jobid);
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
		sleep(1);
	}

	return 0;
}

int batch_job_remove_sge( struct batch_queue *q, batch_job_id_t jobid )
{
	char line[BATCH_JOB_LINE_MAX];
	struct batch_job_info *info;

	info = itable_lookup(q->job_table,jobid);
	if(!info) return 0;

	if(!info->started) info->started = time(0);

	info->finished = time(0);
	info->exited_normally = 0;
	info->exit_signal = 1;

	sprintf(line,"qdel %d",jobid);
	system(line);

	return 0;
}

/***************************************************************************************/

int batch_job_submit_work_queue( struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files )
{
	struct work_queue_task *t;
	char *f, *files;
	
	t = work_queue_task_create(cmd,args);

	if(infile) work_queue_task_add_standard_input_file(t,infile);
	if(cmd) work_queue_task_add_extra_staged_file(t,cmd,cmd);
	
	if(extra_input_files) {
		files = strdup(extra_input_files);
		f = strtok(files," \t,");
		while(f) {
			 work_queue_task_add_extra_staged_file(t,f,f);
			 f = strtok(0," \t,");
		}
		free(files);
	}

	if(extra_output_files) {
		files = strdup(extra_output_files);
		f = strtok(files," \t,");
		while(f) {
			work_queue_task_add_extra_created_file(t,f,f);
			f = strtok(0," \t,");
		}
		free(files);
	}

	work_queue_submit(q->work_queue,t);

	itable_insert(q->output_table,t->taskid,strdup(outfile));

	return t->taskid;
}

batch_job_id_t batch_job_wait_work_queue( struct batch_queue *q, struct batch_job_info *info )
{
	struct work_queue_task *t = work_queue_wait(q->work_queue,WAITFORTASK);
	if(t) {
		info->submitted = t->submit_time/1000000;
		info->started = t->start_time/1000000;
		info->finished = t->finish_time/1000000;
		info->exited_normally = 1;
		info->exit_code = t->result;
		info->exit_signal = 0;

		char *outfile = itable_remove(q->output_table,t->taskid);
		if(outfile) {
			FILE *file = fopen(outfile,"w");
			if(file) {
				fwrite(t->output,strlen(t->output),1,file);
				fclose(file);
			}
			free(outfile);			
		}

		int taskid = t->taskid;
		work_queue_task_delete(t);

		return taskid;
	} else {
		return -1;
	}

}

int batch_job_remove_work_queue( struct batch_queue *q, batch_job_id_t jobid )
{
	debug(D_NOTICE,"batch_job: cannot remove task from work_queue");
	return -1;
}

/***************************************************************************************/

int batch_job_submit_unix( struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files )
{
	char line[BATCH_JOB_LINE_MAX];
	batch_job_id_t jobid;

	if(!cmd) return -1;

	if(!args)    args = "";
	if(!infile)  infile = "/dev/null";
	if(!outfile) outfile = "/dev/null";
	if(!errfile) errfile = "/dev/null";

	sprintf(line,"%s %s <%s >%s 2>%s",cmd,args,infile,outfile,errfile);

	fflush(0);

	jobid = fork();
	if(jobid>0) {
		debug(D_DEBUG,"started process %d: %s",jobid,line);
		struct batch_job_info *info = malloc(sizeof(*info));
		memset(info,0,sizeof(*info));
		info->submitted = time(0);
		info->started = time(0);
		itable_insert(q->job_table,jobid,info);
		return jobid;
	} else if(jobid<0) {
		debug(D_DEBUG,"couldn't create new process: %s\n",strerror(errno));
		return -1;
	} else {
		int result = system(line);
		if(WIFEXITED(result)) {
			_exit(WEXITSTATUS(result));
		} else {
			_exit(1);
		}
	}
}

batch_job_id_t batch_job_wait_unix( struct batch_queue *q, struct batch_job_info *info_out )
{
	batch_job_id_t jobid;
	int status;

	while(1) {
		jobid = wait(&status);
		if(jobid>0) {
			struct batch_job_info *info = itable_remove(q->job_table,jobid);
			if(!info) continue;

			info->finished = time(0);
			if(WIFEXITED(status)) {
				info->exited_normally = 1;
				info->exit_code = WEXITSTATUS(status);
			} else {
				info->exited_normally = 0;
				info->exit_signal = WTERMSIG(status);
			}

			memcpy(info_out,info,sizeof(*info));

			return jobid;
		} else if(errno==ESRCH) {
			return 0;
		}
	}
}

int batch_job_remove_unix( struct batch_queue *q, batch_job_id_t jobid )
{
	if(itable_lookup(q->job_table,jobid)) {
		if(kill(jobid,SIGTERM)==0) {
			debug(D_DEBUG,"signalled process %d",jobid);
			return 1;
		} else {
			debug(D_DEBUG,"could not signal process %d: %s\n",jobid,strerror(errno));
			return 0;
		}
	} else {
		debug(D_DEBUG,"process %d is not under my control.\n",jobid);
		return 0;
	}
}

/***************************************************************************************/

batch_queue_type_t batch_queue_type_from_string( const char *str )
{
	if(!strcmp(str,"condor")) return BATCH_QUEUE_TYPE_CONDOR;
	if(!strcmp(str,"sge"))    return BATCH_QUEUE_TYPE_CONDOR;
	if(!strcmp(str,"unix"))   return BATCH_QUEUE_TYPE_CONDOR;
	if(!strcmp(str,"wq"))     return BATCH_QUEUE_TYPE_WORK_QUEUE;
	return BATCH_QUEUE_TYPE_UNKNOWN;
}

const char * batch_queue_type_to_string( batch_queue_type_t t )
{
	switch(t) {
		  case BATCH_QUEUE_TYPE_UNIX:        return "unix";
		  case BATCH_QUEUE_TYPE_CONDOR:      return "condor";
		  case BATCH_QUEUE_TYPE_SGE:         return "sge";
		  case BATCH_QUEUE_TYPE_WORK_QUEUE:  return "wq";
		  default: return "unknown";
	}
}

struct batch_queue * batch_queue_create( batch_queue_type_t type )
{
	struct batch_queue *q;

	if(type==BATCH_QUEUE_TYPE_UNKNOWN) return 0;

	q = malloc(sizeof(*q));
	q->type = type;
	q->job_table = itable_create(0);
	q->output_table = itable_create(0);

	if(type==BATCH_QUEUE_TYPE_WORK_QUEUE) {
		q->work_queue = work_queue_create(9123, time(0)+60);
	} else {
		q->work_queue = 0;
	}

	return q;
}

batch_job_id_t batch_job_submit( struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files )
{
	if(!q->job_table) q->job_table = itable_create(0);

	if(q->type==BATCH_QUEUE_TYPE_UNIX) {
		return batch_job_submit_unix(q,cmd,args,infile,outfile,errfile,extra_input_files,extra_output_files);
	} else if(q->type==BATCH_QUEUE_TYPE_CONDOR) {
		return batch_job_submit_condor(q,cmd,args,infile,outfile,errfile,extra_input_files,extra_output_files);
	} else if(q->type==BATCH_QUEUE_TYPE_SGE) {
		return batch_job_submit_sge(q,cmd,args,infile,outfile,errfile,extra_input_files,extra_output_files);
	} else if(q->type==BATCH_QUEUE_TYPE_WORK_QUEUE) {
		return batch_job_submit_work_queue(q,cmd,args,infile,outfile,errfile,extra_input_files,extra_output_files);
	} else {
		errno = EINVAL;
		return -1;
	}
}

batch_job_id_t batch_job_wait( struct batch_queue *q, struct batch_job_info *info )
{
	if(!q->job_table) q->job_table = itable_create(0);

	if(q->type==BATCH_QUEUE_TYPE_UNIX) {
		return batch_job_wait_unix(q,info);
	} else if(q->type==BATCH_QUEUE_TYPE_CONDOR) {
		return batch_job_wait_condor(q,info);
	} else if(q->type==BATCH_QUEUE_TYPE_SGE) {
		return batch_job_wait_sge(q,info);
	} else if(q->type==BATCH_QUEUE_TYPE_WORK_QUEUE) {
		return batch_job_wait_work_queue(q,info);
	} else {
		errno = EINVAL;
		return -1;
	}
}

int batch_job_remove( struct batch_queue *q, batch_job_id_t jobid )
{
	if(!q->job_table) q->job_table = itable_create(0);

	if(q->type==BATCH_QUEUE_TYPE_UNIX) {
		return batch_job_remove_unix(q,jobid);
	} else if(q->type==BATCH_QUEUE_TYPE_CONDOR) {
		return batch_job_remove_condor(q,jobid);
	} else if(q->type==BATCH_QUEUE_TYPE_SGE) {
		return batch_job_remove_sge(q,jobid);
	} else if(q->type==BATCH_QUEUE_TYPE_WORK_QUEUE) {
		return batch_job_remove_work_queue(q,jobid);
	} else {
		errno = EINVAL;
		return -1;
	}
}

