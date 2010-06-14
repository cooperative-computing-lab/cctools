/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_job.h"
#include "chirp_protocol.h"
#include "chirp_builtin.h"
#include "chirp_filesystem.h"

#include "stringtools.h"
#include "debug.h"
#include "full_io.h"

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <dirent.h>
#include <sys/wait.h>
#include <sys/file.h>
#include <sys/stat.h>

extern int enable_identity_boxing;
extern const char *chirp_server_path;
extern const char *chirp_transient_path;
extern pid_t chirp_master_pid;
extern const char *chirp_super_user;

/*
chirp_job_info is an internal structure that describes
everything that we need to execute an active storage job.
Note that it is a superset of chirp_job_state, which
is a public structure used in the Chirp API
*/

struct chirp_job_info {
	INT64_T jobid;
	char   cwd[CHIRP_PATH_MAX];
	char   infile[CHIRP_PATH_MAX];
	char   outfile[CHIRP_PATH_MAX];
	char   errfile[CHIRP_PATH_MAX];
	char   command[CHIRP_PATH_MAX];
	char   args[CHIRP_PATH_MAX];
	char   owner[CHIRP_PATH_MAX];
	int    state;
	int    exit_code;
	time_t submit_time;
	time_t start_time;
	time_t stop_time;
	int    pid;
};

INT64_T chirp_job_create_jobid()
{
	char path[CHIRP_PATH_MAX];
	INT64_T jobid;
	int result;
	int fd;
	struct flock fl;

	sprintf(path,"%s/.__jobs/.__jobid",chirp_transient_path);

	fd = open(path,O_CREAT|O_RDWR,0700);
	if(fd<0) return -1;

	fl.l_type = F_WRLCK;
	fl.l_whence = SEEK_SET;
	fl.l_start = 0;
	fl.l_len = 0;
	fcntl(fd,F_SETLK,&fl);

	result = full_pread(fd,&jobid,sizeof(jobid),0);
	if(result==0) {
		jobid = 1;
	} else if(result==sizeof(jobid)) {
		jobid++;
	} else {
		goto failure;
	}

	result = full_pwrite(fd,&jobid,sizeof(jobid),0);
	if(result==sizeof(jobid)) {
		/* success */
	} else {
		goto failure;
	}

	fl.l_type = F_UNLCK;
	fcntl(fd,F_SETLK,&fl);
	close(fd);
	return jobid;

	failure:
	fl.l_type = F_UNLCK;
	fcntl(fd,F_SETLK,&fl);
	close(fd);
	return -1;
}

void chirp_job_info_to_state( struct chirp_job_info *info, struct chirp_job_state *state )
{
	char *shortcmd = strrchr(info->command,'/');

	if(shortcmd) {
		shortcmd++;
	} else {
		shortcmd = info->command;
	}

	state->jobid = info->jobid;
	strcpy(state->command,shortcmd);
	strcpy(state->owner,info->owner);
	state->state = info->state;
	state->exit_code = info->exit_code;
	state->submit_time = info->submit_time;
	state->start_time = info->start_time;
	state->stop_time = info->stop_time;
	state->pid = info->pid;
}

int chirp_job_info_load( INT64_T jobid, struct chirp_job_info *info )
{
	char path[CHIRP_PATH_MAX];
	FILE *file;
	int result;

	sprintf(path,"%s/.__jobs/%lld",chirp_transient_path,jobid);

	file = fopen(path,"r");
	if(!file) return 0;

	result = fread(info,1,sizeof(*info),file);

	fclose(file);

	if(result==sizeof(*info)) {
		return 1;
	} else {
		return 0;
	}
}

int chirp_job_info_save( INT64_T jobid, struct chirp_job_info *info )
{
	char path[CHIRP_PATH_MAX];
	FILE *file;
	int result;

	sprintf(path,"%s/.__jobs/%lld",chirp_transient_path,jobid);

	file = fopen(path,"w");
	if(!file) return 0;

	result = fwrite(info,1,sizeof(*info),file);

	fclose(file); 

	if(result==sizeof(*info)) {
		debug(D_PROCESS,"jobid %lld: %s",jobid,chirp_job_state_string(info->state));
		/*
		Anytime job state changes, send a HUP to everyone,
		which will terminate a sleep() and possible take action.
		*/
		kill(-chirp_master_pid,SIGHUP);
		return 1;
	} else {
		return 0;
	}
}

void chirp_job_info_delete( INT64_T jobid )
{
	char path[CHIRP_PATH_MAX];
	sprintf(path,"%s/.__jobs/%lld",chirp_transient_path,jobid);
	unlink(path);
	debug(D_PROCESS,"jobid %lld: REMOVED",jobid);
}

/*
This big ugly function actually does the work of executing
and waiting for one job in an active storage mode.
*/

int chirp_job_execute( INT64_T jobid, struct chirp_job_info *info )
{
	int i;
	int argc;
	char **argv;
	int maxfd = sysconf(_SC_OPEN_MAX);
	int fds[3];
	pid_t pid;
	char realargs[CHIRP_LINE_MAX];

	int is_builtin = info->command[0]=='@';

	if(!is_builtin && enable_identity_boxing) {
		sprintf(realargs,"parrot_identity_box %s %s %s",info->owner,info->command,info->args);
	} else {
		sprintf(realargs,"%s %s",info->command,info->args);
	}

	if(!is_builtin && access(info->command,X_OK)!=0) {
		debug(D_PROCESS,"jobid %lld %s is not executable\n",jobid,info->command,strerror(errno));
		goto job_failure;
	}

	fds[0] = open(info->infile,O_RDWR,0);
	if(fds[0]<0) {
		debug(D_PROCESS,"jobid %lld couldn't open %s: %s",jobid,info->infile,strerror(errno));
		goto job_failure;
	}

	fds[1] = open(info->outfile,O_WRONLY|O_TRUNC|O_CREAT,0777);
	if(fds[1]<0) {
		debug(D_PROCESS,"jobid %lld couldn't open %s: %s",jobid,info->outfile,strerror(errno));
		close(fds[0]);
		goto job_failure;
	}

	if(!strcmp(info->errfile,info->outfile)) {
		fds[2] = dup(fds[1]);
	} else {
		fds[2] = open(info->errfile,O_WRONLY|O_TRUNC|O_CREAT,0777);
		if(fds[2]<0) {
			debug(D_PROCESS,"jobid %lld couldn't open %s: %s",jobid,info->errfile,strerror(errno));
			close(fds[0]);
			close(fds[1]);
			goto job_failure;
		}
	}

	/* Flush all stdio buffers so that the child does not inherit them. */
	fflush(0);

	pid = fork();
	if(pid==0) {
		int result;

		/* start a new session so as no to see signals from the parent. */
		setsid();

		/* Put our standard input, output, and error in the expected fds */
		dup2(fds[0],0);
		dup2(fds[1],1);
		dup2(fds[2],2);

		/* Close all other fds so that this process doesn't muck with the parent's files */
		for(i=3;i<maxfd;i++) close(i);

		/* Split the args into pieces and execute the program */
		string_split((char*)realargs,&argc,&argv);

		/* Move to the dir containing the program */
		result = chdir(info->cwd);
		if(result!=0) {
			printf("couldn't chdir %s: %s\n",info->cwd,strerror(errno));
			_exit(1);
		}

		if(is_builtin) {
			_exit(chirp_builtin(info->owner,argc,argv));
		} else {
			execvp(argv[0],argv);
			printf("couldn't execute %s: %s\n",info->command,strerror(errno));
			_exit(1);
		}

	} else if(pid>0) {
		pid_t checkpid;
		int status;

		close(fds[0]);
		close(fds[1]);
		close(fds[2]);

		info->state = CHIRP_JOB_STATE_RUNNING;
		info->pid = pid;
		info->start_time = time(0);
		chirp_job_info_save(jobid,info);

		do {
			checkpid = waitpid(pid,&status,0);
			if(checkpid<0 && errno==ECHILD) {
				debug(D_PROCESS,"jobid %lld waitpid returned no children!?!?",jobid);
				goto job_failure;
			}
		} while(checkpid!=pid);

		/*
		At this point, the job has completed, but
		there are two potential complications.

		1) chirp_job_kill has already marked the job as killed,
		and then issued kill(), which we just observed through waitpid.
		If this has happened, do not update the job file.

		2) chirp_job_remove has already removed the state file,
		in which case, we should not recreate it.
		*/

		if(chirp_job_info_load(jobid,info)) {
			if(info->state==CHIRP_JOB_STATE_KILLED) {
				/* job was already killed, do not update */
			} else {
				if(WIFEXITED(status)) {
					int exitcode = WEXITSTATUS(status);
					info->exit_code = exitcode;
				} else {
					int signum = WTERMSIG(status);
					info->exit_code = -signum;
				}

				info->state = CHIRP_JOB_STATE_COMPLETE;
				info->stop_time = time(0);
				chirp_job_info_save(jobid,info);
			}
		} else {
			/* chirp_job_remove already deleted the state */
		}
		return 1;
	} else {
		close(fds[0]);
		close(fds[1]);
		close(fds[2]);

		debug(D_PROCESS,"unable to fork (%s) will try again in five seconds",strerror(errno));
		sleep(5);

		return 0;
	}


	job_failure:
	info->state = CHIRP_JOB_STATE_FAILED;
	info->stop_time = time(0);
	chirp_job_info_save(jobid,info);
	return 0;
}

static int compare_jobid( const void *va, const void *vb )
{
	const INT64_T *a = va;
	const INT64_T *b = vb;

	if(*a>*b) {
		return 1;
	} else if(*a==*b) {
		return 0;
	} else {
		return -1;
	}
}

/*
Scan the job state directory, and build an array of jobids,
sorted from smallest to largest.
*/

static INT64_T * chirp_job_scan()
{
	char path[CHIRP_PATH_MAX];
	static int job_list_size = 16;
	INT64_T *job_list;
	int i=0;
	DIR *dir;
	struct dirent *d;
	
	job_list = malloc(job_list_size*sizeof(INT64_T));

	sprintf(path,"%s/.__jobs",chirp_transient_path);

	dir = opendir(path);
	if(!dir) return 0;

	while((d=readdir(dir))) {
		if(!strcmp(d->d_name,".")) continue;
		if(!strcmp(d->d_name,"..")) continue;
		if(!strncmp(d->d_name,".__",3)) continue;

		job_list[i] = atoll(d->d_name);
		i++;

		if(i>=job_list_size) {
			job_list_size *= 2;
			job_list = realloc(job_list,job_list_size*sizeof(INT64_T));
		}
	}

	closedir(dir);

	job_list[i] = 0;

	qsort(job_list,i,sizeof(*job_list),compare_jobid);

	return job_list;
}


/*
Scan the Chirp jobs directory.  For each job that was
in the RUNNING state, downgrade it to the FAILED state.
*/

static void chirp_job_cleanup()
{
	struct chirp_job_info info;
	INT64_T *job_list, jobid;
	int result;
	int i;

	job_list = chirp_job_scan();
	if(!job_list) return;

	for(i=0;job_list[i];i++) {
		jobid = job_list[i];

		if(chirp_job_info_load(jobid,&info)) {
			if(info.state==CHIRP_JOB_STATE_RUNNING) {
				result = kill(-info.pid,SIGKILL);
				sleep(1);
				info.state = CHIRP_JOB_STATE_IDLE;
				info.pid = 0;
				info.start_time = info.stop_time = 0;
				chirp_job_info_save(jobid,&info);
			}
		}
	}

	free(job_list);
}

/*
This function is called in a separate process
and used to execute one active storage job at a time.
*/

static int job_cleanup_time = 7*24*60*60;

extern int exit_if_parent_fails;

void chirp_job_starter()
{
	char path[CHIRP_PATH_MAX];
	struct chirp_job_info info;
	INT64_T jobid;
	INT64_T *job_list;
	int run_count;
	int i;

	chirp_job_cleanup();

	signal(SIGCHLD,SIG_DFL);

	sprintf(path,"%s/.__jobs",chirp_transient_path);
	mkdir(path,0700);

	while(1) {
                if(exit_if_parent_fails) {
                        if(getppid()<5) {
                                fatal("stopping because parent process died.");
                                exit(0);
                        }
                }

		run_count = 0;

		job_list = chirp_job_scan();
		if(!job_list) break;

		for(i=0;job_list[i];i++) {
			jobid = job_list[i];

			if(chirp_job_info_load(jobid,&info)) {
				switch(info.state) {
					case CHIRP_JOB_STATE_BEGIN:
						if( (time(0)-info.submit_time) > job_cleanup_time ) {
							chirp_job_info_delete(jobid);
						}
						break;
					case CHIRP_JOB_STATE_COMPLETE:
					case CHIRP_JOB_STATE_FAILED:
					case CHIRP_JOB_STATE_KILLED:
						if( (time(0)-info.stop_time) > job_cleanup_time ) {
							chirp_job_info_delete(jobid);
						}
						break;
					case CHIRP_JOB_STATE_IDLE:
						chirp_job_execute(jobid,&info);
						run_count++;
						break;
				}

			}
		}

		free(job_list);

		if(run_count==0) {
			/*
			If there was nothing to do, sleep for a reasonable time.
			If a job state changes, we will be woken by SIGHUP.
			*/
			sleep(5);
		}

	}

}

INT64_T chirp_job_begin( const char *subject, const char *cwd, const char *infile, const char *outfile, const char *errfile, const char *command, const char *args )
{
	struct chirp_job_info info;
	INT64_T jobid;

	jobid = chirp_job_create_jobid();
	if(jobid<0) {
		errno = EBUSY;
		return -1;
	}

	memset(&info,0,sizeof(info));

	info.jobid = jobid;

	strcpy(info.owner,subject);
	strcpy(info.cwd,cwd);
	strcpy(info.infile,infile);
	strcpy(info.outfile,outfile);
	strcpy(info.errfile,errfile);
	strcpy(info.command,command);
	strcpy(info.args,args);

	info.state = CHIRP_JOB_STATE_BEGIN;
	info.submit_time = time(0);

	if(chirp_job_info_save(jobid,&info)) {
		debug(D_PROCESS,"jobid %lld: %s %s stdin:%s stdout:%s stderr:%s",jobid,command,args,infile,outfile,errfile);
		return jobid;
	} else {
		errno = EBUSY;
		return -1;
	}
}

INT64_T chirp_job_wait( const char *subject, INT64_T jobid, struct chirp_job_state *state, time_t stoptime )
{
	struct chirp_job_info info;

	while(1) {
		if(!chirp_job_info_load(jobid,&info)) {
			errno = ESRCH;
			return -1;
		}

		if(strcmp(info.owner,subject)) {
			errno = EPERM;
			return -1;
		}


		if(info.state==CHIRP_JOB_STATE_COMPLETE || info.state==CHIRP_JOB_STATE_FAILED || info.state==CHIRP_JOB_STATE_KILLED ) {
			chirp_job_info_to_state(&info,state);
			return 0;
		}

		if(time(0)>=stoptime) {
			chirp_job_info_to_state(&info,state);
			return 0;
		}

		sleep(5);
	}
}

INT64_T chirp_job_commit( const char *subject, INT64_T jobid )
{
	struct chirp_job_info info;

	if(!chirp_job_info_load(jobid,&info)) {
		errno = ESRCH;
		return -1;
	}

	if(strcmp(info.owner,subject)) {
		errno = EPERM;
		return -1;
	}

	if(info.state==CHIRP_JOB_STATE_BEGIN) {
		info.state = CHIRP_JOB_STATE_IDLE;
		if(chirp_job_info_save(jobid,&info)) {
			return 0;
		} else {
			errno = EBUSY;
			return -1;
		}
	} else {
		return 0;
	}
}

INT64_T chirp_job_remove( const char *subject, INT64_T jobid )
{
	if(chirp_job_kill(subject,jobid)!=0) return -1;
	chirp_job_info_delete(jobid);
	return 0;
}

INT64_T chirp_job_kill( const char *subject, INT64_T jobid )
{
	struct chirp_job_info info;

	if(!chirp_job_info_load(jobid,&info)) {
		errno = ESRCH;
		return -1;
	}

	if(strcmp(info.owner,subject) && (!chirp_super_user || strcmp(subject,chirp_super_user))) {
		errno = EPERM;
		return -1;
	}

	switch(info.state) {
		case CHIRP_JOB_STATE_RUNNING:
		case CHIRP_JOB_STATE_SUSPENDED:
			info.state = CHIRP_JOB_STATE_KILLED;
			info.stop_time = time(0);
			chirp_job_info_save(jobid,&info);
			kill(-info.pid,SIGKILL);
			break;

		case CHIRP_JOB_STATE_BEGIN:
		case CHIRP_JOB_STATE_IDLE:
			info.state = CHIRP_JOB_STATE_KILLED;
			info.stop_time = info.stop_time;
			chirp_job_info_save(jobid,&info);
			break;

		case CHIRP_JOB_STATE_FAILED:
		case CHIRP_JOB_STATE_COMPLETE:
		case CHIRP_JOB_STATE_KILLED:
			break;
	}

	return 0;
}

struct chirp_job_list {
	INT64_T *job_list;
	int current;
};

void * chirp_job_list_open()
{
	struct chirp_job_list *list = malloc(sizeof(*list));
	list->job_list = chirp_job_scan();
	list->current = 0;
	return list;
}

struct chirp_job_state * chirp_job_list_next( void *vlist )
{
	struct chirp_job_list *list = vlist;
	static struct chirp_job_state state;
	struct chirp_job_info info;
	INT64_T jobid;

	while(1) {
		jobid = list->job_list[list->current++];
		if(jobid==0) return 0;

		if(chirp_job_info_load(jobid,&info)) {
			chirp_job_info_to_state(&info,&state);
			return &state;
		}
	}

	return 0;
}

void chirp_job_list_close( void *vlist )
{
	struct chirp_job_list *list = vlist;
	free(list->job_list);
	free(list);
}
