#include "work_queue.h"
#include "int_sizes.h"
#include "link.h"
#include "debug.h"
#include "stringtools.h"
#include "domain_name_cache.h"
#include "hash_table.h"
#include "itable.h"
#include "list.h"
#include "macros.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>

#define WORKER_STATE_INIT  0
#define WORKER_STATE_READY 1
#define WORKER_STATE_BUSY  2
#define WORKER_STATE_NONE  3
#define WORKER_STATE_MAX   (WORKER_STATE_NONE+1)

struct work_queue_worker {
	int state;
	char hostname[DOMAIN_NAME_MAX];
	char addrport[32];
	char hashkey[32];
	int ncpus;
	INT64_T memory_avail;
	INT64_T memory_total;
	INT64_T disk_avail;
	INT64_T disk_total;
	struct hash_table *current_files;
	struct link *link;
	struct work_queue_task *current_task;
	int total_tasks_complete;
	timestamp_t total_task_time;
	int total_bytes_transfered;
	timestamp_t total_transfer_time;
};

struct work_queue {
	struct link * master_link;
	struct list * ready_list;
	struct list * complete_list;
	struct hash_table * worker_table;
	struct link_info * poll_table;
	int poll_table_size;
	int workers_in_state[WORKER_STATE_MAX];
	INT64_T total_tasks_submitted;
	INT64_T total_tasks_complete;
	INT64_T total_task_time;
};



int start_one_task( struct work_queue_task *t, struct work_queue_worker *w );

static int short_timeout = 5;
static int next_taskid = 1;

static int link_printf( struct link *l, const char *fmt, ... )
{
	char line[WORK_QUEUE_LINE_MAX];
	va_list args;
	va_start(args,fmt);
	vsnprintf(line,sizeof(line),fmt,args);
	va_end(args);
	return link_write(l,line,strlen(line),time(0)+short_timeout);
}

struct work_queue_task * work_queue_task_create( const char *program, const char *args )
{
	struct work_queue_task *t = malloc(sizeof(*t));
	memset(t,0,sizeof(*t));
	t->program = strdup(program);
	t->args = strdup(args);
	t->output = NULL;
	t->standard_input_files = NULL;
	t->extra_staged_files = NULL;
	t->extra_created_files = NULL;
	t->command = NULL;
	t->result = 0;
	t->taskid = next_taskid++;

	return t;
}

void work_queue_task_delete( struct work_queue_task *t )
{
	int i;
	struct task_file* tf;
	if(t) {
		if(t->program) free(t->program);
		if(t->args) free(t->args);
		if(t->output) free(t->output);
		if(t->standard_input_files) {
			for(i=0; i<list_size(t->standard_input_files); i++) {
				tf = list_pop_tail(t->standard_input_files);
				if(tf) {
					if(tf->payload) free(tf->payload);
					if(tf->remote_name) free(tf->remote_name);
					free(tf);
				}
			}
			list_delete(t->standard_input_files);
		}
		if(t->extra_staged_files) {
			for(i=0; i<list_size(t->extra_staged_files); i++) {
				tf = list_pop_tail(t->extra_staged_files);
				if(tf) {
					if(tf->payload) free(tf->payload);
					if(tf->remote_name) free(tf->remote_name);
					free(tf);
				}
			}
			list_delete(t->extra_staged_files);
		}
		if(t->extra_created_files) {
			for(i=0; i<list_size(t->extra_created_files); i++) {
				tf = list_pop_tail(t->extra_created_files);
				if(tf) {
					if(tf->payload) free(tf->payload);
					if(tf->remote_name) free(tf->remote_name);
					free(tf);
				}
			}
			list_delete(t->extra_created_files);
		}
		free(t);
	}
}

static void change_worker_state( struct work_queue *q, struct work_queue_worker *w, int state )
{
	q->workers_in_state[w->state]--;
	w->state = state;
	q->workers_in_state[state]++;
}

static void link_to_hash_key( struct link *link, char *key )
{
	sprintf(key,"0x%p",link);
}

void work_queue_get_stats( struct work_queue *q, struct work_queue_stats *s )
{
	memset(s,0,sizeof(*s));
	s->workers_init   = q->workers_in_state[WORKER_STATE_INIT];
	s->workers_ready  = q->workers_in_state[WORKER_STATE_READY];
	s->workers_busy   = q->workers_in_state[WORKER_STATE_BUSY];
	s->tasks_waiting  = list_size(q->ready_list);
	s->tasks_complete = list_size(q->complete_list);
	s->tasks_running  = q->workers_in_state[WORKER_STATE_BUSY];
	s->total_tasks_dispatched = q->total_tasks_submitted;
}

static void add_worker( struct work_queue *q )
{
	struct link *link;
	struct work_queue_worker *w;
	char addr[LINK_ADDRESS_MAX];
	int port;

	link = link_accept(q->master_link,time(0)+short_timeout);
	if(link) {
		link_tune(link,LINK_TUNE_INTERACTIVE);
		if(link_address_remote(link,addr,&port)) {
			w = malloc(sizeof(*w));
			memset(w,0,sizeof(*w));
			w->state = WORKER_STATE_NONE;
			w->link = link;
			w->current_files = hash_table_create(0,0);
			link_to_hash_key(link,w->hashkey);
			sprintf(w->addrport,"%s:%d",addr,port);
			hash_table_insert(q->worker_table,w->hashkey,w);
			change_worker_state(q,w,WORKER_STATE_INIT);
			debug(D_DEBUG,"worker %s added",w->addrport);
		} else {
			link_close(link);
		}			
	}
}



static void remove_worker( struct work_queue *q, struct work_queue_worker *w )
{
	char *key, *value;

	debug(D_DEBUG,"worker %s removed",w->addrport);

	hash_table_firstkey(w->current_files);
	while(hash_table_nextkey(w->current_files,&key,(void**)&value)) {
		hash_table_remove(w->current_files,key);
		free(value);
	}
	hash_table_remove(q->worker_table,w->hashkey);
	if(w->current_task) list_push_head(q->ready_list,w->current_task);
	change_worker_state(q,w,WORKER_STATE_NONE);
	if(w->link) link_close(w->link);
	free(w);
}

static int get_extra_created_files( struct work_queue_task *t, struct work_queue_worker *w )
{
	char line[WORK_QUEUE_LINE_MAX];
	struct task_file* tf;
	int actual,fd;
	int length;

	if(t->extra_created_files) {
		while((tf = list_pop_head(t->extra_created_files)) != NULL)
		{
			if(strlen(tf->remote_name) < 255 && strlen(tf->payload) < 255)
				debug(D_DEBUG,"%s (%s) sending back %s to %s",w->hostname,w->addrport,tf->remote_name, tf->payload);

			link_printf(w->link,"get %s\n",tf->remote_name);
			if(!link_readline(w->link,line,sizeof(line),time(0)+short_timeout)) goto failure;
			if(sscanf(line,"%d",&length)!=1) goto failure;
			fd = open(tf->payload,O_WRONLY|O_TRUNC|O_CREAT,0700);
			if(fd<0) goto failure;
			actual = link_stream_to_fd(w->link,fd,length,time(0)+short_timeout);
			close(fd);
			if(actual!=length) { unlink(tf->payload); goto failure; }
		}
	}
	return 1;

	failure:
	if(strlen(tf->remote_name) < 255 && strlen(tf->payload) < 255)
	    debug(D_DEBUG,"%s (%s) failed to receive %s into %s",w->addrport,w->hostname,tf->remote_name,tf->payload);
	return 0;
}

static int handle_worker( struct work_queue *q, struct link *l )
{
	char line[WORK_QUEUE_LINE_MAX];
	char key[WORK_QUEUE_LINE_MAX];
	struct work_queue_worker *w;
	int result, output_length;

	link_to_hash_key(l,key);
	w = hash_table_lookup(q->worker_table,key);

	if(link_readline(l,line,sizeof(line),time(0)+short_timeout)) {
		if(sscanf(line,"ready %s %d %lld %lld %lld %lld",w->hostname,&w->ncpus,&w->memory_avail,&w->memory_total,&w->disk_avail,&w->disk_total)==6) {
			if(w->state==WORKER_STATE_INIT) {
				change_worker_state(q,w,WORKER_STATE_READY);
				debug(D_DEBUG,"%s (%s) ready",w->hostname,w->addrport);
			}
		} else if(sscanf(line,"result %d %d",&result,&output_length)) {
			struct work_queue_task *t = w->current_task;
			int actual;

			t->output = malloc(output_length+1);

			if(output_length>0) {
				actual = link_read(l,t->output,output_length,time(0)+short_timeout);
				if(actual!=output_length) {
					free(t->output);
					t->output = 0;
					goto failure;
				}
			} else {
				actual = 0;
			}

			t->output[actual] = 0;
			t->result = result;

			if(!get_extra_created_files(t,w)) {
				free(t->output);
				t->output = 0;
				goto failure;
			}

			list_push_head(q->complete_list,w->current_task);
			w->current_task = 0;
			change_worker_state(q,w,WORKER_STATE_READY);

			t->finish_time = timestamp_get();
			strcpy(t->host,w->addrport);
			q->total_tasks_complete++;
			q->total_task_time += (t->finish_time-t->start_time);
			w->total_tasks_complete++;
			w->total_task_time += (t->finish_time-t->start_time);
			debug(D_DEBUG,"%s (%s) done in %.02lfs total tasks %d average %.02lfs",w->hostname,w->addrport,(t->finish_time-t->start_time)/1000000.0,w->total_tasks_complete,w->total_task_time/w->total_tasks_complete/1000000.0);

			/* If this worker completed a job ok, then send it a new one. */

			t = list_pop_head(q->ready_list);
			if(t) {
				if(start_one_task(t,w)) {
					change_worker_state(q,w,WORKER_STATE_BUSY);
					w->current_task = t;
				} else {
					debug(D_DEBUG,"%s (%s) removed because couldn't send task.",w->hostname,w->addrport);
					remove_worker(q,w);
				}
			}

		} else {
			goto failure;
		}
	} else {
		goto failure;
	}

	return 1;

	failure:
	debug(D_NOTICE,"%s (%s) failed and removed.",w->hostname,w->addrport);
	remove_worker(q,w);
	return 0;
}

static int build_poll_table( struct work_queue *q )
{
	int n=1;
	char *key;
	struct work_queue_worker *w;

	q->poll_table[0].link = q->master_link;
	q->poll_table[0].events = LINK_READ;
	q->poll_table[0].revents = 0;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table,&key,(void**)&w)) {
		q->poll_table[n].link = w->link;
		q->poll_table[n].events = LINK_READ;
		q->poll_table[n].revents = 0;
		n++;
	}

	return n;
}

static int send_standard_input_files( struct work_queue_task *t, struct work_queue_worker *w )
{
	struct task_file* tf;
	int actual=0;
	int total_bytes=0;
	timestamp_t open_time=0;
	timestamp_t close_time=0;
	timestamp_t sum_time=0;
	int i;
	int fl;
	
	if(t->standard_input_files) {
		for(i=0; i< list_size(t->standard_input_files); i++) {
			struct stat local_info;
			struct stat *remote_info;
			tf = list_pop_head(t->standard_input_files);

			if(tf->fname_or_literal == LITERAL) {
				fl = tf->length;
				time_t stoptime = time(0) + MAX(2.0,fl/1250000.0);
				debug(D_DEBUG,"%s (%s) needs buffer data as %s",w->hostname,w->addrport,tf->remote_name);
				open_time = timestamp_get();
				link_printf(w->link,"put %s %d %o\n",tf->remote_name,fl,0777);
				debug(D_DEBUG,"Limit sending %i bytes to %.02lfs seconds (or 1 if <0)",fl,(fl)/1250000.0);
			    actual = link_write(w->link, tf->payload, fl,stoptime);
			    close_time = timestamp_get();
			    if(actual!=(fl)) goto failure;
			    total_bytes+=actual;
			    sum_time+=(close_time-open_time);
			}
			else {
				if(stat(tf->payload,&local_info)<0) goto failure;
				
				remote_info = hash_table_lookup(w->current_files,tf->payload);
				if(!remote_info || remote_info->st_mtime != local_info.st_mtime || remote_info->st_size != local_info.st_size ) {
					if(remote_info) {
						hash_table_remove(w->current_files,tf->payload);
						free(remote_info);
					}
					
					debug(D_DEBUG,"%s (%s) needs file %s",w->hostname,w->addrport,tf->payload);
					int fd = open(tf->payload,O_RDONLY,0);
					if(fd<0) goto failure;
					time_t stoptime = time(0) + MAX(2.0,local_info.st_size/1250000.0);
					open_time = timestamp_get();
					link_printf(w->link,"put %s %d %o\n",tf->remote_name,(int)local_info.st_size,local_info.st_mode&0777);
					actual = link_stream_from_fd(w->link,fd,local_info.st_size,stoptime);
					close(fd);
					close_time = timestamp_get();
					if(actual!=local_info.st_size) goto failure;
					
					remote_info = malloc(sizeof(*remote_info));
					memcpy(remote_info,&local_info,sizeof(local_info));
					hash_table_insert(w->current_files,tf->payload,remote_info);
					total_bytes+=actual;
					sum_time+=(close_time-open_time);
				}
			}
			list_push_tail(t->standard_input_files, tf);
		}
		t->total_bytes_transfered += total_bytes;
		t->total_transfer_time += sum_time;
		w->total_bytes_transfered += total_bytes;
		w->total_transfer_time += sum_time;
		debug(D_DEBUG,"%s (%s) got %d bytes in %.02lfs (%.02lfs Mbps) average %.02lfs Mbps",w->hostname,w->addrport,total_bytes,sum_time/1000000.0,((8.0*total_bytes)/sum_time),(8.0*w->total_bytes_transfered)/w->total_transfer_time);
	}
	

	return 1;

	failure:
	if(tf->fname_or_literal == FNAME) 
	    debug(D_DEBUG,"%s (%s) failed to send %s (%i bytes received).",w->hostname,w->addrport,tf->payload,actual);
	else
	    debug(D_DEBUG,"%s (%s) failed to send buffer data (%i bytes received).",w->hostname,w->addrport,actual);
	return 0;
}

static int send_extra_staged_files( struct work_queue_task *t, struct work_queue_worker *w )
{
	struct task_file* tf;
	int actual=0;
	int total_bytes=0;
	timestamp_t open_time=0;
	timestamp_t close_time=0;
	timestamp_t sum_time=0;
	int i;
	int fl;
	
	if(t->extra_staged_files) {
		for(i=0; i< list_size(t->extra_staged_files); i++) {
			struct stat local_info;
			struct stat *remote_info;
			tf = list_pop_head(t->extra_staged_files);

			if(tf->fname_or_literal == LITERAL) {
				fl = tf->length;
				time_t stoptime = time(0) + MAX(2.0,fl/1250000.0);
				debug(D_DEBUG,"%s (%s) needs buffer data as %s",w->hostname,w->addrport,tf->remote_name);
				open_time = timestamp_get();
				link_printf(w->link,"put %s %d %o\n",tf->remote_name,fl,0777);
				debug(D_DEBUG,"Limit sending %i bytes to %.02lfs seconds (or 1 if <0)",fl,(fl)/1250000.0);
			    actual = link_write(w->link, tf->payload, fl,stoptime);
			    close_time = timestamp_get();
			    if(actual!=(fl)) goto failure;
			    total_bytes+=actual;
			    sum_time+=(close_time-open_time);
			}
			else {
				if(stat(tf->payload,&local_info)<0) goto failure;
				
				remote_info = hash_table_lookup(w->current_files,tf->payload);
				if(!remote_info || remote_info->st_mtime != local_info.st_mtime || remote_info->st_size != local_info.st_size ) {
					if(remote_info) {
						hash_table_remove(w->current_files,tf->payload);
						free(remote_info);
					}
					
					debug(D_DEBUG,"%s (%s) needs file %s",w->hostname,w->addrport,tf->payload);
					int fd = open(tf->payload,O_RDONLY,0);
					if(fd<0) goto failure;
					time_t stoptime = time(0) + MAX(2.0,local_info.st_size/1250000.0);
					open_time = timestamp_get();
					link_printf(w->link,"put %s %d %o\n",tf->remote_name,(int)local_info.st_size,local_info.st_mode&0777);
					actual = link_stream_from_fd(w->link,fd,local_info.st_size,stoptime);
					close(fd);
					close_time = timestamp_get();
					if(actual!=local_info.st_size) goto failure;
					
					remote_info = malloc(sizeof(*remote_info));
					memcpy(remote_info,&local_info,sizeof(local_info));
					hash_table_insert(w->current_files,tf->payload,remote_info);
					total_bytes+=actual;
					sum_time+=(close_time-open_time);
				}
			}
			list_push_tail(t->extra_staged_files, tf);
		}
		t->total_bytes_transfered += total_bytes;
		t->total_transfer_time += sum_time;
		w->total_bytes_transfered += total_bytes;
		w->total_transfer_time += sum_time;
		debug(D_DEBUG,"%s (%s) got %d bytes in %.02lfs (%.02lfs Mbps) average %.02lfs Mbps",w->hostname,w->addrport,total_bytes,sum_time/1000000.0,((8.0*total_bytes)/sum_time),(8.0*w->total_bytes_transfered)/w->total_transfer_time);
	}
	

	return 1;

	failure:
	if(tf->fname_or_literal == FNAME) 
	    debug(D_DEBUG,"%s (%s) failed to send %s (%i bytes received).",w->hostname,w->addrport,tf->payload,actual);
	else
	    debug(D_DEBUG,"%s (%s) failed to send buffer data (%i bytes received).",w->hostname,w->addrport,actual);
	return 0;
}

void build_full_command(struct work_queue_task *t) {
	if(t->command)
	{
		char* tmp = malloc((strlen(t->command)+strlen(" | ./")+strlen(t->program)+1+strlen(t->args)+1)*sizeof(char));
		sprintf(tmp,"%s | ./%s %s",t->command, t->program, t->args);
		free(t->command);
		t->command = tmp;
		debug(D_DEBUG,"Full command (with inputs): '%s'",t->command);
	}
	else {
		char* tmp = malloc((2+strlen(t->program)+1+strlen(t->args)+1)*sizeof(char));
		sprintf(tmp,"./%s %s", t->program, t->args);
		t->command = tmp;
		debug(D_DEBUG,"Full command (no inputs): '%s'",t->command);
	}

}

int start_one_task( struct work_queue_task *t, struct work_queue_worker *w )
{
	build_full_command(t);
	if(!send_standard_input_files(t,w)) return 0;
	if(!send_extra_staged_files(t,w)) return 0;
	t->start_time = timestamp_get();
	link_printf(w->link,"work %d\n",strlen(t->command));
	link_write(w->link,t->command,strlen(t->command),time(0)+short_timeout);
	debug(D_DEBUG,"%s (%s) busy on '%s'",w->hostname,w->addrport,t->command);
	return 1;
}

struct work_queue_worker * find_best_worker( struct work_queue *q )
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = 0;
	double best_average_task_time = 1000000.0;
	double average_task_time;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table,&key,(void**)&w)) {
		if(w->state==WORKER_STATE_READY) {
			if(w->total_tasks_complete==0) {
				average_task_time = 0;
			} else {
				average_task_time = w->total_tasks_complete/(double)w->total_task_time;				
			}
			if(!best_worker || average_task_time<best_average_task_time) {
				best_worker = w;
				best_average_task_time = average_task_time;
			}
		}
	}

	return best_worker;
}

static void start_tasks( struct work_queue *q )
{
	struct work_queue_task *t;
	struct work_queue_worker *w;

	while(list_size(q->ready_list)) {

		w = find_best_worker(q);
		if(!w) return;
		
		t = list_pop_head(q->ready_list);
		if(start_one_task(t,w)) {
			change_worker_state(q,w,WORKER_STATE_BUSY);
			w->current_task = t;
		} else {
			debug(D_DEBUG,"%s (%s) removed because couldn't send task.",w->hostname,w->addrport);
			remove_worker(q,w);
		}
	}
}

void abort_slow_workers( struct work_queue *q )
{
	struct work_queue_worker *w;
	char *key;

	if(q->total_tasks_complete<10) return;

	timestamp_t average_task_time = q->total_task_time / q->total_tasks_complete;
	timestamp_t current = timestamp_get();

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table,&key,(void**)&w)) {
		if(w->state==WORKER_STATE_BUSY) {
			timestamp_t runtime = current - w->current_task->start_time;
			if(runtime>(average_task_time*10)) {
				debug(D_NOTICE,"%s (%s) has run too long: %.02lfs (average is %.02lfs)",w->hostname,w->addrport,runtime/1000000.0,average_task_time/1000000.0);
				remove_worker(q,w);
			}
		}
	}

}

struct work_queue * work_queue_create( int port , time_t stoptime)
{
	struct work_queue *q = malloc(sizeof(*q));
	
	memset(q,0,sizeof(*q));
	
	do {
		q->master_link = link_serve(port);
	} while(!q->master_link && time(0) < stoptime);

	if(!q->master_link)
		return 0;
	
	q->ready_list = list_create();
	q->complete_list = list_create();
	q->worker_table = hash_table_create(0,0);
	
	q->poll_table_size = 1024;
	q->poll_table = malloc(sizeof(*q->poll_table)*q->poll_table_size);
	
	int i;
	for(i=0;i<WORKER_STATE_MAX;i++) {
		q->workers_in_state[i] = 0;
	}
	
	return q;
}

void work_queue_delete( struct work_queue *q )
{
	if(q) {
		hash_table_delete(q->worker_table);
		list_delete(q->ready_list);
		list_delete(q->complete_list);
		free(q->poll_table);
		link_close(q->master_link);
		free(q);
	}
}

void work_queue_submit( struct work_queue *q, struct work_queue_task *t )
{
	list_push_tail(q->ready_list,t);
	t->submit_time = timestamp_get();
	q->total_tasks_submitted++;
}

struct work_queue_task * work_queue_wait( struct work_queue *q , time_t timeout)
{
	struct work_queue_task *t;
	int i;
	time_t end_time;
	int result;
	
	if(timeout != WAITFORTASK)
		end_time = time(0) + timeout;
	else
		end_time = 0;
	
	while(1) {
		t = list_pop_head(q->complete_list);
		if(t) return t;

		if(q->workers_in_state[WORKER_STATE_BUSY]==0 && list_size(q->ready_list)==0) break;

		start_tasks(q);

		int n = build_poll_table(q);

		result = link_poll(q->poll_table,n,100000);
		//result = link_poll(q->poll_table,n,1000000);
		if(result<=0) continue;

		if(q->poll_table[0].revents) {
			add_worker(q);
		}

		for(i=1;i<n;i++) {
			if(q->poll_table[i].revents) {
				handle_worker(q,q->poll_table[i].link);
			}
			if(end_time && time(0) > end_time)
			{
				t = list_pop_head(q->complete_list);
				if(t) return t;
				return 0;
			}
		}

		abort_slow_workers(q);
	}

	return 0;
}


int work_queue_hungry(struct work_queue* q)
{
	
	struct work_queue_stats info;
	int i,j;
	work_queue_get_stats(q,&info);

	if(info.total_tasks_dispatched<100) return (100-info.total_tasks_dispatched);

        //i = 2 * number of current workers
	//j = # of queued tasks.
	//i-j = # of tasks to queue to re-reach the status quo.
	i = (1.1*(info.workers_init + info.workers_ready + info.workers_busy));
	j = (info.tasks_waiting);
	return MAX(i-j,0);
}

INT64_T work_queue_task_add_standard_input_buf( struct work_queue_task* t, const char* buf, int length) {

	struct task_file* tf = malloc(sizeof(struct task_file));
	tf->fname_or_literal = LITERAL;
	tf->cacheable = 0;
	tf->length = length;
	tf->payload = malloc(length);
	memcpy(tf->payload, buf, length);
	tf->remote_name = malloc(11*sizeof(char));
	string_cookie( tf->remote_name , 10);
	if(t->command) {
		char* tmp = malloc((strlen(t->command)+strlen(tf->remote_name)+1)*sizeof(char));
		sprintf(tmp,"%s %s",t->command,tf->remote_name);
		free(t->command);
		t->command = NULL;
		t->command = tmp;
	}
	else {
		char* tmp = malloc((strlen("/bin/cat ")+strlen(tf->remote_name)+1)*sizeof(char));
		sprintf(tmp,"/bin/cat %s",tf->remote_name);
		t->command = tmp;	
	}
	if(t->command)
		debug(D_DEBUG,"Current command: '%s'\n",t->command);
	else
		debug(D_DEBUG,"No command!\n");
	
	if(!t->standard_input_files)
		t->standard_input_files = list_create();
	return list_push_tail(t->standard_input_files,tf);
}


INT64_T work_queue_task_add_standard_input_file( struct work_queue_task* t, const char* fname) {
	struct task_file* tf = malloc(sizeof(struct task_file));
	tf->fname_or_literal = FNAME;
	tf->cacheable = 0;
	tf->length = strlen(fname);
	tf->payload = strdup(fname);
	tf->remote_name = malloc(11*sizeof(char));
	string_cookie( tf->remote_name , 10);
	if(t->command) {
		char* tmp = malloc((strlen(t->command)+strlen(tf->remote_name)+1)*sizeof(char));
		sprintf(tmp,"%s %s",t->command,tf->remote_name);
		free(t->command);
		t->command = NULL;
		t->command = tmp;
	}
	else {
		char* tmp = malloc((strlen("/bin/cat ")+strlen(tf->remote_name)+1)*sizeof(char));
		sprintf(tmp,"/bin/cat %s",tf->remote_name);
		t->command = tmp;	
	}
	if(t->command)
		debug(D_DEBUG,"Current command: '%s'\n",t->command);
	else
		debug(D_DEBUG,"No command!\n");
	if(!t->standard_input_files)
		t->standard_input_files = list_create();
	return list_push_tail(t->standard_input_files,tf);
}

INT64_T work_queue_task_add_extra_created_file( struct work_queue_task* t, const char* rname, const char* fname) {
	struct task_file* tf = malloc(sizeof(struct task_file));
	tf->fname_or_literal = FNAME;
	tf->cacheable = 0;
	tf->length = strlen(fname);
	tf->payload = strdup(fname);
	tf->remote_name = strdup(rname);
	if(!t->extra_created_files)
		t->extra_created_files = list_create();
	return list_push_tail(t->extra_created_files,tf);
}

INT64_T work_queue_task_add_extra_staged_buf( struct work_queue_task* t, const char* buf, int length, const char* rname) {
	struct task_file* tf = malloc(sizeof(struct task_file));
	tf->fname_or_literal = LITERAL;
	tf->cacheable = 1;
	tf->length = length;
	tf->payload = malloc(length);
	memcpy(tf->payload, buf, length);
	tf->remote_name = strdup(rname);
	if(!t->extra_staged_files)
		t->extra_staged_files = list_create();
	return list_push_tail(t->extra_staged_files,tf);
}

INT64_T work_queue_task_add_extra_staged_file( struct work_queue_task* t, const char* fname, const char* rname) {
	struct task_file* tf = malloc(sizeof(struct task_file));
	tf->fname_or_literal = FNAME;
	tf->cacheable = 1;
	tf->length = strlen(fname);
	tf->payload = strdup(fname);
	tf->remote_name = strdup(rname);
	if(!t->extra_staged_files)
		t->extra_staged_files = list_create();
	return list_push_tail(t->extra_staged_files,tf);
}

int work_queue_shut_down_workers (struct work_queue* q, int n)
{

    struct work_queue_worker *w;
    char *key;
    int i=0;
    // send worker exit.
    while((n==0 || i<n) && hash_table_nextkey(q->worker_table,&key,(void**)&w)) {
	link_printf(w->link,"exit\n");
	remove_worker(q,w);
	i++;
    }

    return i;  
}


int work_queue_empty (struct work_queue* q) {

    return ((list_size(q->ready_list)+list_size(q->complete_list)+q->workers_in_state[WORKER_STATE_BUSY])==0);
}
