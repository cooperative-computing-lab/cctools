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
#include "process.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>
#include <sys/wait.h>

#define WORKER_STATE_INIT  0
#define WORKER_STATE_READY 1
#define WORKER_STATE_BUSY  2
#define WORKER_STATE_NONE  3
#define WORKER_STATE_MAX   (WORKER_STATE_NONE+1)

#define FNAME 0
#define LITERAL 1

#define CHOOSE_BY_DEFAULT 0
#define CHOOSE_BY_FILES 1
#define CHOOSE_BY_TIME 2


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
	double fast_abort_multiplier;
};


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

struct task_file {
    short fname_or_literal; //0 or 1
    short cacheable; // 0=no, 1=yes.
    int length;
    void* payload; // name on master machine or buffer of data.
    char* remote_name; // name on remote machine.
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

struct work_queue_task * work_queue_task_create( const char *command_line)
{
	struct work_queue_task *t = malloc(sizeof(*t));
	memset(t,0,sizeof(*t));
	t->command_line = strdup(command_line);
	t->tag = NULL;
	t->worker_algorithm = CHOOSE_BY_FILES;
	t->output = NULL;
	t->input_files = NULL;
	t->output_files = NULL;
	t->return_status = 0;
	t->result = WQ_RESULT_UNSET;
	t->taskid = next_taskid++;

	t->submit_time = t->start_time = t->finish_time = t->total_bytes_transfered = t->total_transfer_time = 0;
	
	return t;
}

void work_queue_task_delete( struct work_queue_task *t )
{
	int i;
	struct task_file* tf;
	if(t) {
		if(t->command_line) free(t->command_line);
		if(t->tag) free(t->tag);
		if(t->output) free(t->output);
		if(t->input_files) {
			for(i=0; i<list_size(t->input_files); i++) {
				tf = list_pop_tail(t->input_files);
				if(tf) {
					if(tf->payload) free(tf->payload);
					if(tf->remote_name) free(tf->remote_name);
					free(tf);
				}
			}
			list_delete(t->input_files);
		}
		if(t->output_files) {
			for(i=0; i<list_size(t->output_files); i++) {
				tf = list_pop_tail(t->output_files);
				if(tf) {
					if(tf->payload) free(tf->payload);
					if(tf->remote_name) free(tf->remote_name);
					free(tf);
				}
			}
			list_delete(t->output_files);
		}
		if(t->host) free(t->host);
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

static int get_output_files( struct work_queue_task *t, struct work_queue_worker *w )
{
	char line[WORK_QUEUE_LINE_MAX];
	struct task_file* tf;
	int actual,fd;
	int length;
	time_t stoptime;
	
	if(t->output_files) {
		while((tf = list_pop_head(t->output_files)) != NULL)
		{
			if(strlen(tf->remote_name) < 255 && strlen(tf->payload) < 255)
				debug(D_DEBUG,"%s (%s) sending back %s to %s",w->hostname,w->addrport,tf->remote_name, tf->payload);

			link_printf(w->link,"get %s\n",tf->remote_name);
			if(!link_readline(w->link,line,sizeof(line),time(0)+short_timeout)) goto failure;
			if(sscanf(line,"%d",&length)!=1) goto failure;
			if(length>=0) {
				fd = open(tf->payload,O_WRONLY|O_TRUNC|O_CREAT,0700);
				if(fd<0) goto failure;
				stoptime = time(0) + MAX(1.0,(length)/1250000.0);
				actual = link_stream_to_fd(w->link,fd,length,stoptime);
				close(fd);
				if(actual!=length) { unlink(tf->payload); goto failure; }
			} else {
				debug(D_DEBUG,"%s (%s) did not create expected file %s",w->hostname,w->addrport,tf->remote_name);
				if(t->result == WQ_RESULT_UNSET) t->result = WQ_RESULT_OUTPUT_FAIL;
				t->return_status = 1;
			}
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
	time_t stoptime;
	
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
				stoptime = time(0) + MAX(1.0,output_length/1250000.0);
				actual = link_read(l,t->output,output_length,stoptime);
				if(actual!=output_length) {
					free(t->output);
					t->output = 0;
					goto failure;
				}
			} else {
				actual = 0;
			}

			t->output[actual] = 0;
			t->return_status = result;
			if(t->return_status != 0)
				t->result = WQ_RESULT_FUNCTION_FAIL;
			
			if(!get_output_files(t,w)) {
				free(t->output);
				t->output = 0;
				goto failure;
			}

			list_push_head(q->complete_list,w->current_task);
			w->current_task = 0;
			change_worker_state(q,w,WORKER_STATE_READY);

			t->finish_time = timestamp_get();

			t->host = strdup(w->addrport);
			q->total_tasks_complete++;
			q->total_task_time += (t->finish_time-t->start_time);
			w->total_tasks_complete++;
			w->total_task_time += (t->finish_time-t->start_time);
			debug(D_DEBUG,"%s (%s) done in %.02lfs total tasks %d average %.02lfs",w->hostname,w->addrport,(t->finish_time-t->start_time)/1000000.0,w->total_tasks_complete,w->total_task_time/w->total_tasks_complete/1000000.0);

			/* If this worker completed a job ok, then send it a new one.

			t = list_pop_head(q->ready_list);
			if(t) {
				if(start_one_task(t,w)) {
					change_worker_state(q,w,WORKER_STATE_BUSY);
					w->current_task = t;
				} else {
					debug(D_DEBUG,"%s (%s) removed because couldn't send task.",w->hostname,w->addrport);
					w->current_task = t;
					remove_worker(q,w);
				}
			} */

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

static int send_input_files( struct work_queue_task *t, struct work_queue_worker *w )
{
	struct task_file* tf;
	int actual=0;
	int total_bytes=0;
	timestamp_t open_time=0;
	timestamp_t close_time=0;
	timestamp_t sum_time=0;
	int i;
	int fl;
	time_t stoptime;
	
	if(t->input_files) {
		for(i=0; i< list_size(t->input_files); i++) {
			struct stat local_info;
			struct stat *remote_info;
			tf = list_pop_head(t->input_files);

			if(tf->fname_or_literal == LITERAL) {
				debug(D_DEBUG,"%s (%s) needs buffer data as %s",w->hostname,w->addrport,tf->remote_name);
				fl = tf->length;
				stoptime = time(0) + MAX(1.0,fl/1250000.0);
				open_time = timestamp_get();
				link_printf(w->link,"put %s %d %o\n",tf->remote_name,fl,0777);
				debug(D_DEBUG,"Limit sending %i bytes to %.03lfs seconds (or 1 if <0)",fl,(fl)/1250000.0);
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
					stoptime = time(0) + MAX(1.0,local_info.st_size/1250000.0);
					open_time = timestamp_get();
					link_printf(w->link,"put %s %d %o\n",tf->remote_name,(int)local_info.st_size,local_info.st_mode&0777);
					actual = link_stream_from_fd(w->link,fd,local_info.st_size,stoptime);
					close(fd);
					close_time = timestamp_get();
					if(actual!=local_info.st_size) goto failure;
					if(tf->cacheable) {
						remote_info = malloc(sizeof(*remote_info));
						memcpy(remote_info,&local_info,sizeof(local_info));
						hash_table_insert(w->current_files,tf->payload,remote_info);
					}
					total_bytes+=actual;
					sum_time+=(close_time-open_time);
				}
			}
			list_push_tail(t->input_files, tf);
		}
		t->total_bytes_transfered += total_bytes;
		t->total_transfer_time += sum_time;
		w->total_bytes_transfered += total_bytes;
		w->total_transfer_time += sum_time;
		debug(D_DEBUG,"%s (%s) got %d bytes in %.03lfs (%.02lfs Mbps) average %.02lfs Mbps",w->hostname,w->addrport,total_bytes,sum_time/1000000.0,((8.0*total_bytes)/sum_time),(8.0*w->total_bytes_transfered)/w->total_transfer_time);
	}
	

	return 1;

	failure:
	if(tf->fname_or_literal == FNAME) 
	    debug(D_DEBUG,"%s (%s) failed to send %s (%i bytes received).",w->hostname,w->addrport,tf->payload,actual);
	else
	    debug(D_DEBUG,"%s (%s) failed to send buffer data (%i bytes received).",w->hostname,w->addrport,actual);
	t->return_status = 1;
	t->result = WQ_RESULT_INPUT_FAIL;
	return 0;
}



int start_one_task( struct work_queue_task *t, struct work_queue_worker *w )
{
	if(!send_input_files(t,w)) return 0;
	t->start_time = timestamp_get();
	link_printf(w->link,"work %d\n",strlen(t->command_line));
	link_write(w->link,t->command_line,strlen(t->command_line),time(0)+short_timeout);
	debug(D_DEBUG,"%s (%s) busy on '%s'",w->hostname,w->addrport,t->command_line);
	return 1;
}

struct work_queue_worker * find_worker_by_time( struct work_queue *q )
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

struct work_queue_worker * find_worker_by_cache( struct work_queue *q , struct work_queue_task *t )
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = 0;
	INT64_T most_task_cached_bytes = 0;
	INT64_T task_cached_bytes;
	struct stat *remote_info;
	struct task_file* tf;
	int i,j;

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table,&key,(void**)&w)) {
		if(w->state==WORKER_STATE_READY) {
			task_cached_bytes = 0;
			j = list_size(t->input_files);
			for(i=0; i<j ; i++) {
				tf = list_pop_head(t->input_files);
				if(tf->fname_or_literal == FNAME && tf->cacheable) {
					remote_info = hash_table_lookup(w->current_files,tf->payload);
					if(remote_info)
						task_cached_bytes += remote_info->st_size;
				}
				list_push_tail(t->input_files, tf);
			}
			
			if(!best_worker || task_cached_bytes>most_task_cached_bytes) {
				best_worker = w;
				most_task_cached_bytes = task_cached_bytes;
			}
		}
	}
	if(best_worker) debug(D_DEBUG,"Worker %s has the most cached bytes for this task (%ld)\n",best_worker->hostname,most_task_cached_bytes);
	return best_worker;
}

struct work_queue_worker * find_worker_by_available( struct work_queue *q )
{
	char *key;
	struct work_queue_worker *w;
	struct work_queue_worker *best_worker = 0;
	
	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table,&key,(void**)&w)) {
		if(w->state==WORKER_STATE_READY) {
		    return w;
		}
	}

	return best_worker;
}

struct work_queue_worker * find_best_worker( struct work_queue *q, struct work_queue_task *t ) {

    if(t->worker_algorithm==CHOOSE_BY_FILES)
	return find_worker_by_cache(q,t);
    else if(t->worker_algorithm==CHOOSE_BY_TIME)
	return find_worker_by_time(q);
    else
	return find_worker_by_available(q);
    
}

static void start_tasks( struct work_queue *q )
{
	struct work_queue_task *t;
	struct work_queue_worker *w;

	while(list_size(q->ready_list)) {
		
		t = list_pop_head(q->ready_list);
		w = find_best_worker(q,t);
		
		if(!w) {
			list_push_head(q->ready_list,t);
			return;
		}
		
		if(start_one_task(t,w)) {
			change_worker_state(q,w,WORKER_STATE_BUSY);
			w->current_task = t;
		} else {
			debug(D_DEBUG,"%s (%s) removed because couldn't send task.",w->hostname,w->addrport);
			w->current_task = t;
			remove_worker(q,w);
		}
	}
}

int work_queue_activate_fast_abort(struct work_queue* q, double multiplier)
{
	if(multiplier >= 1) {
		q->fast_abort_multiplier = multiplier;
		return 0;
	}
	else {
		debug(D_DEBUG,"Bad multiplier (%.03lf) given for fast abort. Using the default (10)", multiplier);
		q->fast_abort_multiplier = 10;
		return 1;
	}
}

void abort_slow_workers( struct work_queue *q )
{
	struct work_queue_worker *w;
	char *key;
	const double multiplier = q->fast_abort_multiplier;
	
	if(q->total_tasks_complete<10) return;

	timestamp_t average_task_time = q->total_task_time / q->total_tasks_complete;
	timestamp_t current = timestamp_get();

	hash_table_firstkey(q->worker_table);
	while(hash_table_nextkey(q->worker_table,&key,(void**)&w)) {
		if(w->state==WORKER_STATE_BUSY) {
			timestamp_t runtime = current - w->current_task->start_time;
			if(runtime>(average_task_time*multiplier)) {
				debug(D_NOTICE,"%s (%s) has run too long: %.02lf s (average is %.02lf s)",w->hostname,w->addrport,runtime/1000000.0,average_task_time/1000000.0);
				remove_worker(q,w);
			}
		}
	}

}

struct work_queue * work_queue_create( int port, time_t stoptime)
{
	struct work_queue *q = malloc(sizeof(*q));
	int waittime = 1;
	
	if(port == 0) {
		const char *portstring = getenv("WORK_QUEUE_PORT");
		if(portstring) {
			port = atoi(portstring);
		} else {
			port = WORK_QUEUE_DEFAULT_PORT;
		}
	}

	memset(q,0,sizeof(*q));
	
	do {
		q->master_link = link_serve(port);
		if(!q->master_link) {
			debug(D_NOTICE,"Could not create work_queue on port %i. Trying again in %i seconds.",port,waittime);
			sleep(waittime);
			waittime*=2;
		}
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

	q->fast_abort_multiplier = -1.0;
	
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

struct work_queue_task * work_queue_wait( struct work_queue *q, int timeout )
{
	struct work_queue_task *t;
	int i;
	time_t stoptime;
	int result;
	
	if(timeout != WAITFORTASK)
		stoptime = time(0) + timeout;
	else
		stoptime = 0;
	
	while(1) {
		t = list_pop_head(q->complete_list);
		if(t) return t;

		if(q->workers_in_state[WORKER_STATE_BUSY]==0 && list_size(q->ready_list)==0) break;

		start_tasks(q);

		int n = build_poll_table(q);
		int msec = MAX(0,(time(0)-stoptime)*1000);

		result = link_poll(q->poll_table,n,msec);

		// If time has expired, return without a task.
		if(stoptime && time(0)>stoptime) return 0;

		// If a process is waiting to complete, return without a task.
		if(process_pending()) return 0;

		// If nothing was awake, restart the loop.
		if(result<=0) continue;

		if(q->poll_table[0].revents) {
			add_worker(q);
		}

		for(i=1;i<n;i++) {
			if(q->poll_table[i].revents) {
				handle_worker(q,q->poll_table[i].link);
			}
			if(stoptime && time(0) > stoptime)
			{
				return list_pop_head(q->complete_list);
			}
		}

		if(q->fast_abort_multiplier > 0) // fast abort is turned on. 
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

        //i = 1.1 * number of current workers
	//j = # of queued tasks.
	//i-j = # of tasks to queue to re-reach the status quo.
	i = (1.1*(info.workers_init + info.workers_ready + info.workers_busy));
	j = (info.tasks_waiting);
	return MAX(i-j,0);
}


INT64_T work_queue_task_specify_tag( struct work_queue_task* t, const char* tag) {
    if(t->tag)
	free(t->tag);
    t->tag = malloc((strlen(tag)+1)*sizeof(char));
    strcpy(t->tag,tag);
    return 0;
}


INT64_T work_queue_task_specify_output_file( struct work_queue_task* t, const char* rname, const char* fname) {
	struct task_file* tf = malloc(sizeof(struct task_file));
	tf->fname_or_literal = FNAME;
	tf->cacheable = 0;
	tf->length = strlen(fname);
	tf->payload = strdup(fname);
	tf->remote_name = strdup(rname);
	if(!t->output_files)
		t->output_files = list_create();
	return list_push_tail(t->output_files,tf);
}

INT64_T work_queue_task_specify_input_buf( struct work_queue_task* t, const char* buf, int length, const char* rname) {
	struct task_file* tf = malloc(sizeof(struct task_file));
	tf->fname_or_literal = LITERAL;
	tf->cacheable = 0;
	tf->length = length;
	tf->payload = malloc(length);
	memcpy(tf->payload, buf, length);
	tf->remote_name = strdup(rname);
	if(!t->input_files)
		t->input_files = list_create();
	return list_push_tail(t->input_files,tf);
}

INT64_T work_queue_task_specify_input_file( struct work_queue_task* t, const char* fname, const char* rname) {
	struct task_file* tf = malloc(sizeof(struct task_file));
	tf->fname_or_literal = FNAME;
	tf->cacheable = 1;
	tf->length = strlen(fname);
	tf->payload = strdup(fname);
	tf->remote_name = strdup(rname);
	if(!t->input_files)
		t->input_files = list_create();
	return list_push_tail(t->input_files,tf);
}

int work_queue_shut_down_workers (struct work_queue* q, int n)
{

    struct work_queue_worker *w;
    char *key;
    int i=0;
    if(!q)
	return -1;
    
    // send worker exit.
    hash_table_firstkey( q->worker_table);
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
