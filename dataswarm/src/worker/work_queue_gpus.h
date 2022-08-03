#ifndef WORK_QUEUE_GPUS_H
#define WORK_QUEUE_GPUS_H

void work_queue_gpus_init( int ngpus );
void work_queue_gpus_debug();
void work_queue_gpus_free( int taskid );
void work_queue_gpus_allocate( int n, int task );
char *work_queue_gpus_to_string( int taskid );

#endif
