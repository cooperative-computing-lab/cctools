#ifndef MAKEFLOW_SUMMARY_H
#define MAKEFLOW_SUMMARY_H

void makeflow_summary_create( struct dag *d, const char *filename, const char *email_summary_to, timestamp_t runtime, timestamp_t time_completed, int argc, char *argv[], const char *dagfile, struct batch_queue *remote_queue );

#endif

