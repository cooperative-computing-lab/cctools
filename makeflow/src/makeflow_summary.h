/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MAKEFLOW_SUMMARY_H
#define MAKEFLOW_SUMMARY_H

void makeflow_summary_create( struct dag *d, const char *filename, const char *email_summary_to, timestamp_t runtime, timestamp_t time_completed, int argc, char *argv[], const char *dagfile, struct batch_queue *remote_queue, int abort_flag, int failed_flag );

#endif
