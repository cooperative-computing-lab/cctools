/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef RMONITOR_POLL_H
#define RMONITOR_POLL_H

#include "itable.h"
#include "hash_table.h"

#include "rmonitor_types.h"

void monitor_poll_all_processes_once(struct itable *processes, struct process_info *acc);
void monitor_poll_all_wds_once(struct hash_table *wdirs, struct wdir_info *acc);
void monitor_poll_all_fss_once(struct itable *filesysms, struct filesys_info *acc);

int monitor_poll_process_once(struct process_info *p);
int monitor_poll_wd_once(struct wdir_info *d);
int monitor_poll_fs_once(struct filesys_info *f);

int get_cpu_time_usage(pid_t pid, struct cpu_time_info *cpu);
void acc_cpu_time_usage(struct cpu_time_info *acc, struct cpu_time_info *other);

int get_mem_usage(pid_t pid, struct mem_info *mem);
void acc_mem_usage(struct mem_info *acc, struct mem_info *other);

int get_sys_io_usage(pid_t pid, struct io_info *io);
void acc_sys_io_usage(struct io_info *acc, struct io_info *other);

int get_map_io_usage(pid_t pid, struct io_info *io);
void acc_map_io_usage(struct io_info *acc, struct io_info *other);

int get_dsk_usage(const char *path, struct statfs *disk);
void acc_dsk_usage(struct statfs *acc, struct statfs *other);

int get_wd_usage(struct wdir_info *d);
void acc_wd_usage(struct wdir_info *acc, struct wdir_info *other);

FILE *open_proc_file(pid_t pid, char *filename);
int get_int_attribute(FILE *fstatus, char *attribute, uint64_t *value, int rewind_flag);


#endif
