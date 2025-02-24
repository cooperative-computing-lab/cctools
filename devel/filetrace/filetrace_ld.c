#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <string.h>

#include "itable.h"

typedef FILE *(*fopen_t)(const char *p, const char *m);
typedef int (*open_t)(const char *p, int f);
typedef ssize_t(*rw_t)(int fd, void *b, ssize_t c);
typedef int (*stat_t)(const char *p, void *s);
typedef int (*fstat_t)(int fd, void *s);

typedef struct {
	char *pathname;
	int num_open;
	int bytes_read;
	int bytes_write;
	int num_reads;
	int num_writes;
	int num_stat;
} table_entry;


struct itable *file_table; /* maps fd -> table_entry */

void print_file_table(struct itable *t);

void __attribute__((constructor)) filetrace_init() {
	file_table = itable_create(0);
}

void __attribute__((destructor)) filetrace_exit() {
	print_file_table(file_table);
	itable_clear(file_table, 0);
	itable_delete(file_table);
}

void file_table_event_open(int fd, const char *pathname, int flags) {
	if(!itable_lookup(file_table, fd)){
		table_entry *e = malloc(sizeof(table_entry));
		e->num_open = 1;
		e->pathname = strdup(pathname); 
		itable_insert(file_table, fd, e);
	} else {
		table_entry *e = itable_lookup(file_table, fd);
		e->num_open += 1;
	}
}

FILE *fopen(const char *pathname, const char *mode) {
	fopen_t real_fopen = dlsym(RTLD_NEXT, "fopen");
	return real_fopen(pathname,mode);
}

int open(const char *pathname, int flags) {
	open_t real_open = dlsym(RTLD_NEXT, "open");
	int fd = real_open(pathname,flags);
	file_table_event_open(fd, pathname, flags);
	return fd;
}

ssize_t read(int fd, void *buf, size_t count){
	rw_t real_read = dlsym(RTLD_NEXT, "read");
	return real_read(fd, buf, count);
}

ssize_t write(int fd, void *buf, size_t count){
	rw_t real_write = dlsym(RTLD_NEXT, "write");
	return real_write(fd, buf, count);
}

int stat(const char *pathname, void *statbuf){
	stat_t real_stat = dlsym(RTLD_NEXT, "stat");
	return real_stat(pathname, statbuf);
}

int fstat(int fd, void *statbuf){
	fstat_t real_fstat = dlsym(RTLD_NEXT, "fstat");
	return real_fstat(fd, statbuf);
}

void print_file_table(struct itable *t){
	printf("Filetrace Summary\n");
	
	uint64_t fd;
	table_entry *e;
	ITABLE_ITERATE(t, fd, e){
		printf("File Descriptor: %d\n", fd);
		printf("\tpathname: %s\n", e->pathname);
		printf("\tnum_open: %d\n\n", e->num_open);
	}
}
