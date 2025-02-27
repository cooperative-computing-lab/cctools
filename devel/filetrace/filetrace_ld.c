#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <string.h>

#include "itable.h"

typedef FILE *(*fopen_t)(const char *p, const char *m);
typedef int (*open_t)(const char *p, int f, ...);
typedef ssize_t(*rw_t)(int fd, void *b, ssize_t c);
typedef int (*stat_t)(const char *p, void *s);
typedef int (*fstat_t)(int fd, void *s);
typedef off_t(*lseek_t)(int fd, off_t o, int w);
typedef int(*fseek_t)(FILE* s, long off, int org);

//#define LOCK_EX 	2
//#define LOCK_UN 	8

//typedef int (*flock_t)(int fd, int op);

typedef struct {
	char *pathname;
	int num_open;
	int bytes_read;
	int bytes_write;
	int num_reads;
	int num_writes;
	int num_stat;
	int num_seek;
	int flags;
} table_entry;

struct itable *file_table; /* maps fd -> table_entry */

void log_file_table_plain(struct itable *t);
void log_file_table_json(struct itable *t);

void __attribute__((constructor(101))) filetrace_init() {
	file_table = itable_create(0);
}

void __attribute__((destructor)) filetrace_exit() {
	log_file_table_plain(file_table);
	log_file_table_json(file_table);
	itable_clear(file_table, 0);
	itable_delete(file_table);
}

void file_table_event_open(int fd, const char *pathname, int flags) {
	if(!itable_lookup(file_table, fd)){
		table_entry *e = calloc(1, sizeof(table_entry));
		e->num_open = 1;
		e->pathname = strdup(pathname);
	        e->flags = flags;	
		itable_insert(file_table, fd, e);
	} else {
		table_entry *e = itable_lookup(file_table, fd);
		e->num_open += 1;
	}
}

FILE *fopen(const char *pathname, const char *mode) {
	fopen_t real_fopen = dlsym(RTLD_NEXT, "fopen");
	FILE *f = real_fopen(pathname,mode);
	
	if(f && file_table){
		file_table_event_open(fileno(f), pathname, 0);
	}
	return f;
}

inline __attribute__((always_inline)) int open(const char *pathname, int flags, ...) {
	open_t real_open = dlsym(RTLD_NEXT, "open");
	int fd = real_open(pathname, flags, __builtin_va_arg_pack());
	
	/* The table may not exist inside the constructors of other shared libraries */
	if(file_table){
		file_table_event_open(fd, pathname, 1);
	}
	return fd;
}

ssize_t read(int fd, void *buf, size_t count){
	rw_t real_read = dlsym(RTLD_NEXT, "read");
	table_entry *e = itable_lookup(file_table, fd);
	if(e){
		e->num_reads += 1;
		e->bytes_read += count;
	}
	return real_read(fd, buf, count);
}

ssize_t write(int fd, void *buf, size_t count){
	rw_t real_write = dlsym(RTLD_NEXT, "write");
	table_entry *e = itable_lookup(file_table, fd);
	if(e){
		e->num_writes += 1;
		e->bytes_write += count;
	}
	return real_write(fd, buf, count);
}

int stat(const char *pathname, void *statbuf){
	stat_t real_stat = dlsym(RTLD_NEXT, "stat");
	return real_stat(pathname, statbuf);
}

off_t lseek(int fd, off_t offset, int whence){
	lseek_t real_lseek = dlsym(RTLD_NEXT, "lseek");
	table_entry *e = itable_lookup(file_table, fd);
	if(e){
		e->num_seek += 1;
	}
	return real_lseek(fd, offset, whence);
}

int fseek(FILE *stream, long offset, int origin){
	fseek_t real_fseek = dlsym(RTLD_NEXT, "fseek");
	table_entry *e = itable_lookup(file_table, fileno(stream));
	if(e){
		e->num_seek += 1;
	}
	return real_fseek(stream, offset, origin);
}

int fstat(int fd, void *statbuf){
	fstat_t real_fstat = dlsym(RTLD_NEXT, "fstat");
	return real_fstat(fd, statbuf);
}

void log_file_table_plain(struct itable *t){
	FILE *log = fopen("filetrace_log", "w");

	fprintf(log, "Filetrace Summary\n");
	uint64_t fd;
	table_entry *e;
	ITABLE_ITERATE(t, fd, e){
		fprintf(log, "File Descriptor: %ld\n", fd);
		fprintf(log, "\tpathname: %s\n", e->pathname);
		fprintf(log, "\tnum_open: %d\n", e->num_open);
		fprintf(log, "\tnum_read: %d\n", e->num_reads);
		fprintf(log, "\tnum_write: %d\n", e->num_writes);
		fprintf(log, "\tbytes_read: %d\n", e->bytes_read);
		fprintf(log, "\tbytes_written: %d\n", e->bytes_write);
		fprintf(log, "\tnum_seek: %d\n", e->num_seek);
		fprintf(log, "\topened with: %s\n\n", e->flags ? "fopen" : "open");
	}
	fclose(log);
}

void log_file_table_json(struct itable *t){
	FILE *log = fopen("filetrace_log.json", "w");
	//flock_t flock = dlsym(RTLD_NEXT, "flock");

	
	//flock(fileno(log), LOCK_EX);

	uint64_t fd;
	table_entry *e;
	fprintf(log, "{");
	ITABLE_ITERATE(t, fd, e){
		fprintf(log,"\"%ld\":{", fd);
		fprintf(log,"\"pathname\":\"%s\",", e->pathname);
		fprintf(log,"\"num_open\":%d,", e->num_open);
		fprintf(log,"\"num_reads\":%d,", e->num_reads);
		fprintf(log,"\"num_writes\":%d,", e->num_writes);
		fprintf(log,"\"bytes_read\":%d,", e->bytes_read);
		fprintf(log,"\"bytes_written\":%d,", e->bytes_write);
		fprintf(log, "\"num_seek\":%d,", e->num_seek);
		fprintf(log,"\"opened_with\":%s", e->flags?"\"fopen\"":"\"open\"");
		fprintf(log,"},");
	}
	fprintf(log, "\"end\":\"true\"");
	fprintf(log, "}");
	
	//flock(fileno(log), LOCK_UN);
}
