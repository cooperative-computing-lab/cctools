/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_reli.h"

#include "auth_all.h"
#include "full_io.h"
#include "getopt_aux.h"

#include <fcntl.h>
#include <unistd.h>

#include <sys/stat.h>
#ifndef CCTOOLS_OPSYS_CYGWIN
#include <sys/syscall.h>
#endif
#include <sys/time.h>

#include <errno.h>
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


#ifdef CCTOOLS_OPSYS_DARWIN
#define do_sync 0
#else
#define do_sync (getenv("CHIRP_SYNC") ? O_SYNC : 0)
#endif

#define STOPTIME (time(NULL)+5)

int do_chirp;
char *host;
double measure[10000];
double total;
double average;
double variance;
double stddev;
int loops, cycles;
int measure_bandwidth = 0;

long do_open(const char *file, int flags, int mode)
{
	if(do_chirp) {
		return (long) chirp_reli_open(host, file, flags, mode, STOPTIME);
	} else {
		return open(file, flags, mode);
	}
}

int do_close(long fd)
{
	if(do_chirp) {
		return chirp_reli_close((struct chirp_file *) fd, STOPTIME);
	} else {
		return close(fd);
	}
}

int do_fsync(long fd)
{
	if(do_chirp) {
		return 0;
	} else {
		return fsync(fd);
	}
}

int do_pread(long fd, char *buffer, int length, int offset)
{
	if(do_chirp) {
		return chirp_reli_pread((struct chirp_file *) fd, buffer, length, offset, STOPTIME);
	} else {
		return full_pread(fd, buffer, length, offset);
	}
}

int do_pwrite(long fd, const char *buffer, int length, int offset)
{
	if(do_chirp) {
		return chirp_reli_pwrite((struct chirp_file *) fd, buffer, length, offset, STOPTIME);
	} else {
		return full_pwrite(fd, buffer, length, offset);
	}
}

int do_stat(const char *file, struct stat *buf)
{
	if(do_chirp) {
		struct chirp_stat lbuf;
		return chirp_reli_stat(host, file, &lbuf, STOPTIME);
	} else {
		return stat(file, buf);
	}
}

int do_bandwidth(const char *file, int bytes, int blocksize, int do_write)
{
	int offset = 0;
	long fd;
	char *buffer = malloc(blocksize);
	int i;

	if(!buffer)
		return 0;

	for(i = 0; i < blocksize; i++)
		buffer[i] = (char) i;

	fd = do_open(file, (do_write ? O_WRONLY|O_TRUNC|O_CREAT : O_RDONLY) | do_sync, 0777);
	if(fd < 0 || fd == 0) {
		fprintf(stderr, "couldn't open %s: %s\n", file, strerror(errno));
		free(buffer);
		return 0;
	}

	while(bytes > 0) {
		if(do_write) {
			do_pwrite(fd, buffer, blocksize, offset);
		} else {
			do_pread(fd, buffer, blocksize, offset);
		}
		offset += blocksize;
		bytes -= blocksize;
	}

	do_close(fd);
	free(buffer);
	return 1;
}

void print_total()
{
	int j;
	total = 0;
	variance = 0;
	for(j = 0; j < cycles; j++)
		total += measure[j];
	average = total / cycles;
	for(j = 0; j < cycles; j++)
		variance += (measure[j] - average) * (measure[j] - average);
	stddev = sqrt(variance / (cycles - 1));

	printf("%9.4f +/- %9.4f ", average, stddev);

	if(measure_bandwidth) {
		printf(" MB/s\n");
	} else {
		printf(" usec\n");
	}
}

#define RUN_LOOP( name, test ) \
	printf("%s\t",name);\
	for(j=0;j<cycles;j++) {\
		gettimeofday( &start, 0 );\
		for( i=0; i<loops; i++ ) {\
			test;\
		}\
		gettimeofday( &stop, 0 );\
		runtime = (stop.tv_sec-start.tv_sec)*1000000 + (stop.tv_usec-start.tv_usec);\
		if(measure_bandwidth) {\
			measure[j] = (filesize*loops/(double)runtime);\
		} else {\
			measure[j] = ((double)(runtime))/loops;\
		}\
	}\
	print_total();


int main(int argc, char *argv[])
{
	long fd;
	int i, j, k;
	int bwloops;
	char *fname;
	char data[8192];
	int runtime;
	struct stat buf;
	struct timeval start, stop;
	int filesize = 16 * 1024 * 1024;

	if(argc != 6) {
		printf("use: %s <host> <file> <loops> <cycles> <bwloops>\n", argv[0]);
		return -1;
	}

	auth_register_all();

	host = argv[1];
	fname = argv[2];
	loops = atoi(argv[3]);
	cycles = atoi(argv[4]);
	bwloops = atoi(argv[5]);

	if(!strcmp(host, "unix")) {
		do_chirp = 0;
	} else {
		do_chirp = 1;
	}

#ifdef SYS_getpid
	RUN_LOOP("getpid", syscall(SYS_getpid));
#else
	RUN_LOOP("getpid", getpid());
#endif

	fd = do_open(fname, O_WRONLY | O_CREAT | O_TRUNC | do_sync, 0777);
	if(fd < 0 || fd == 0) {
		perror(fname);
		return -1;
	}

	memset(data, -1, sizeof(data));
	RUN_LOOP("write1", do_pwrite(fd, data, 1, 0));
	RUN_LOOP("write8", do_pwrite(fd, data, 8192, 0));

	do_close(fd);

	fd = do_open(fname, O_RDONLY | do_sync, 0777);
	if(fd < 0) {
		perror(fname);
		return -1;
	}

	RUN_LOOP("read1", do_pread(fd, data, 1, 0));
	RUN_LOOP("read8", do_pread(fd, data, 8192, 0));

	do_close(fd);

	RUN_LOOP("stat", do_stat(fname, &buf));
	RUN_LOOP("open", fd = do_open(fname, O_RDONLY | do_sync, 0777); do_close(fd););

	if(bwloops == 0)
		return 0;

	loops = bwloops;
	measure_bandwidth = 1;

	for(k = filesize; k >= (4 * 1024); k = k / 2) {
		printf("%4d ", k / 1024);
		RUN_LOOP("write", do_bandwidth(fname, filesize, k, 1));
		sync();
		printf("%4d ", k / 1024);
		RUN_LOOP("read", do_bandwidth(fname, filesize, k, 0));
		sync();
	}

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
