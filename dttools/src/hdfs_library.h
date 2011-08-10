/*
Copyright (C) 2011- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef HDFS_LIBRARY_H
#define HDFS_LIBRARY_H

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "int_sizes.h"

#ifndef HDFS_EINTERNAL
#define HDFS_EINTERNAL 255
#endif

typedef INT32_T tSize;
typedef time_t tTime;
typedef INT64_T tOffset;
typedef UINT16_T tPort;

typedef enum tObjectKind {
	kObjectKindFile = 'F',
	kObjectKindDirectory = 'D',
} tObjectKind;

typedef void *hdfsFS;
typedef void *hdfsFile;

typedef struct {
	tObjectKind mKind;
	char *mName;
	tTime mLastMod;
	tOffset mSize;
	short mReplication;
	tOffset mBlockSize;
	char *mOwner;
	char *mGroup;
	short mPermissions;
	tTime mLastAccess;
} hdfsFileInfo;

struct hdfs_library {
	void *libjvm_handle;
	void *libhdfs_handle;
	  hdfsFS(*connect) (const char *, tPort);
	  hdfsFS(*connect_as_user) (const char *, tPort, const char *, const char *[], int);
	int (*disconnect) (hdfsFS);
	hdfsFileInfo *(*listdir) (hdfsFS, const char *, int *);
	  hdfsFile(*open) (hdfsFS, const char *, int, int, short, tSize);
	int (*close) (hdfsFS, hdfsFile);
	int (*flush) (hdfsFS, hdfsFile);
	  tSize(*read) (hdfsFS, hdfsFile, void *, tSize);
	  tSize(*pread) (hdfsFS, hdfsFile, tOffset, void *, tSize);
	  tSize(*write) (hdfsFS, hdfsFile, const void *, tSize);
	int (*exists) (hdfsFS, const char *);
	int (*mkdir) (hdfsFS, const char *);
	int (*unlink) (hdfsFS, const char *, int recursive );
	int (*rename) (hdfsFS, const char *, const char *);
	hdfsFileInfo *(*stat) (hdfsFS, const char *);
	void (*free_stat) (hdfsFileInfo *, int);
	char ***(*get_hosts) (hdfsFS, const char *, tOffset, tOffset);
	void (*free_hosts) (char ***);
	  tOffset(*get_default_block_size) (hdfsFS);
	  tOffset(*get_capacity) (hdfsFS);
	  tOffset(*get_used) (hdfsFS);
	int (*chmod) (hdfsFS, const char *, short);
	int (*utime) (hdfsFS, const char *, tTime, tTime);
	int (*chdir) (hdfsFS, const char *);
	  tOffset(*tell) (hdfsFS, hdfsFile);
};

struct hdfs_library *hdfs_library_open();
void hdfs_library_close(struct hdfs_library *hs);

#endif
