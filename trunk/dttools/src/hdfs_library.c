/*
Copyright (C) 2011- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "hdfs_library.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <dlfcn.h>
#include <unistd.h>
#include <errno.h>

#include "debug.h"

#define HDFS_LOAD_FUNC( lval, name ) \
	hs->lval = dlsym(hs->libhdfs_handle,name); \
	if(!hs->lval) { \
		debug(D_NOTICE|D_HDFS,"couldn't find %s in libhdfs.so",name); \
		goto failure; \
	}

void hdfs_library_close(struct hdfs_library *hs)
{
	dlclose(hs->libhdfs_handle);
	dlclose(hs->libjvm_handle);
	free(hs);
}

struct hdfs_library *hdfs_library_open()
{
	static int did_warning = 0;

	if(!getenv("JAVA_HOME") || !getenv("HADOOP_HOME") || !getenv("CLASSPATH") || !getenv("LIBHDFS_PATH") || !getenv("LIBJVM_PATH")) {
		if(!did_warning) {
			debug(D_NOTICE | D_HDFS, "Sorry, to use HDFS, you need to set up Java and Hadoop first.\n");
			debug(D_NOTICE | D_HDFS, "Please set JAVA_HOME and HADOOP_HOME appropriately,\n");
			debug(D_NOTICE | D_HDFS, "then use chirp_server_hdfs or parrot_run_hdfs as needed.\n");
			did_warning = 0;
		}
		errno = ENOSYS;
		return 0;
	}

	struct hdfs_library *hs = malloc(sizeof(*hs));

	const char *libjvm_path = getenv("LIBJVM_PATH");
	hs->libjvm_handle = dlopen(libjvm_path, RTLD_LAZY);
	if(!hs->libjvm_handle) {
		debug(D_NOTICE | D_HDFS, "couldn't dlopen LIBJVM_PATH=%s: %s", libjvm_path, dlerror());
		free(hs);
		return 0;
	}


	const char *libhdfs_path = getenv("LIBHDFS_PATH");
	hs->libhdfs_handle = dlopen(libhdfs_path, RTLD_LAZY);
	if(!hs->libhdfs_handle) {
		dlclose(hs->libjvm_handle);
		free(hs);
		debug(D_NOTICE | D_HDFS, "couldn't dlopen LIBHDFS_PATH=%s: %s", dlerror());
		return 0;
	}


	HDFS_LOAD_FUNC(connect, "hdfsConnect");
	HDFS_LOAD_FUNC(connect_as_user, "hdfsConnectAsUser");
	HDFS_LOAD_FUNC(disconnect, "hdfsDisconnect");
	HDFS_LOAD_FUNC(listdir, "hdfsListDirectory");
	HDFS_LOAD_FUNC(open, "hdfsOpenFile");
	HDFS_LOAD_FUNC(close, "hdfsCloseFile");
	HDFS_LOAD_FUNC(flush, "hdfsFlush");
	HDFS_LOAD_FUNC(read, "hdfsRead");
	HDFS_LOAD_FUNC(pread, "hdfsPread");
	HDFS_LOAD_FUNC(write, "hdfsWrite");
	HDFS_LOAD_FUNC(exists, "hdfsExists");
	HDFS_LOAD_FUNC(mkdir, "hdfsCreateDirectory");
	HDFS_LOAD_FUNC(unlink, "hdfsDelete");
	HDFS_LOAD_FUNC(rename, "hdfsRename");
	HDFS_LOAD_FUNC(stat, "hdfsGetPathInfo");
	HDFS_LOAD_FUNC(free_stat, "hdfsFreeFileInfo");
	HDFS_LOAD_FUNC(get_hosts, "hdfsGetHosts");
	HDFS_LOAD_FUNC(free_hosts, "hdfsFreeHosts");
	HDFS_LOAD_FUNC(get_default_block_size, "hdfsGetDefaultBlockSize");
	HDFS_LOAD_FUNC(get_capacity, "hdfsGetCapacity");
	HDFS_LOAD_FUNC(get_used, "hdfsGetUsed");
	HDFS_LOAD_FUNC(chmod, "hdfsChmod");
	HDFS_LOAD_FUNC(utime, "hdfsUtime");
	HDFS_LOAD_FUNC(chdir, "hdfsSetWorkingDirectory");
	HDFS_LOAD_FUNC(tell, "hdfsTell");
	HDFS_LOAD_FUNC(setrep, "hdfsSetReplication");

	return hs;

      failure:
	hdfs_library_close(hs);
	return 0;
}
