/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "buffer.h"
#include "debug.h"
#include "hdfs_library.h"
#include "path.h"

#include <dlfcn.h>
#include <unistd.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Use the cryptic *(void **)(&hs->lval) cast, rather than just
 hs->lval because we get a warning when converting an object
 pointer to a function pointer. */
#define HDFS_LOAD_FUNC( lval, name ) \
	*(void **)(&hs->lval) = dlsym(hs->libhdfs_handle,name); \
	if(!hs->lval) { \
		debug(D_NOTICE|D_HDFS,"couldn't find %s in libhdfs.so",name); \
		goto failure; \
	}

int hdfs_library_envinit (void)
{
	buffer_t B;
	const char *CLASSPATH;
	const char *HADOOP_HOME;
	const char *JAVA_HOME;
	int rc = 0;

	buffer_init(&B);
	buffer_abortonfailure(&B, 1);

	if ((JAVA_HOME = getenv("JAVA_HOME")) == NULL) {
		debug(D_HDFS, "sorry, you must set JAVA_HOME to point to your Java installation.");
		goto invalid;
	}
	debug(D_HDFS, "JAVA_HOME=`%s'", JAVA_HOME);
	if ((HADOOP_HOME = getenv("HADOOP_HOME")) == NULL) {
		debug(D_HDFS, "sorry, you must set HADOOP_HOME to point to your Java installation.");
		goto invalid;
	}
	debug(D_HDFS, "HADOOP_HOME=`%s'", HADOOP_HOME);

	if ((CLASSPATH = getenv("CLASSPATH"))) {
		buffer_printf(&B, "%s:", CLASSPATH);
	}
	buffer_printf(&B, "%s/jdk/jre/lib", JAVA_HOME);
	buffer_printf(&B, ":%s/conf", HADOOP_HOME);
	{
		const char *path;
		buffer_t paths;

		buffer_init(&paths);
		buffer_abortonfailure(&paths, 1);
		if (path_find(&paths, HADOOP_HOME, "*.jar", 1) == -1) {
			debug(D_DEBUG, "failure to search `%s': %s", HADOOP_HOME, strerror(errno));
			buffer_free(&paths);
			goto failure;
		}
		/* NUL padded */
		for (path = buffer_tostring(&paths); *path; path = path+strlen(path)+1) {
			buffer_printf(&B, ":%s", path);
		}
		buffer_free(&paths);
	}
	rc = setenv("CLASSPATH", buffer_tostring(&B), 1);
	debug(D_HDFS, "CLASSPATH=`%s'", buffer_tostring(&B));
	if (rc == -1)
		goto failure;

	rc = 0;
	goto out;
invalid:
	errno = EINVAL;
	goto failure;
failure:
	/* errno already set */
	rc = -1;
	goto out;
out:
	buffer_free(&B);
	return rc;
}

static int load_lib (void **handle, const char *envpath, const char *envhome, const char *name)
{
	int rc;
	const char *HOME;
	const char *PATH;
	const char *path;
	buffer_t B;

	buffer_init(&B);

	if ((HOME = getenv(envhome)) == NULL)
		goto invalid;

	if ((PATH = getenv(envpath))) {
		debug(D_DEBUG, "%s set to explicitly load `%s'", envpath, PATH);
		buffer_printf(&B, "%s%c", PATH, 0);
	} else {
		int found;
		debug(D_DEBUG, "looking for all DSO that match `%s' in %s=`%s'", name, envhome, HOME);
		found = path_find(&B, HOME, name, 1);
		if (found == -1) {
			debug(D_DEBUG, "failure to search `%s': %s", HOME, strerror(errno));
			goto failure;
		}
	}

	/* NUL padded */
	for (path = buffer_tostring(&B); *path; path += strlen(path)+1) {
		debug(D_HDFS, "trying to load `%s'", path);
		*handle = dlopen(path, RTLD_LAZY);
		if (*handle) {
			rc = 0;
			goto out;
		} else {
			debug(D_HDFS, "dlopen failed: %s", dlerror());
		}
	}
	debug(D_NOTICE | D_HDFS, "could not find/load %s in %s=`%s'", name, envhome, HOME);
	errno = ENOENT;
	goto failure;
invalid:
	errno = EINVAL;
	goto failure;
failure:
	/* errno already set */
	rc = -1;
	goto out;
out:
	buffer_free(&B);
	return rc;
}

void hdfs_library_close(struct hdfs_library *hs)
{
	if (hs->libhdfs_handle)
		dlclose(hs->libhdfs_handle);
	if (hs->libjvm_handle)
		dlclose(hs->libjvm_handle);
	free(hs);
}

struct hdfs_library *hdfs_library_open()
{
	struct hdfs_library *hs;

	hs = malloc(sizeof(*hs));
	if (hs == NULL)
		return 0;

	if (load_lib(&hs->libjvm_handle, "LIBJVM_PATH", "JAVA_HOME", "*/libjvm.so") == -1)
		goto failure;
	if (load_lib(&hs->libhdfs_handle, "LIBHDFS_PATH", "HADOOP_HOME", "*/libhdfs.so") == -1)
		goto failure;

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
	HDFS_LOAD_FUNC(copy, "hdfsCopy");

	return hs;

failure:
	hdfs_library_close(hs);
	return 0;
}

/* vim: set noexpandtab tabstop=8: */
