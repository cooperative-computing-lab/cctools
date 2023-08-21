/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_transfer.h"
#include "vine_protocol.h"

#include "debug.h"
#include "host_disk_info.h"
#include "link.h"
#include "path.h"
#include "stringtools.h"
#include "unlink_recursive.h"
#include "url_encode.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* Temporary hacks to access global values. */

__attribute__((format(printf, 2, 3))) void send_message(struct link *l, const char *fmt, ...);
int recv_message(struct link *l, char *line, int length, time_t stoptime);

/*
This module implements the streaming directory transfer,
making it efficient to move large directory trees without
multiple round trips needed for remote procedure calls.

Each file, directory, or symlink is represented by a single
header line giving the name, length, and mode of the entry.
Files and symlinks are followed by the raw contents of the file
or link, respectively, while directories are followed by more
lines containing the contents of the directory, until an "end"
is received.

For example, the following directory tree:

- mydir
-- 1.txt
-- 2.txt
-- mysubdir
--- a.txt
--- b.txt
-- z.jpb

Is represented as follows:

dir mydir
file 1.txt 35291 0600
  (35291 bytes of 1.txt)
file 2.txt 502 0666
  (502 bytes of 2.txt)
dir mysubdir
file a.txt 321 0600
  (321 bytes of a.txt)
file b.txt 456 0600
  (456 bytes of a.txt)
end
file z.jpg 40001 0644
  (40001 bytes of z.jpg)
end

*/

static int vine_transfer_put_internal(struct link *lnk, const char *full_name, const char *relative_name,
		vine_transfer_mode_t xfer_mode, time_t stoptime)
{
	struct stat info;
	int64_t actual, length;
	int mode;

	/* URL encode filename to handle spaces and unprintable characters. */
	char relative_name_encoded[VINE_LINE_MAX];
	url_encode(relative_name, relative_name_encoded, sizeof(relative_name_encoded));

	if (stat(full_name, &info) != 0) {
		goto access_failure;
	}

	mode = info.st_mode & 0777;

	if (S_ISREG(info.st_mode)) {
		int fd = open(full_name, O_RDONLY, 0);
		if (fd >= 0) {
			length = info.st_size;
			send_message(lnk, "file %s %" PRId64 " 0%o\n", relative_name_encoded, length, mode);
			actual = link_stream_from_fd(lnk, fd, length, stoptime);
			close(fd);
			if (actual != length)
				goto send_failure;
		} else {
			goto access_failure;
		}
	} else if (xfer_mode == VINE_TRANSFER_MODE_FILE_ONLY) {
		/*
		The caller only wants a file, but full_name is something else.
		Choose a suitable error number to return in the error message.
		*/
		if (S_ISDIR(info.st_mode)) {
			errno = EISDIR;
		} else {
			errno = EINVAL;
		}
		goto access_failure;
	} else if (S_ISDIR(info.st_mode)) {
		DIR *dir = opendir(full_name);
		if (!dir)
			goto access_failure;

		send_message(lnk, "dir %s 0\n", relative_name_encoded);

		struct dirent *dent;
		while ((dent = readdir(dir))) {
			if (!strcmp(dent->d_name, ".") || !strcmp(dent->d_name, ".."))
				continue;
			char *sub_full_name = string_format("%s/%s", full_name, dent->d_name);
			int sub_result = vine_transfer_put_internal(
					lnk, sub_full_name, dent->d_name, xfer_mode, stoptime);
			free(sub_full_name);

			// Bail out of transfer if we cannot send any more
			if (!sub_result)
				break;
		}
		closedir(dir);
		send_message(lnk, "end\n");

	} else if (S_ISLNK(info.st_mode)) {
		char link_target[VINE_LINE_MAX];
		int result = readlink(full_name, link_target, sizeof(link_target));
		if (result > 0) {
			send_message(lnk, "symlink %s %d\n", relative_name_encoded, result);
			link_write(lnk, link_target, result, stoptime);
		} else {
			goto access_failure;
		}
	} else {
		goto access_failure;
	}

	return 1;

access_failure:
	// An error here is not a failure from our perspective, keep going.
	send_message(lnk, "error %s %d\n", relative_name_encoded, errno);
	return 1;

send_failure:
	debug(D_VINE,
			"Sending back output file - %s failed: bytes to send = %" PRId64
			" and bytes actually sent = %" PRId64 ".",
			full_name,
			length,
			actual);
	return 0;
}

/*
Send a cached object of any type down the wire.
*/

int vine_transfer_put_any(struct link *lnk, struct vine_cache *cache, const char *filename,
		vine_transfer_mode_t xfer_mode, time_t stoptime)
{
	char *cached_path = vine_cache_full_path(cache, filename);
	int r = vine_transfer_put_internal(lnk, cached_path, path_basename(filename), xfer_mode, stoptime);
	free(cached_path);
	return r;
}

/*
Handle an incoming symbolic link inside the recursive protocol.
The filename of the symlink was already given in the message,
and the target of the symlink is given as the "body" which
must be read off of the wire.  The symlink target does not
need to be url_decoded because it is sent in the body.
*/

static int vine_transfer_get_symlink_internal(struct link *lnk, char *filename, int length, time_t stoptime)
{
	char *target = malloc(length);

	int actual = link_read(lnk, target, length, stoptime);
	if (actual != length) {
		free(target);
		return 0;
	}

	int result = symlink(target, filename);
	if (result < 0) {
		debug(D_VINE, "could not create symlink %s: %s", filename, strerror(errno));
		free(target);
		return 0;
	}

	free(target);

	return 1;
}

/*
Handle an incoming file inside the recursive protocol.
Notice that we trust the caller to have created
the necessary parent directories and checked the
name for validity.
*/

static int vine_transfer_get_file_internal(
		struct link *lnk, const char *filename, int64_t length, int mode, time_t stoptime)
{
	if (!check_disk_space_for_filesize(".", length, 0)) {
		debug(D_VINE,
				"Could not put file %s, not enough disk space (%" PRId64 " bytes needed)\n",
				filename,
				length);
		return 0;
	}

	int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0700);
	if (fd < 0) {
		debug(D_VINE, "Could not open %s for writing. (%s)\n", filename, strerror(errno));
		return 0;
	}

	int64_t actual = link_stream_to_fd(lnk, fd, length, stoptime);

	if (actual != length) {
		debug(D_VINE, "Failed to put file - %s (%s)\n", filename, strerror(errno));
		close(fd);
		return 0;
	}

	if (close(fd) < 0) {
		debug(D_VINE, "Failed to close file - %s (%s)\n", filename, strerror(errno));
		return 0;
	}

	chmod(filename, mode);

	return 1;
}

static int vine_transfer_get_dir_internal(struct link *lnk, const char *dirname, int64_t *totalsize, time_t stoptime);

/*
Receive a single item of unknown type into the directory "dirname".
Returns 0 on failure to transfer.
Returns 1 on successful transfer of one item.
Returns 2 on successful receipt of "end" of list.
*/

static int vine_transfer_get_any_internal(struct link *lnk, const char *dirname, int64_t *totalsize, time_t stoptime)
{
	char line[VINE_LINE_MAX];
	char name_encoded[VINE_LINE_MAX];
	char name[VINE_LINE_MAX];
	int64_t size;
	int mode;
	int errornum;

	if (!recv_message(lnk, line, sizeof(line), stoptime))
		return 0;

	int r = 0;

	if (sscanf(line, "file %s %" SCNd64 " 0%o", name_encoded, &size, &mode) == 3) {

		url_decode(name_encoded, name, sizeof(name));

		char *subname = string_format("%s/%s", dirname, name);
		r = vine_transfer_get_file_internal(lnk, subname, size, mode, stoptime);
		free(subname);

		*totalsize += size;

	} else if (sscanf(line, "symlink %s %" SCNd64, name_encoded, &size) == 2) {

		url_decode(name_encoded, name, sizeof(name));

		char *subname = string_format("%s/%s", dirname, name);
		r = vine_transfer_get_symlink_internal(lnk, subname, size, stoptime);
		free(subname);

		*totalsize += size;

	} else if (sscanf(line, "dir %s", name_encoded) == 1) {

		url_decode(name_encoded, name, sizeof(name));

		char *subname = string_format("%s/%s", dirname, name);
		r = vine_transfer_get_dir_internal(lnk, subname, totalsize, stoptime);
		free(subname);

	} else if (sscanf(line, "error %s %d", name_encoded, &errornum) == 2) {

		debug(D_VINE, "unable to transfer %s: %s", name_encoded, strerror(errornum));
		r = 0;

	} else if (!strcmp(line, "end")) {
		r = 2;
	}

	return r;
}

/*
Handle an incoming directory inside the recursive dir protocol.
Notice that we have already checked the dirname for validity,
and now we process "file" and "dir" commands within the list
until "end" is reached.
*/

static int vine_transfer_get_dir_internal(struct link *lnk, const char *dirname, int64_t *totalsize, time_t stoptime)
{
	int result = mkdir(dirname, 0777);
	if (result < 0) {
		debug(D_VINE, "unable to create %s: %s", dirname, strerror(errno));
		return 0;
	}

	while (1) {
		int r = vine_transfer_get_any_internal(lnk, dirname, totalsize, stoptime);
		if (r == 1) {
			// Successfully received one item.
			continue;
		} else if (r == 2) {
			// Sucessfully got end of sequence.
			return 1;
		} else {
			// Failed to receive item.
			return 0;
		}
	}

	return 0;
}

int vine_transfer_get_dir(struct link *lnk, struct vine_cache *cache, const char *dirname, time_t stoptime)
{
	int64_t totalsize = 0;
	char *cached_path = vine_cache_full_path(cache, dirname);
	int r = vine_transfer_get_dir_internal(lnk, cached_path, &totalsize, stoptime);
	if (r) {
		vine_cache_addfile(cache, totalsize, 0755, dirname);
	} else {
		// Remove the file if there's any problem with getting it.
		if (unlink_recursive(cached_path) < 0) {
			debug(D_VINE, "Can't remove invalid file %s: (%s)", cached_path, strerror(errno));
		}
	}
	free(cached_path);
	return r;
}

int vine_transfer_get_file(struct link *lnk, struct vine_cache *cache, const char *filename, int64_t length, int mode,
		time_t stoptime)
{
	char *cached_path = vine_cache_full_path(cache, filename);
	int r = vine_transfer_get_file_internal(lnk, cached_path, length, mode, stoptime);
	if (r) {
		vine_cache_addfile(cache, length, mode, filename);
	} else {
		// Remove the file if there's any problem with getting it.
		if (unlink_recursive(cached_path) < 0) {
			debug(D_VINE, "Can't remove invalid file %s: (%s)", cached_path, strerror(errno));
		}
	}
	free(cached_path);
	return r;
}

int vine_transfer_get_any(struct link *lnk, struct vine_cache *cache, const char *filename, time_t stoptime)
{
	int64_t totalsize = 0;
	send_message(lnk, "get %s\n", filename);
	char *cache_root = vine_cache_full_path(cache, "");
	int r = vine_transfer_get_any_internal(lnk, cache_root, &totalsize, stoptime);
	if (r) {
		vine_cache_addfile(cache, totalsize, 0755, filename);
	} else {
		// Remove the file or directory if there's any problem with getting it.
		char *cached_path = string_format("%s/%s", cache_root, filename);
		if (unlink_recursive(cached_path) < 0) {
			debug(D_VINE, "Can't remove invalid any %s: (%s)", cached_path, strerror(errno));
		}
		free(cached_path);
	}
	free(cache_root);
	return r;
}
