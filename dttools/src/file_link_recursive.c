#include "file_link_recursive.h"
#include "path.h"
#include "stringtools.h"

#include <dirent.h>
#include <string.h>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

int file_link_recursive(const char *source, const char *target, int allow_symlinks)
{
	struct stat info;

	if (lstat(source, &info) < 0)
		return 0;

	if (S_ISDIR(info.st_mode)) {
		DIR *dir = opendir(source);
		if (!dir)
			return 0;

		mkdir(target, 0777);

		struct dirent *d;
		int result = 1;

		while ((d = readdir(dir))) {
			if (!strcmp(d->d_name, "."))
				continue;
			if (!strcmp(d->d_name, ".."))
				continue;

			char *subsource = string_format("%s/%s", source, d->d_name);
			char *subtarget = string_format("%s/%s", target, d->d_name);

			result = file_link_recursive(subsource, subtarget, allow_symlinks);

			free(subsource);
			free(subtarget);

			if (!result)
				break;
		}
		closedir(dir);

		return result;
	} else {
		if (link(source, target) == 0)
			return 1;

		/*
		If the hard link failed, perhaps because the source
		was a directory, or if hard links are not supported
		in that file system, fall back to a symlink.
		*/

		if (allow_symlinks) {

			/*
			Use an absolute path when symlinking, otherwise the link will
			be accidentally relative to the current directory.
			*/

			char *cwd = path_getcwd();
			char *absolute_source = string_format("%s/%s", cwd, source);

			int result = symlink(absolute_source, target);

			free(absolute_source);
			free(cwd);

			if (result == 0)
				return 1;
		}

		return 0;
	}
}
