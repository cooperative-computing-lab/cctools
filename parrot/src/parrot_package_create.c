#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include <utime.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <getopt.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>
#include <libgen.h>
#include <fcntl.h>
#include <sys/sendfile.h>
#include <time.h>

#include "copy_stream.h"
#include "debug.h"

const char *namelist;
const char *packagepath;
const char *envlist;

int line_process(const char *path, char *caller, int ignore_direntry, int is_direntry, FILE *special_file);

//files from these paths will be ignored.
const char *special_path[] = {"var", "sys", "dev", "proc", "net", "misc", "selinux"};
#define special_path_len (sizeof(special_path))/(sizeof(const char *))

mode_t default_dirmode = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH;
mode_t default_regmode = S_IRWXU | S_IRGRP;

static void show_help(const char *cmd)
{
	fprintf(stdout, "Use: %s [options] ...\n", cmd);
	fprintf(stdout, " %-34s The path of the namelist list.\n", "-n,--name-list=<listpath>");
	fprintf(stdout, " %-34s The path of the environment variable file.\n", "-e,--env-list=<envlist>");
	fprintf(stdout, " %-34s The path of the package.\n", "-p,--package-path=<packagepath>");
	fprintf(stdout, " %-34s Enable debugging for this sub-system.    (PARROT_DEBUG_FLAGS)\n", "-d,--debug=<name>");
	fprintf(stdout, " %-34s Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal) (PARROT_DEBUG_FILE)\n", "-o,--debug-file=<file>");
	fprintf(stdout, " %-34s Show the help info.\n", "-h,--help");
	return;
}

void print_time()
{
	time_t curtime;
	struct tm *loctime;
	curtime = time(NULL);
	loctime = localtime(&curtime);
	if(fputs(asctime(loctime), stdout) == EOF) {
		debug(D_DEBUG, "fputs fails: %s\n", strerror(errno));
	}
}

/* Use `sort -u` shell command to sort the namelist and remove the duplicate items. */
int sort_uniq_namelist(const char *filename, int *fd) {
	int fds[2];
	pid_t pid;
	if(pipe(fds)) {
		debug(D_DEBUG, "pipe fails: %s\n", strerror(errno));
		return -1;
	}
	pid = fork();
	if (pid == 0) {
		int input = open(filename, O_RDONLY);
		if(input == -1) {
			debug(D_DEBUG, "sort_uniq_namelist: open(`%s`) func fails: %s\n", filename, strerror(errno));
			exit(EXIT_FAILURE);
		}
		if(dup2(input, STDIN_FILENO) == -1) {
			debug(D_DEBUG, "sort_uniq_namelist: dup2 fails: %s\n", strerror(errno));
			exit(EXIT_FAILURE);
		}
		close(input);
		close(fds[0]);
		if(dup2(fds[1], STDOUT_FILENO) == -1) {
			debug(D_DEBUG, "sort_uniq_namelist: dup2 fails: %s\n", strerror(errno));
			exit(EXIT_FAILURE);
		}
		close(fds[1]);
		if(execlp("sort", "sort", "-u", NULL) == -1) {
			debug(D_DEBUG, "sort_uniq_namelist: execlp fails: %s\n", strerror(errno));
			exit(EXIT_FAILURE);
		}
	} else if (pid > 0) {
		close(fds[1]);
		*fd = fds[0];
	} else {
		debug(D_DEBUG, "fork fails: %s\n", strerror(errno));
		return -1;
	}
	return 0;
}

/* Transfer absolute symlink into relative symlink. (e.g. `/home/hmeng/linkfile` will become `../../home/hmeng/linkfile`) */
void relative_path(char *newpath, const char *oldpath, const char *path)
{
	char *s;
	strcpy(newpath, "");
	s = strchr(path, '/');
	while(s != NULL) {
		s++;
		strcat(newpath, "../");
		s = strchr(s, '/');
	}
	newpath[strlen(newpath) - 4] = '\0';
	strcat(newpath, oldpath);
}

void remove_final_slashes(char *path)
{
	int n;
	n = strlen(path);
	while(path[n-1] == '/') {
		n--;
	}
	path[n] = '\0';
}

/*
Function with behaviour like `mkdir -p'.
Correctly copying the file permissions from AFS items into the package,
which is stored on the local filesystem, is inefficient, because AFS has
its own ACLs, which is different UNIX file permission mechanism.
If `fixed_mode` is 1, use the mode parameter; otherwise use the mode of the original file.
Currently, each directory is created using fixed mode (i.e., fixed_mode = 1).
*/
int mkpath(const char *path, mode_t mode, int fixed_mode) {
    (void)mode; /* silence warnings */
	debug(D_DEBUG, "mkpath(`%s`) func\n", path);
	if(access(path, F_OK) == 0) {
		debug(D_DEBUG, "%s already exists, mkpath exist!\n", path);
		return 0;
	}

	if(fixed_mode == 0) {
		const char *old_path;
		old_path = path + strlen(packagepath);
		struct stat st;
		if(stat(old_path, &st) == 0) {
			mode = st.st_mode;
		} else {
			debug(D_DEBUG, "stat(`%s`) fails: %s\n", old_path, strerror(errno));
			return -1;
		}
	}

	char pathcopy[PATH_MAX], *parent_dir;
	int rv;
	rv = -1;
	if(strcmp(path, ".") == 0 || strcmp(path, "/") == 0)
		return 0;

	if(strcpy(pathcopy, path) == NULL)
		return -1;

	if((parent_dir = dirname(pathcopy)) == NULL)
		return -1;

	if((mkpath(parent_dir, default_dirmode, 1) == -1) && (errno != EEXIST))
		return -1;

	if((mkdir(path, default_dirmode) == -1) && (errno != EEXIST))
		rv = -1;
	else
		rv = 0;
	return rv;
}


/*
preprocess: check whether the environment variable file exists; check whether the list namelist file exists; check whether the package path exists
create the package directory.
*/
int prepare_work()
{
	if(access(envlist, F_OK) == -1) {
		fprintf(stderr, "The environment variable file (`%s`) does not exist.\n", envlist);
		return -1;
	}
	if(access(namelist, F_OK) == -1) {
		fprintf(stderr, "The namelist file (`%s`) does not exist.\n", namelist);
		return -1;
	}
	if(access(packagepath, F_OK) != -1) {
		fprintf(stderr, "The package path (`%s`) has already existed, please delete it first or refer to another package path.\n", packagepath);
		return -1;
	}
	if(mkpath(packagepath, default_dirmode, 1) == -1) {
		fprintf(stderr, "mkdir(`%s`) fails: %s\n", packagepath, strerror(errno));
		return -1;
	}
	return 0;
}

/*
If the path path is special, ignore it.
*/
int is_special_path(const char *path)
{
	int size;
	unsigned int i;
	char pathcopy[PATH_MAX], *first_dir, *tmp_dir;
	strcpy(pathcopy, path);
	first_dir = strchr(pathcopy, '/') + 1;
	if((tmp_dir = strchr(first_dir, '/')) == NULL) {
		size = strlen(first_dir);
	} else {
		size = strlen(first_dir) - strlen(tmp_dir);
	}
	first_dir[size] = '\0';
	for(i = 0; i < special_path_len; i++){
		if(strcmp(special_path[i], first_dir) == 0) {
			return 1;
		}
	}
	if(strcmp("afs", first_dir) == 0)
		return 2;
	else
		return 0;
}

/*
Create one subitem entry of one directory using metadatacopy.
Currently only copy DIR REG LINK; the remaining files are ignored.
*/
int dir_entry(const char* filename, FILE *special_file)
{
	struct stat source_stat;
	char new_path[PATH_MAX];
	strcpy(new_path, packagepath);
	strcat(new_path, filename);
	if(access(new_path, F_OK) == 0) {
		debug(D_DEBUG, "dir_entry: `%s` already exists\n", new_path);
	} else if(lstat(filename, &source_stat) == 0) {
		if(S_ISDIR(source_stat.st_mode)) {
			debug(D_DEBUG, "dir_entry: `%s`, ---dir\n", filename);
		} else if(S_ISREG(source_stat.st_mode)) {
			debug(D_DEBUG, "dir_entry: `%s`, ---regular file\n", filename);
		} else if(S_ISLNK(source_stat.st_mode)) {
			debug(D_DEBUG, "dir_entry: `%s`, ---link file!\n", filename);
		} else if(S_ISCHR(source_stat.st_mode)) {
			debug(D_DEBUG, "dir_entry: `%s`, ---character!\n", filename);
		} else if(S_ISBLK(source_stat.st_mode)) {
			debug(D_DEBUG, "dir_entry: `%s`, ---block!\n", filename);
		} else if(S_ISFIFO(source_stat.st_mode)) {
			debug(D_DEBUG, "dir_entry: `%s`, ---fifo special file!\n", filename);
		} else if(S_ISSOCK(source_stat.st_mode)) {
			debug(D_DEBUG, "dir_entry: `%s`, ---socket file!\n", filename);
		}
		line_process(filename, "metadatacopy", 1, 1, special_file);
	} else {
		debug(D_DEBUG, "lstat(`%s`): %s\n", filename, strerror(errno));
		exit(EXIT_FAILURE);
	}
	return 0;
}

/* Create empty subitems of one directory to maintain its structure. */
int create_dir_subitems(const char *path, char *new_path, FILE *special_file) {
	DIR *dir;
	struct dirent *entry;
	char full_entrypath[PATH_MAX], dir_name[PATH_MAX];
	if(strcpy(dir_name, path) == NULL) {
		debug(D_DEBUG, "create_dir_subitems(`%s`) error: %s\n", path, strerror(errno));
		return -1;
	}
	debug(D_DEBUG, "create_dir_subitems(`%s`) func\n", path);
	strcat(dir_name, "/");
	if ((dir = opendir(path)) != NULL)
	{
		while ((entry = readdir(dir)))
		{
			strcpy(full_entrypath, dir_name);
			strcat(full_entrypath, entry->d_name);
			dir_entry(full_entrypath, special_file);
		}
		closedir(dir);
	} else {
		debug(D_DEBUG, "Couldn't open the directory `%s`.\n", path);
	}
	return 0;
}

/*
ignore_direntry is to tell whether the directory struture of one directory needs to be maintained. The directory structure here means: create each subitems but not copy their contents.
if is_direntry is 1, ignore the process to check whether its parent dir has been created in the target package, which can greatly reduce the amount of `access` syscall.
Currently only process DIR REG LINK, all the remaining files are ignored.
*/
int line_process(const char *path, char *caller, int ignore_direntry, int is_direntry, FILE *special_file)
{
	debug(D_DEBUG, "line_process(`%s`) func\n", path);
	int afs_item = 0;
	switch(is_special_path(path)) {
	case 1:
		debug(D_DEBUG, "`%s`: Special path, ignore!\n", path);
		return 0;
	case 2:
		debug(D_DEBUG, "this path is under /afs!\n");
		afs_item = 1;
		break;
	default:
		break;
	}
	int fullcopy, existance;
	char new_path[PATH_MAX];
	struct stat source_stat;
	fullcopy = 0;
	ignore_direntry = 1;
	if(strcmp(caller,"metadatacopy") == 0) {
		fullcopy = 0;
	} else {
		fullcopy = 1;
		ignore_direntry = 0;
	}

	strcpy(new_path, packagepath);
	strcat(new_path, path);
	/* existance: whether this item has existed in the target package. */
	existance = 0;
	/* if the item has existed in the target package and the copy degree is `metadatacopy`, it is done! */
	if(access(new_path, F_OK) == 0) {
		existance = 1;
		if(fullcopy == 0) {
			debug(D_DEBUG, "`%s`: metadata copy, already exist!\n", path);
			return 0;
		}
	}

	if(lstat(path, &source_stat) == -1) {
		debug(D_DEBUG, "lstat(`%s`): %s\n", path, strerror(errno));
		return -1;
	}

	if(S_ISREG(source_stat.st_mode)) {
		debug(D_DEBUG, "`%s`: regular file\n", path);
		if(existance) {
			/* here, the copy degree must be fullcopy. */
			struct stat target_stat;
			if(stat(new_path, &target_stat) == -1) {
				debug(D_DEBUG, "stat(%s) fails: %s\n", new_path, strerror(errno));
				return -1;
			}
			/*
			Firstly we tried to use use `truncate` system call to change the size of one empty file (`st_size`) and use `st_blocks` to check whether a file
			is really empty. In normal linux filesystem, the `st_blocks` of one empty file is always 0 even if its size is set to non-zero by truncate.
			However, we give up truncate finally. Because using truncate system call on an empty file on afs results in the `st_blocks` becomes non-zero.
			*/
			if(target_stat.st_size) {
				debug(D_DEBUG, "`%s`: fullcopy exist! pass!\n", path);
			} else {
				if(remove(new_path) == -1) {
					debug(D_DEBUG, "remove(`%s`) fails: %s\n", new_path, strerror(errno));
					return -1;
				}
				if(copy_file_to_file(path, new_path) < 0) {
					debug(D_DEBUG, "copy_file_to_file from %s to %s fails.\n", path, new_path);
					return -1;
				}
				else
					debug(D_DEBUG, "`%s`: fullcopy not exist, metadatacopy exist! create fullcopy ...\n", path);
			}
		} else {
			if(is_direntry == 0) {
				char tmppath[PATH_MAX], dir_name[PATH_MAX];
				strcpy(tmppath, path);
				strcpy(dir_name, dirname(tmppath));
				line_process(dir_name, "metadatacopy", 1, 0, special_file);
			}
			if(fullcopy) {
				if(copy_file_to_file(path, new_path) < 0) {
					debug(D_DEBUG, "copy_file_to_file from %s to %s fails.\n", path, new_path);
					return -1;
				}
				else
					debug(D_DEBUG, "`%s`: fullcopy not exist, metadatacopy not exist! create fullcopy ...\n", path);
			} else {
				int fd = open(new_path, O_CREAT|O_WRONLY, S_IRUSR|S_IWUSR);
				if (fd == -1) {
					debug(D_DEBUG, "open(`%s`) fails: %s\n", new_path, strerror(errno));
					return -1;
				}
				close(fd);
				debug(D_DEBUG, "`%s`: metadatacopy not exist! create metadatacopy ...\n", path);
			}
		}
		/* copy the metadata info of the file */
		struct utimbuf time_buf;
		time_buf.modtime = source_stat.st_mtime;
		time_buf.actime = source_stat.st_atime;
		if(utime(new_path, &time_buf) == -1) {
			debug(D_DEBUG, "utime(`%s`) fails: %s\n", new_path, strerror(errno));
			return -1;
		}
		/* if the path is under /afs, use a fixed default st_mode setting instead of its original st_mode. */
		if(afs_item) {
			if(chmod(new_path, default_regmode) == -1) {
				debug(D_DEBUG, "chmod(`%s`) fails: %s\n", new_path, strerror(errno));
				return -1;
			}
		}
		if(chmod(new_path, source_stat.st_mode) == -1) {
			debug(D_DEBUG, "chmod(`%s`) fails: %s\n", new_path, strerror(errno));
			return -1;
		}
	} else if(S_ISDIR(source_stat.st_mode)) {
		debug(D_DEBUG, "`%s`: regular dir\n", path);
		if(is_direntry == 0) {
			if(mkpath(new_path, default_dirmode, 1) == -1) {
				debug(D_DEBUG, "mkpath(`%s`) fails.\n", new_path);
				return -1;
			}
			if((ignore_direntry == 0) && (create_dir_subitems(path, new_path, special_file) == -1)) {
				debug(D_DEBUG, "create_dir_subitems(`%s`) fails.\n", path);
				return -1;
			}
		} else {
			if(mkdir(new_path, default_dirmode) == -1) {
				debug(D_DEBUG, "mkdir(`%s`) fails: %s\n", new_path, strerror(errno));
				return -1;
			}
		}
	} else if(S_ISLNK(source_stat.st_mode)) {
		char buf[PATH_MAX], linked_path[PATH_MAX], pathcopy[PATH_MAX], dir_name[PATH_MAX], newbuf[PATH_MAX];
		int len;
		/* first use `readlink` to obtain the target of the symlink. */
		if ((len = readlink(path, buf, sizeof(buf)-1)) != -1) {
			buf[len] = '\0';
		} else {
			debug(D_DEBUG, "readlink(`%s`) fails: %s\n", path, strerror(errno));
			return -1;
		}
		debug(D_DEBUG, "`%s`: symbolink, the direct real path: `%s`\n", path, buf);

		/* then obtain the complete path of the target of the symlink. */
		strcpy(pathcopy, path);
		strcpy(dir_name, dirname(pathcopy));
		if(buf[0] == '/') {
			strcpy(linked_path, buf);
		} else {
			strcpy(linked_path, dir_name);
			if(linked_path[strlen(linked_path) - 1] != '/')
				strcat(linked_path, "/");
			strcat(linked_path, buf);
		}
		debug(D_DEBUG, "the relative version of direct real path `%s` is: `%s`\n", path, linked_path);

		/* process the target of the symlink recursively. */
		if(fullcopy) {
			line_process(linked_path, "fullcopy", 0, 0, special_file);
		} else {
			line_process(linked_path, "metadatacopy", 1, 0, special_file);
		}

		/* ensure the directory of the symlink has been created in the target package. */
		if(is_direntry == 0) {
			char new_dir[PATH_MAX];
			strcpy(new_dir, packagepath);
			strcat(new_dir, dir_name);
			if(access(new_dir, F_OK) == -1) {
				debug(D_DEBUG, "the dir `%s` of the target of symbolink file `%s` does not exist, need to be created firstly\n", dir_name, path);
				line_process(dir_name, "metadatacopy", 1, 0, special_file);
			}
		}

		/* Transform absolute symlink into relative symlink. */
		if(buf[0] == '/') {
			relative_path(newbuf, buf, path);
			strcpy(buf, newbuf);
		}
		/* Create the symlink relationship */
		if(symlink(buf, new_path) == -1) {
			debug(D_DEBUG, "symlink from `%s` to `%s` create fail, %s\n", new_path, buf, strerror(errno));
			return -1;
		} else {
			debug(D_DEBUG, "create symlink from `%s` to `%s`.\n", new_path, buf);
		}
	} else {
		debug(D_DEBUG, "The file type is not DIR or REG or LINK, write this item into special file!\n");
		char item[PATH_MAX*2];
		snprintf(item, sizeof(item), "%s %s\n", path, path);
		if (fputs(item, special_file) == EOF) {
			debug(D_DEBUG, "fputs special_file fails: %s\n", strerror(errno));
			return -1;
		}
	}
	return 0;
}

/* copy the environment variable file into the package; create common-mountlist file. */
int post_process( ) {
	char new_envlist[PATH_MAX], common_mountlist[PATH_MAX], size_cmd[PATH_MAX], cmd_rv[100];
	FILE *file, *cmd_fp;

	snprintf(new_envlist, PATH_MAX, "%s/%s", packagepath, "env_list");
	if(copy_file_to_file(envlist, new_envlist) == -1) {
		debug(D_DEBUG, "copy_file_to_file(`%s`) fails.\n", envlist);
		return -1;
	}

	snprintf(common_mountlist, PATH_MAX, "%s/%s", packagepath, "common-mountlist");
	file = fopen(common_mountlist, "w");
	if(!file) {
		debug(D_DEBUG, "common-mountlist file `%s` can not be opened.\n", common_mountlist);
		return -1;
	}
	if((fputs("/dev /dev\n", file) == EOF) ||
		(fputs("/misc /misc\n", file) == EOF) ||
		(fputs("/net /net\n", file) == EOF) ||
		(fputs("/proc /proc\n", file) == EOF) ||
		(fputs("/sys /sys\n", file) == EOF) ||
		(fputs("/var /var\n", file) == EOF) ||
		(fputs("/selinux /selinux\n", file) == EOF)) {
			debug(D_DEBUG, "fputs fails: %s\n", strerror(errno));
			exit(EXIT_FAILURE);
	}
	fclose(file);

	fprintf(stdout, "Package Path: %s\nPackage Size: ", packagepath);
	snprintf(size_cmd, PATH_MAX, "du -hs %s", packagepath);

	cmd_fp = popen(size_cmd, "r");
	if(cmd_fp == NULL) {
		debug(D_DEBUG, "popen(`%s`) fails: %s\n", size_cmd, strerror(errno));
		return -1;
	}
	while(fgets(cmd_rv, sizeof(cmd_rv) - 1, cmd_fp) != NULL) {
		fprintf(stdout, "%s", cmd_rv);
	}
	pclose(cmd_fp);
	return 0;
}

static void wait_for_children(int sig)
{
	while(wait(NULL) >= 0 || errno == EINTR)
		;
}

int main(int argc, char *argv[])
{
	int c, fd, count, path_len;
	FILE *namelist_file;
	char line[PATH_MAX], path[PATH_MAX], *caller;

	signal(SIGCHLD, wait_for_children);

	struct option long_options[] = {
		{"help", no_argument, 0, 'h'},
		{"name-list", required_argument, 0, 'n'},
		{"env-list", required_argument, 0, 'e'},
		{"package-path", required_argument, 0, 'p'},
		{"debug", required_argument, 0, 'd'},
		{"debug-file", required_argument, 0, 'o'},
		{0,0,0,0}
	};

	while((c=getopt_long(argc, argv, "+hd:o:e:n:p:", long_options, NULL)) > -1) {
		switch(c) {
		case 'e':
			envlist = optarg;
			break;
		case 'n':
			namelist = optarg;
			break;
		case 'p':
			packagepath = optarg;
			break;
		case 'd':
			if(!debug_flags_set(optarg)) show_help(argv[0]);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		default:
			show_help(argv[0]);
			exit(EXIT_FAILURE);
			break;
		}
	}
	debug_config_file_size(0); /* do not rotate debug file by default */

	//preprocess: check whether the environment variable file exists; check whether the namelist file exists; check whether the package path exists;
	if((prepare_work()) != 0) {
		show_help(argv[0]);
		exit(EXIT_FAILURE);
	}

	fprintf(stdout, "The packaging process has began ...\nThe start time is: ");
	print_time();

	char special_filename[PATH_MAX];
	FILE *special_file;
	snprintf(special_filename, PATH_MAX, "%s%s", packagepath, "/special_files");
	special_file = fopen(special_filename, "w");
	if(!special_file) {
		debug(D_DEBUG, "fopen(`%s`) failed: %s\n", special_filename, strerror(errno));
		exit(EXIT_FAILURE);
	}

	if(sort_uniq_namelist(namelist, &fd) == -1) {
		debug(D_DEBUG, "sort_uniq_namelist func fails.\n");
		exit(EXIT_FAILURE);
	}

	namelist_file = fdopen(fd, "r");
	if(!namelist_file) {
		debug(D_DEBUG, "`sort -u %s` execute fails. fdopen fails: %s\n", packagepath, strerror(errno));
		exit(EXIT_FAILURE);
	}
	count = 0;
	while(fgets(line, PATH_MAX, namelist_file) != NULL) {
		count++;
		char *s;
		if((s = strchr(line, '|')) != NULL) {
			caller = s + 1;
			caller[strlen(caller) - 1] = '\0';
			path_len = strlen(line) - strlen(caller) - 1;
			strcpy(path, line);
			path[path_len] = '\0';
		} else {
			caller = "open_object";
			strcpy(path, line);
			path[strlen(line) - 1] = '\0';
			path_len = strlen(path);
		}
		remove_final_slashes(path);
		debug(D_DEBUG, "%d --- line: %s; path_len: %d\n", count, line, path_len);
		if(line_process(path, caller, 0, 0, special_file) == -1)
			debug(D_DEBUG, "line(%s) does not been processed perfectly.\n", line);
	}
	fclose(namelist_file);
	fclose(special_file);
	if(post_process() == -1) {
		debug(D_DEBUG, "post_process fails.\n");
		exit(EXIT_FAILURE);
	}

	fprintf(stdout, "The packaging process has finished.\nThe end time is: ");
	print_time();
	return 0;
}
