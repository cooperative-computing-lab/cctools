/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "mapreduce.h"

#include "batch_job.h"
#include "create_dir.h"
#include "copy_stream.h"
#include "debug.h"
#include "delete_dir.h"
#include "list.h"
#include "stringtools.h"
#include "xmalloc.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <fcntl.h>
#include <libgen.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>

struct mr_config {
	char   *mapper;
	char   *reducer;
	char   *inputlist;
	int	bqtype;
	int	nmappers;
	int	nreducers;
	char   *bin_dir;
	char   *scratch_dir;
	int	do_merge;

	char	curr_dir[MR_MAX_STRLEN];
	char	work_dir[MR_MAX_STRLEN];
	int	has_parrot_hdfs;
};

struct mr_job {
	char	args[MR_MAX_STRLEN];
	char	error_file[MR_MAX_STRLEN];
	char	input_files[MR_MAX_STRLEN];
	int	number;
	batch_job_id_t		jid;
	struct batch_job_info	jinfo;
	int	attempts;
};

static void print_mr_config( struct mr_config *cfg ) {
	printf("0. Configuration\n");
	printf("\tMapper:\t%s\n", cfg->mapper);
	printf("\tReducer:\t%s\n", cfg->reducer);
	printf("\tInputlist:\t%s\n", cfg->inputlist);
	printf("\tBatch Queue Type:\t%s\n",batch_queue_type_to_string(cfg->bqtype));
	printf("\tNumber of Mappers:\t%d\n", cfg->nmappers);
	printf("\tNumber of Reducers:\t%d\n", cfg->nreducers);
	printf("\tDo Final Merge:\t%s\n", (cfg->do_merge ? "true" : "false"));
	printf("\tCurrent Directory:\t%s\n", cfg->curr_dir);
	printf("\tWorking Directory:\t%s\n", cfg->work_dir);
	printf("\tScratch Directory:\t%s\n", cfg->scratch_dir);
	printf("\tHas parrot_hdfs:\t%s\n", (cfg->has_parrot_hdfs ? "true" : "false"));
}

static int file_exists( const char *path ) {
	FILE *fp;

	if ((fp = fopen(path, "r"))) {
		fclose(fp);
		return 1;
	}

	return 0;
}

static int copy_file( const char *src, const char *dst, mode_t mode ) {
	FILE *sfp  = NULL;
	FILE *dfp  = NULL;
	int result = -1;

	sfp = fopen(src, "r");
	if (!sfp) goto cf_return;
	
	dfp = fopen(dst, "w");
	if (!dfp) goto cf_return;
	chmod(dst, mode);

	result = copy_stream_to_stream(sfp, dfp);

cf_return:
	if (sfp) fclose(sfp);
	if (dfp) fclose(dfp);
	return result;
}

#define COPY_FILE(s, d) \
	if ((result = copy_file((s), (d), 0777)) < 0) { \
		fprintf(stderr, "\tunable to copy %s to %s", (s), (d)); \
		goto s_return; \
	} \
	printf("\t%s:\tcopied %d bytes\n", string_basename(d), result); \
	total += result; 

#define COPY_BIN_TO_WORK_DIR(bin) \
	snprintf(srcpath, MR_MAX_STRLEN, "%s/%s", cfg->bin_dir, (bin)); \
	snprintf(dstpath, MR_MAX_STRLEN, "%s/%s", cfg->work_dir, (bin)); \
	COPY_FILE(srcpath, dstpath)

#define COPY_FILE_TO_WORK_DIR(s, d) \
	snprintf(srcpath, MR_MAX_STRLEN, "%s", (s)); \
	snprintf(dstpath, MR_MAX_STRLEN, "%s/%s", cfg->work_dir, d); \
	COPY_FILE(srcpath, dstpath)

static int sandbox( struct mr_config *cfg ) {
	char srcpath[MR_MAX_STRLEN];
	char dstpath[MR_MAX_STRLEN];
	int result, total = 0;
	
	printf("1. Sandbox\n");
	
	create_dir(cfg->work_dir, 0700);
	
	COPY_BIN_TO_WORK_DIR("mr_map")
	COPY_BIN_TO_WORK_DIR("mr_merge")
	COPY_BIN_TO_WORK_DIR("mr_reduce")
	COPY_BIN_TO_WORK_DIR("mr_wrapper.sh")
	COPY_BIN_TO_WORK_DIR("parrot")
	if (cfg->has_parrot_hdfs) {
		COPY_BIN_TO_WORK_DIR("parrot_hdfs")
	} 

	COPY_FILE_TO_WORK_DIR(cfg->mapper,    MR_MAPPER)
	COPY_FILE_TO_WORK_DIR(cfg->reducer,   MR_REDUCER)
	COPY_FILE_TO_WORK_DIR(cfg->inputlist, MR_INPUTLIST)

	result = total;

s_return:
	if (result < 0) {
		fprintf(stderr, "\t%s: failed to copy = %s\n", string_basename(srcpath), strerror(errno));
	}
	return result;
}

static int run_batch_jobs( struct mr_config *cfg, struct mr_job **jobs, const char *phase ) {
	struct batch_queue* q;
	int i, njobs, ncompleted = 0, result = 0;
	
	q = batch_queue_create(cfg->bqtype);
	if (!q) return -1;

	if (strcmp(phase, "map") == 0) {
		njobs = cfg->nmappers;
	} else {
		njobs = cfg->nreducers;
	}

	for (i = 0; i < njobs; i++) {
		jobs[i]->number = i;
		jobs[i]->attempts++;
		snprintf(jobs[i]->error_file, MR_MAX_STRLEN, "%s.error.%d", phase, i);
		jobs[i]->jid = batch_job_submit(q, "./mr_wrapper.sh", jobs[i]->args, NULL, NULL, jobs[i]->error_file, jobs[i]->input_files, NULL);
		printf("\t%s job %d:\tsubmitted as job %d\n", phase, jobs[i]->number, jobs[i]->jid);
	}
	
	/** TODO: Check for stragglers (need batch_job_wait_timeout)
	 **/ 
	while (ncompleted < njobs) {
		struct batch_job_info jinfo;
		batch_job_id_t jid;

		jid = batch_job_wait(q, &jinfo);
		for (i = 0; i < njobs; i++) if (jobs[i]->jid == jid) break;
		memcpy((void *)&(jobs[i]->jinfo), (void *)&jinfo, sizeof(struct batch_job_info));

		if (jinfo.exited_normally) {
			if (jinfo.exit_code == 0) {
				printf("\t%s job %d:\tsuccess\n", phase, jobs[i]->number);
				jobs[i]->jid = -1;
				ncompleted++;
				continue;
			} else {
				printf("\t%s job %d:\tfailure\n", phase, jobs[i]->number);
			}
		} else {
			printf("\t%s job %d:\terror\n", phase, jobs[i]->number);
		}

		if (jobs[i]->attempts < MR_MAX_ATTEMPTS) {
			jobs[i]->jid = batch_job_submit(q, "./mr_wrapper.sh", jobs[i]->args, NULL, NULL, jobs[i]->error_file, jobs[i]->input_files, NULL);
			printf("\t%s job %d:\tresubmitted as job %d\n", phase, jobs[i]->number, jobs[i]->jid);
			jobs[i]->attempts++;
		} else {
			printf("\t%s job %d:\ttoo many failed attempts\n", phase, jobs[i]->number);
			jobs[i]->jid = -1;
			result = -1;
			break;
		}
	}

	if (ncompleted != njobs) { 
		for (i = 0; i < njobs; i++) if (jobs[i]->jid >= 0) batch_job_remove(q, jobs[i]->jid);
	}
	return result;
}

#define CLEANUP_JOBS(jobs, njobs) \
	if (jobs) { \
		for (i = 0; i < (njobs); i++) if ((jobs)[i]) free((jobs)[i]); \
		free(jobs); \
	}

static int map( struct mr_config *cfg ) {
	struct mr_job **jobs = NULL; 
	int i, result = -1;
	
	printf("2. Map\n");

	jobs = xxmalloc(sizeof(struct mr_job*)*cfg->nmappers);
	memset(jobs, 0, sizeof(struct mr_job*)*cfg->nmappers);
	for (i = 0; i < cfg->nmappers; i++) {
		jobs[i] = xxmalloc(sizeof(struct mr_job));

		snprintf(jobs[i]->args, MR_MAX_STRLEN, "map %s %d %d %d", cfg->scratch_dir, i, cfg->nmappers, cfg->nreducers);
		snprintf(jobs[i]->input_files, MR_MAX_STRLEN,"parrot,mr_map,%s,%s", MR_MAPPER, MR_INPUTLIST);
		if (cfg->has_parrot_hdfs) {
			strcat(jobs[i]->input_files, ",parrot_hdfs");
		}
	}

	result = run_batch_jobs(cfg, jobs, "map");
	CLEANUP_JOBS(jobs, cfg->nmappers)
	return result;
}

static int reduce( struct mr_config *cfg ) {
	struct mr_job **jobs = NULL; 
	char buffer[MR_MAX_STRLEN];
	int i, result = -1;
	
	printf("3. Reduce\n");

	jobs = xxmalloc(sizeof(struct mr_job*)*cfg->nreducers);
	memset(jobs, 0, sizeof(struct mr_job*)*cfg->nreducers);
	for (i = 0; i < cfg->nreducers; i++) {
		jobs[i] = xxmalloc(sizeof(struct mr_job));
		
		snprintf(jobs[i]->args, MR_MAX_STRLEN, "reduce %s %d %d %d", cfg->scratch_dir, i, cfg->nmappers, cfg->nreducers);
		if (cfg->bqtype != BATCH_QUEUE_TYPE_UNIX && cfg->scratch_dir == MR_DEFAULT_SCRATCH_DIR) {
			snprintf(buffer, MR_MAX_STRLEN, "tar cf reduce.input.%d.tar map.output.*.%d", i, i);
			if (system(buffer) < 0) {
				fprintf(stderr, "\tunable to archive map.output.*.%d into reduce.input.%d.tar: %s", i, i, strerror(errno));
				goto r_return;
			}
			snprintf(buffer, MR_MAX_STRLEN, "rm -f map.output.*.%d", i);
			if (system(buffer) < 0) {
				fprintf(stderr, "\tunable to delete map.output.*.%d: %s", i, strerror(errno));
				goto r_return;
			}
		}
		snprintf(jobs[i]->input_files, MR_MAX_STRLEN,"parrot,mr_reduce,mr_merge,%s,reduce.input.%d.tar", MR_REDUCER, i);
		if (cfg->has_parrot_hdfs) {
			strcat(jobs[i]->input_files, ",parrot_hdfs");
		}
	}
	
	result = run_batch_jobs(cfg, jobs, "reduce");
r_return:
	CLEANUP_JOBS(jobs, cfg->nreducers)
	return result;
}

static int merge( struct mr_config *cfg ) {
	char cmd[MR_MAX_STRLEN];

	if (!cfg->do_merge) return 0;

	printf("4. Merge\n");

	snprintf(cmd, MR_MAX_STRLEN, "./mr_wrapper.sh merge %s m %d %d", cfg->scratch_dir, cfg->nmappers, cfg->nreducers);
	if (system(cmd) < 0) {
		fprintf(stderr, "\tunable to merge final output");
		return -1;
	}
	
	if (cfg->scratch_dir == MR_DEFAULT_SCRATCH_DIR) {
		printf("\tFinal Output:\t%s/merge.output\n", cfg->work_dir);
	} else {
		printf("\tFinal Output:\t%s/merge.output\n", cfg->scratch_dir);
	}

	return 0;
}

static int mapreduce( struct mr_config *cfg ) {
	if (sandbox(cfg) < 0) {
		return EXIT_FAILURE;
	}

	chdir(cfg->work_dir);
	if (map(cfg) < 0) {
		return EXIT_FAILURE;
	}
	if (reduce(cfg) < 0) {
		return EXIT_FAILURE;
	}
	if (merge(cfg) < 0) {
		return EXIT_FAILURE;
	}
	chdir(cfg->curr_dir);

	return EXIT_SUCCESS;
}

static void show_help( const char *cmd ) {
	printf("Use: %s [options] <mapper> <reducer> <inputlist>\n", cmd);
	printf("where general options are:\n");
	printf(" -d <subsystem>   Enable debugging for this subsystem.  (Try -d all to start.)\n");
	printf(" -o <file>        Send debugging to this file.\n");
	printf(" -h               Show this help screen\n");
	printf(" -v               Show version string\n\n");
	
	printf("where mapreduce options are:\n");
	printf(" -M               Perform final merge.\n");
	printf(" -q <bqtype>      Type of batch queue (condor or unix).\n");
	printf(" -m <nmappers>    Number of mappers.\n");
	printf(" -r <nreducers>   Number of reducers.\n");
	printf(" -b <bin_dir>     Path to executable binaries.\n");
	printf(" -s <scratch_dir> Scratch directory.\n");
}

static void show_version( const char *cmd ) {       
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

int main( int argc, char *argv[] ) {
	const char *progname = "mapreduce";
	struct mr_config cfg;
	char c;

	cfg.mapper      = NULL;
	cfg.reducer     = NULL;
	cfg.nmappers    = MR_DEFAULT_NMAPPERS;
	cfg.nreducers   = MR_DEFAULT_NREDUCERS;
	cfg.bqtype      = MR_DEFAULT_BQTYPE;
	cfg.bin_dir     = MR_DEFAULT_BIN_DIR;
	cfg.scratch_dir = MR_DEFAULT_SCRATCH_DIR;
	cfg.do_merge	= 0;
	cfg.has_parrot_hdfs = 0;
	
	debug_config(progname);
	
	while ((c = getopt(argc, argv, "d:o:hvMq:m:r:b:s:")) != (char)-1) {
		switch (c) {
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'o':
				debug_config_file(optarg);
				break;
			case 'h':
				show_help(progname);
				exit(EXIT_SUCCESS);
				break;
			case 'v':
				show_version(progname);
				exit(EXIT_SUCCESS);
				break;
			case 'M':
				cfg.do_merge = 1;
				break;
			case 'q':
				cfg.bqtype = batch_queue_type_from_string(optarg);
				if(cfg.bqtype==BATCH_QUEUE_TYPE_UNKNOWN) {
					fprintf(stderr, "unknown batch queue type: %s\n", optarg);
					exit(EXIT_FAILURE);
				}
				break;
			case 'm':
				cfg.nmappers = atoi(optarg);
				if (!cfg.nmappers) {
					fprintf(stderr, "invalid number of mappers: %d\n", cfg.nmappers);
					exit(EXIT_FAILURE);
				}
				break;
			case 'r':
				cfg.nreducers = atoi(optarg);
				if (!cfg.nmappers) {
					fprintf(stderr, "invalid number of reducers: %d\n", cfg.nreducers);
					exit(EXIT_FAILURE);
				}
				break;
			case 'b':
				cfg.bin_dir = optarg;
				break;
			case 's':
				cfg.scratch_dir = optarg;
				break;
			default:
				fprintf(stderr, "unknown option flag: %c\n", c);
				exit(EXIT_FAILURE);
		}
	}
	
	if ((argc - optind) != 3) {
		show_help(progname);
		exit(EXIT_FAILURE);
	}
		
	cfg.mapper    = argv[optind];
	cfg.reducer   = argv[optind + 1];
	cfg.inputlist = argv[optind + 2];

	getcwd(cfg.curr_dir, MR_MAX_STRLEN);
	snprintf(cfg.work_dir, MR_MAX_STRLEN, "/tmp/mapreduce-%d-%d", getuid(), getpid());
	
	char path[MR_MAX_STRLEN];
	snprintf(path, MR_MAX_STRLEN, "%s/parrot_hdfs", cfg.bin_dir);
	cfg.has_parrot_hdfs = file_exists(path);

	print_mr_config(&cfg);

	return mapreduce(&cfg);
}

// vim: sw=8 sts=8 ts=8 ft=cpp
