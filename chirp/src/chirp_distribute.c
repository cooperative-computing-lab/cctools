/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
chirp_distribute copies a directory from one host to many others
by building a spanning tree at runtime using third party transfers.
The -X option will delete the directory from all of the named hosts.
*/

#include "chirp_client.h"
#include "chirp_reli.h"

#include "auth.h"
#include "auth_all.h"
#include "cctools.h"
#include "debug.h"
#include "stringtools.h"
#include "timestamp.h"
#include "macros.h"
#include "random.h"
#include "getopt_aux.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include <signal.h>

static int timeout = 300;
static int overall_timeout = 3600;
static int overall_stoptime = 0;
static int did_explicit_auth = 0;
static int destroy_mode = 0;
static int detail_mode = 0;
static int maxprocs = 100;
static int randomize_mode = 0;
static int confirm_mode = 0;
static int transfers_needed = 0;
static int transfers_complete = 0;

static char *failure_matrix = 0;
static int failure_matrix_size = 0;
static char *failure_matrix_filename = 0;

const double threshold = .2;

static double *bw_matrix = 0;
static int bw_matrix_size = 400;


static void bw_matrix_init(int n)
{
	bw_matrix = malloc(sizeof(double) * n * n);
	memset(bw_matrix, 0, sizeof(double) * n * n);
	bw_matrix_size = n;
}

static void bw_matrix_set(int s, int t, double c)
{
	bw_matrix[s * bw_matrix_size + t] = c;
}

static double bw_matrix_get(int s, int t)
{
	return bw_matrix[s * bw_matrix_size + t];
}

static double bw_find_max(int s)
{
	int max = 0, i;
	for(i = 0; i < bw_matrix_size; i++) {
		if(max < bw_matrix_get(s, i))
			max = bw_matrix_get(s, i);
	}
	return max;
}



static int compute_stoptime()
{
	return MIN(time(0) + timeout, overall_stoptime);
}

#define FAILURE_MARK_NONE    ' '
#define FAILURE_MARK_FAILED  '#'
#define FAILURE_MARK_SUCCESS '+'

static void failure_matrix_init(int n)
{
	failure_matrix = malloc(sizeof(char) * n * n);
	memset(failure_matrix, FAILURE_MARK_NONE, sizeof(char) * n * n);
	failure_matrix_size = n;
}

static void failure_matrix_set(int s, int t, char c)
{
	failure_matrix[s * failure_matrix_size + t] = c;
}

static char failure_matrix_get(int s, int t)
{
	return failure_matrix[s * failure_matrix_size + t];
}

static void failure_matrix_print()
{
	FILE *file;
	int i, j;

	if(!failure_matrix_filename)
		return;

	file = fopen(failure_matrix_filename, "w");
	if(!file)
		return;

	for(i = 0; i < failure_matrix_size; i++) {
		for(j = 0; j < failure_matrix_size; j++) {
			fputc(failure_matrix_get(i, j), file);
		}
		fputc('\n', file);
	}

	fclose(file);
}

typedef enum {
	TARGET_STATE_FRESH,
	TARGET_STATE_RECEIVING,
	TARGET_STATE_SENDING,
	TARGET_STATE_IDLE,
	TARGET_STATE_FAILED
} target_state_t;



struct m_server_info {
	char *name;
	int cid;
	int index;
	double max;
	int status;
};

struct server_info {
	char *name;
	int cid;
};

struct target_info {
	const char *name;
	target_state_t state;
	pid_t pid;
	int cid;
};

static void show_help()
{
	fprintf(stdout, "Use: chirp_distribute [options] <sourcehost> <sourcepath> <host1> <host2> ...\n");
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " %-30s Require this authentication mode.\n", "-a,--auth=<flag>");
	fprintf(stdout, " %-30s Enable debugging for this subsystem.\n", "-d,--debug=<flag>");
	fprintf(stdout, " %-30s Show detailed location, time, and performance of each transfer.\n", "-D,--info-transfer");
	fprintf(stdout, " %-30s Write matrix of failures to this file.\n", "-F,--failures-file=<file>");
	fprintf(stdout, " %-30s Comma-delimited list of tickets to use for authentication.\n", "-i,--tickets=<files>");
	fprintf(stdout, " %-30s Stop after this number of successful copies.\n", "-N,--copies-max=<num>");
	fprintf(stdout, " %-30s Maximum number of processes to run at once (default=%d)\n", "-p,--jobs=<num>", maxprocs);
	fprintf(stdout, " %-30s Randomize order of target hosts given on command line.\n", "-R,--randomize-hosts");
	fprintf(stdout, " %-30s Timeout for for each copy. (default is %ds)\n", "-t,--timeout=<time>", timeout);
	fprintf(stdout, " %-30s Overall timeout for entire distribution. (default is %d)\n", "-T,--timeout-all=<time>", overall_timeout);
	fprintf(stdout, " %-30s Show program version.\n", "-v,--version");
	fprintf(stdout, " %-30s Delete data from all of the target hosts.\n", "-X,--delete-target");
	fprintf(stdout, " %-30s Show confirmation of successful placements.\n", "-Y,--info-success");
	fprintf(stdout, " %-30s This message.\n", "-h,--help");
	fprintf(stdout, "\nchirp_distribute copies a directory from one host to many hosts\n");
	fprintf(stdout, "by creating a spanning tree and then transferring data in parallel\n");
	fprintf(stdout, "using third party transfer.  The path of each newly created copy\n");
	fprintf(stdout, "is printed on stdout.  The -X option deletes all but one copy.\n\n");
}

int main(int argc, char *argv[])
{
	INT64_T result;
	signed char c;
	char *sourcehost, *sourcepath;
	struct target_info *targets;
	struct server_info *servers;
	int ntargets;
	int i, j;
	pid_t pid;
	int nprocs = 0;
	char *tickets = NULL;

	int length;		//, start_time, end_time;
	int server_count = 0;
	int cluster_count = 0;

	random_init();

	debug_config(argv[0]);

	static const struct option long_options[] = {
		{"auth", required_argument, 0, 'a'},
		{"debug", required_argument, 0, 'd'},
		{"info-transfer", no_argument, 0, 'D'},
		{"failures-file", required_argument, 0, 'F'},
		{"tickets", required_argument, 0, 'i'},
		{"copies-max", required_argument, 0, 'N'},
		{"jobs", required_argument, 0, 'p'},
		{"randomize-hosts", no_argument, 0, 'R'},
		{"timeout", required_argument, 0, 't'},
		{"timeout-all", required_argument, 0, 'T'},
		{"version", no_argument, 0, 'v'},
		{"delete-target", no_argument, 0, 'X'},
		{"info-success", no_argument, 0, 'Y'},
		{"help", no_argument, 0, 'h'},
		{0, 0, 0, 0}
	};

	while(((c = getopt_long(argc, argv, "a:d:DF:i:N:p:Rt:T:vXYh", long_options, NULL)) > -1)) {
		switch (c) {
		case 'R':
			randomize_mode = 1;
			break;
		case 'X':
			destroy_mode = 1;
			break;
		case 'Y':
			confirm_mode = 1;
			break;
		case 'D':
			detail_mode = 1;
			break;
		case 'F':
			failure_matrix_filename = optarg;
			break;
		case 'T':
			overall_timeout = string_time_parse(optarg);
			break;
		case 'N':
			transfers_needed = atoi(optarg);
			break;
		case 'p':
			maxprocs = atoi(optarg);
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			return 0;
			break;
		case 't':
			timeout = string_time_parse(optarg);
			break;
		case 'a':
			if (!auth_register_byname(optarg))
				fatal("could not register authentication method `%s': %s", optarg, strerror(errno));
			did_explicit_auth = 1;
			break;
		case 'i':
			tickets = strdup(optarg);
			break;
		default:
		case 'h':
			show_help();
			return 1;
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if(!did_explicit_auth)
		auth_register_all();
	if(tickets) {
		auth_ticket_load(tickets);
		free(tickets);
	} else if(getenv(CHIRP_CLIENT_TICKETS)) {
		auth_ticket_load(getenv(CHIRP_CLIENT_TICKETS));
	} else {
		auth_ticket_load(NULL);
	}

	if((argc - optind) < 2) {
		show_help();
		return 1;
	}


	sourcehost = argv[optind];
	sourcepath = argv[optind + 1];
	ntargets = argc - optind - 1;



	struct chirp_stat buf;

	char home_dir[50];
	sprintf(home_dir, "%s/.chirp", getenv("HOME"));
	mkdir(home_dir, 0777);

	result = chirp_reli_stat(sourcehost, sourcepath, &buf, time(0) + 20);
	if(result < 0) {
		printf("%s %s %" PRId64 " %i %s chirp stat failed\n", sourcehost, sourcepath, result, errno, strerror(errno));
		if(errno == 2)
			return 0;
	}


	if(randomize_mode) {
		for(i = 0; i < ntargets * 10; i++) {
			int a = optind + 2 + rand() % (ntargets - 1);
			int b = optind + 2 + rand() % (ntargets - 1);
			char *t;

			t = argv[a];
			argv[a] = argv[b];
			argv[b] = t;
		}
	}

	overall_stoptime = time(0) + overall_timeout;

	failure_matrix_init(ntargets);

	if(destroy_mode) {
		INT64_T result;
		int status;

		for(i = 0; i < (ntargets - 1); i++) {
			if(nprocs < maxprocs) {
				//printf("%s\n",argv[optind+2+i]);
				nprocs++;
				fflush(0);
				pid = fork();
				if(pid > 0) {
					/* keep going */
				} else if(pid == 0) {
					result = chirp_reli_rmall(argv[optind + 2 + i], sourcepath, compute_stoptime());
					if(result == 0) {
						printf("destroyed %s\n", argv[optind + 2 + i]);
						exit(0);
					} else {
						printf("couldn't destroy %s: %s\n", argv[optind + 2 + i], strerror(errno));
						exit(1);
					}
				} else {
					printf("couldn't fork: %s\n", strerror(errno));
					sleep(1);
					i--;
				}
			} else {
				pid = wait(&status);
				if(pid > 0)
					nprocs--;
				i--;
			}
		}

		while(1) {
			pid = wait(&status);
			if(pid < 0 && errno == ECHILD)
				break;
		}

		return 0;
	}
	//open output file to create cluster file
	char outRe[80];
	sprintf(outRe, "%s/out.txt", home_dir);
	char clusterFile[80];
	sprintf(clusterFile, "%s/cluster.txt", home_dir);


	FILE *out = fopen(clusterFile, "w");
	float d;
	char ar1[100];
	char ar2[100];
	int c_count = 0;
	struct m_server_info temp_sever;
	struct m_server_info *m_servers = malloc(sizeof(struct m_server_info) * 400);
	bw_matrix_init(400);
	FILE *fp = fopen(outRe, "r");
	//process output file
	if(fp != NULL)
		while(fscanf(fp, "%s %s %f", ar1, ar2, &d) != EOF) {
			int s = 0, t = 0;
			int test = 0;
			int length;
			for(i = 0; i < 100; i++) {
				if(ar1[i] == '\0')
					break;
			}
			temp_sever.name = malloc(sizeof(char) * (i + 1));
			for(j = 0; j <= i; j++) {
				temp_sever.name[j] = ar1[j];
			}

			for(j = 0; j < c_count; j++) {
				if(strlen(m_servers[j].name) > strlen(temp_sever.name))
					length = strlen(temp_sever.name);
				else
					length = strlen(m_servers[j].name);

				if(!strncmp(temp_sever.name, m_servers[j].name, length)) {
					test = 1;
					s = j;
					break;
				}
			}
			if(test == 0) {
				m_servers[c_count].name = malloc(sizeof(char) * (i + 1));
				for(j = 0; j <= i; j++) {
					m_servers[c_count].name[j] = temp_sever.name[j];
				}
				m_servers[c_count].cid = 0;
				m_servers[c_count].index = c_count;
				t = c_count;
				c_count++;
			}

			for(i = 0; i < 100; i++) {
				if(ar2[i] == '\0')
					break;
			}
			temp_sever.name = malloc(sizeof(char) * (i + 1));
			for(j = 0; j <= i; j++) {
				temp_sever.name[j] = ar2[j];
			}

			for(j = 0; j < c_count; j++) {
				if(strlen(m_servers[j].name) > strlen(temp_sever.name))
					length = strlen(temp_sever.name);
				else
					length = strlen(m_servers[j].name);

				if(!strncmp(temp_sever.name, m_servers[j].name, length)) {
					test = 1;
					t = j;
					break;
				}
			}
			if(test == 0) {
				m_servers[c_count].name = malloc(sizeof(char) * (i + 1));
				for(j = 0; j <= i; j++) {
					m_servers[c_count].name[j] = temp_sever.name[j];
				}
				m_servers[c_count].index = c_count;
				m_servers[c_count].cid = 0;
				t = c_count;
				c_count++;
			}
			//printf("%d %d\n",s,t);

			if(bw_matrix_get(s, t) < d) {
				bw_matrix_set(s, t, d);
				bw_matrix_set(t, s, d);
			}
		}

	if(fp != NULL)
		fclose(fp);
	int stack[1000];
	int bottom = -1;
	int count = 0;

	int save_i = 0;
	double max = 0;
	int test = 1;
	//create cluster file
	while(test) {
		max = 0;
		bottom = -1;
		for(i = 0; i < c_count; i++) {
			if(max < bw_find_max(i)) {
				max = bw_find_max(i);
				save_i = i;
			}
		}
		if(max == 0) {
			test = 0;
			break;
		}

		count++;
		fprintf(out, "Cluster %d bw %2.1f : ", count, max);
		fprintf(out, "%s_XXX", m_servers[save_i].name);
		for(i = 0; i < c_count; i++) {
			if(bw_matrix_get(save_i, i) > threshold * max) {
				m_servers[save_i].cid = count;
				m_servers[i].cid = count;
				bottom++;
				stack[bottom] = i;
			}
		}

		for(j = 0; j < c_count; j++) {
			bw_matrix_set(save_i, j, 0);
			bw_matrix_set(j, save_i, 0);
		}

		while(bottom >= 0) {
			i = stack[bottom];
			fprintf(out, ",%s_XXX", m_servers[i].name);
			bottom--;
			for(j = 0; j < c_count; j++) {
				if(bw_matrix_get(i, j) > threshold * max) {
					m_servers[j].cid = count;
					bottom++;
					stack[bottom] = j;
				}
			}
			for(j = 0; j < c_count; j++) {
				bw_matrix_set(i, j, 0);
				bw_matrix_set(j, i, 0);
			}
		}
		fprintf(out, "\n\n");
	}


	if(out != NULL)
		fclose(out);

	targets = malloc(sizeof(struct target_info) * ntargets);
	servers = malloc(sizeof(struct server_info) * 400);

	/***read in chirp server with cluster information***/
	char temp[4000];
	char t_name[40];
	int name_count = 0;

	test = 0;
	out = fopen(outRe, "a");

	/*
	   char outR[15];
	   sprintf(outR, "%i", start_time);
	   FILE *outResult = fopen(outR,"w");
	 */

	fp = fopen(clusterFile, "r");
	if(fp != NULL) {
		while(fgets(temp, 4000, fp) != NULL) {
			name_count = 0;

			if(strlen(temp) > 10)
				cluster_count = cluster_count + 1;
			for(i = 0; i < (int) strlen(temp); i++) {
				if((test != 0) && (temp[i] != '_') && (temp[i] != ' ')) {
					t_name[name_count] = temp[i];
					name_count++;
				}

				if(temp[i] == ':')
					test = 1;
				if(temp[i] == '_') {
					test = 0;
					t_name[name_count] = '\0';
					name_count++;
				}
				if(temp[i] == ',') {
					test = 1;
					servers[server_count].name = malloc(sizeof(char) * (name_count));

					for(j = 0; j < name_count; j++) {
						servers[server_count].name[j] = t_name[j];
					}
					servers[server_count].cid = cluster_count;
					name_count = 0;
					server_count++;
				}
			}
		}
	}
	//set inital cluster state
	int c_State[cluster_count];
	for(i = 0; i < cluster_count; i++) {
		c_State[i] = -1;	//-1: cluster does not have files, does not need files
	}


	targets[0].name = sourcehost;
	targets[0].state = TARGET_STATE_IDLE;

	test = 0;
	for(j = 0; j < server_count; j++) {
		if(strlen(servers[j].name) > strlen(targets[0].name))
			length = strlen(targets[0].name);
		else
			length = strlen(servers[j].name);

		if(!strncmp(targets[0].name, servers[j].name, length)) {
			targets[0].cid = servers[j].cid;
			c_State[targets[0].cid] = 1;
			test = 1;
			break;
		}
	}
	if(test == 0) {
		targets[0].cid = 0;	//unknow servers will be considered in cluster #0
		c_State[targets[0].cid] = 1;
	}

	for(i = 1; i < ntargets; i++) {
		if(i != 0) {
			targets[i].name = argv[optind + 1 + i];
			targets[i].state = TARGET_STATE_FRESH;
		}

		test = 0;
		for(j = 0; j < server_count; j++) {
			if(strlen(servers[j].name) > strlen(targets[i].name))
				length = strlen(targets[i].name);
			else
				length = strlen(servers[j].name);

			if(!strncmp(targets[i].name, servers[j].name, length)) {
				targets[i].cid = servers[j].cid;
				if(c_State[targets[i].cid] == -1)
					c_State[targets[i].cid] = 0;	//0 does not have files, need files
				test = 1;
				break;
			}
		}
		if(test == 0)
			targets[i].cid = 0;	//unknow servers will be considered in cluster #0
	}

	if(detail_mode) {
		printf("%u   start -> %s    0 secs, 0 MB/sec\n", (unsigned) time(0), sourcehost);
	}


	while(time(0) < overall_stoptime) {
		int source = -1;
		int target = -1;

		if(transfers_needed != 0 && transfers_complete >= transfers_needed) {
			for(i = 1; i < ntargets; i++) {
				if(targets[i].state == TARGET_STATE_SENDING) {
					kill(targets[i].pid, SIGKILL);
				}
			}
			break;
		}

		for(i = 0; i <= ntargets - 1; i++) {	//(i=(ntargets-1);i>=0;i--) {
			if(targets[i].state == TARGET_STATE_IDLE) {
				source = i;
				break;
			}
		}

		if(source != -1) {
			int freshtargets = 0;
			target = -1;
			for(i = 1; i < ntargets; i++) {
				if(targets[i].state == TARGET_STATE_FRESH) {
					freshtargets++;
					if(c_State[targets[i].cid] == 0) {	//cluster does not have files
						if(failure_matrix_get(source, i) != FAILURE_MARK_FAILED) {
							c_State[targets[i].cid] = 2;	//cluster is receving files
							target = i;

							if(!strncmp(targets[target].name, sourcehost, strlen(sourcehost)))
								printf("New cluster %s, %s, %s \n", targets[source].name, sourcehost, targets[target].name);
							break;
						}
					}
				}
			}
			if(target == -1)
				for(i = 1; i < ntargets; i++) {
					if(targets[i].state == TARGET_STATE_FRESH) {
						if(targets[i].cid == targets[source].cid) {	//target and source are in the same cluster
							if(failure_matrix_get(source, i) != FAILURE_MARK_FAILED) {
								target = i;
								break;
							}
						}
					}
				}
			if(target == -1)
				for(i = 1; i < ntargets; i++) {
					if(targets[i].state == TARGET_STATE_FRESH) {
						if(failure_matrix_get(source, i) != FAILURE_MARK_FAILED) {
							target = i;
							break;
						}
					}
				}


			if(target == -1 && freshtargets > 0) {
				/* nothing left for this one to do */
				targets[source].state = TARGET_STATE_FAILED;
				continue;
			}
		}

		if(nprocs < maxprocs && target != -1 && source != -1) {
			if(target == 0) {
				printf("%s, %s, %s %d \n", targets[source].name, sourcehost, targets[target].name, target);
				exit(0);
			}
			fflush(0);
			pid = fork();
			if(pid > 0) {
				nprocs++;
				targets[source].state = TARGET_STATE_SENDING;
				targets[source].pid = pid;
				targets[target].state = TARGET_STATE_RECEIVING;
				targets[target].pid = pid;
			} else if(pid == 0) {
				timestamp_t start, stop;
				start = timestamp_get();

				result = chirp_reli_thirdput(targets[source].name, sourcepath, targets[target].name, sourcepath, compute_stoptime());
				stop = timestamp_get();
				if(start == stop)
					stop++;
				if(result >= 0) {
					c_State[targets[target].cid] = 1;	//mark cluster to "have files"
					//targets[target].state = TARGET_STATE_IDLE;
					if(detail_mode) {
						printf("%u   %s (%d) -> %s (%d)   %.2lf secs, %.1lf MB/sec\n", (unsigned) time(0), targets[source].name, targets[source].cid, targets[target].name, targets[target].cid, (stop - start) / 1000000.0,
							   result / (double) (stop - start));
						fprintf(out, "%s %s %.1lf\n", targets[source].name, targets[target].name, result / (double) (stop - start));
						//end_time = time(0);
						//fprintf(outResult,"%d\n",end_time-start_time);

					} else {
						//end_time = time(0);
						//fprintf(outResult,"%d\n",end_time-start_time);
					}
					if(confirm_mode) {
						printf("YES %s\n", targets[target].name);
					}
					fflush(0);
					exit(0);
				} else {
					if(c_State[targets[target].cid] == 2)
						c_State[targets[target].cid] = 0;	//cluster did not get files
					int save_errno = errno;
					if(detail_mode) {
						printf("%u   %s(%d) -> %s(%d)    failed: %s\n", (unsigned) time(0), targets[source].name, targets[source].cid, targets[target].name, targets[target].cid, strerror(errno));
					}

					fflush(0);
					chirp_reli_rmall(targets[target].name, sourcepath, compute_stoptime());
					exit(save_errno);
				}
			} else {
				fprintf(stderr, "chirp_distribute: %s\n", strerror(errno));
				sleep(1);
			}
		} else {
			int status;
			pid = wait(&status);
			if(pid > 0) {
				int transfer_ok;
				int error_code;

				nprocs--;

				if(WIFEXITED(status)) {
					error_code = WEXITSTATUS(status);
					transfer_ok = (error_code == 0);
					if(transfer_ok) {
						transfers_complete++;
						//end_time = time(0);
					}
				} else {
					transfer_ok = 0;
					error_code = 0;
				}

				for(i = 0; i < ntargets; i++) {
					if(targets[i].pid == pid) {
						if(targets[i].state == TARGET_STATE_RECEIVING) {
							target = i;
							if(transfer_ok) {
								targets[i].state = TARGET_STATE_IDLE;
							} else if(error_code == ECONNRESET) {
								targets[i].state = TARGET_STATE_FAILED;
							} else {
								targets[i].state = TARGET_STATE_FRESH;
							}
						} else if(targets[i].state == TARGET_STATE_SENDING) {
							source = i;
							targets[i].state = TARGET_STATE_IDLE;
						}
					}
				}
				if(transfer_ok) {
					failure_matrix_set(source, target, FAILURE_MARK_SUCCESS);
				} else {
					failure_matrix_set(source, target, FAILURE_MARK_FAILED);
				}
			} else if(errno == ECHILD) {
				break;
			} else {
				fprintf(stderr, "chirp_distribute: wait: %s\n", strerror(errno));
				sleep(1);
			}
		}
	}
	if(out != NULL)
		fclose(out);
	if(fp != NULL)
		fclose(fp);
	/*if(outResult!=NULL)
	   fclose(outResult); */




	failure_matrix_print();
	//end_time = time(0);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
