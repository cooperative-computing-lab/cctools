/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

/*
	Connor Keenan
	Submits all jobs in Worker Queue mode
	Reads a file listing input and output files
	Then runs the executable on then input and outputs to the specified file
	----------------
	To run workers:
	system: condor_submit_workers <hostname> <port> <number>
	Default batch_job_work_queue port: 9123
*/

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#include "chirp_reli.h"
#include "debug.h"
#include "auth_all.h"
#include "batch_job.h"

int read_Numberoflines(FILE *fin)
{
	int numlines = 0;
	char check = '\n'; // Default is new line, in case the file is empty

	if (fin)
	{
		while((check = getc(fin)) != EOF)
		{
			if (check == '\n')
			{
				// For each new line character, increment 1
				numlines++;
			}
		}
	}
	rewind(fin);
	return numlines;
}

static void show_version(const char *cmd)
{
        printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help( const char *cmd )
{
	printf("use: %s [options] <executable> <datafile>\n",cmd);
	printf("where options are:\n");
	printf(" -R         Retry failures until all jobs complete.\n");
	printf(" -T <type>  Batch queue type: unix, condor, wq, or sge\n");
	printf(" -d <flag>  Enable debugging for this subsystem.\n");
	printf(" -v         Show version string.\n");
	printf(" -h         This message.\n");
	printf("details of the work_queue submissions are stored in <datafile>.log\n");
}

int main(int argc, char *argv[])
{
	char inFile[500], outFile[500], cmdLine[1000], logfile[1024];	
	int submit_to_start, start_to_finish;
	int total_time = 0, completed = 0, total_lines = 0, resubmitted = 0, jobs = 0, failed = 0;
	int i;
	int resubmission = 0; 
	long stf_total = 0;
	struct batch_queue *q;
	struct batch_job_info batchinfo;
	batch_job_id_t jobid;
	batch_job_id_t result;
	time_t starttime;
	char c;
	int run_complete = 0;
	batch_queue_type_t queue_type = BATCH_QUEUE_TYPE_WORK_QUEUE;

	while((c=getopt(argc,argv,"rRT:d:vh"))!=(char)-1) {
		switch(c) {
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'T':
				queue_type = batch_queue_type_from_string(optarg);
				break;
			case 'v':
				show_version(argv[0]);
				exit(0);
				break;
			case 'r':
			case 'R':
				run_complete = 1;
				break;
			case 'h':
				show_help(argv[0]);
				exit(0);
				break;
		}
	}

	if(argc-optind!=2) {
		show_help(argv[0]);
		return 0;
	}

	const char *executable = argv[optind];
	const char *datafile = argv[optind+1];

	debug_config(argv[0]);

	q = batch_queue_create(queue_type);

	// Open the input/output files
	sprintf(logfile, "%s.log", datafile);
	FILE *fin = fopen(datafile, "r");
	FILE *fout = fopen(logfile, "w");
	if (fin == NULL)
	{
		printf("Error opening file\n");
		return 1;
	}
	total_lines = read_Numberoflines(fin);
	starttime = time(NULL);
	fprintf(fout, "%d Jobs Submitted\n", total_lines);

	while (1)
	{
		if (jobs < 1000 || feof(fin))
		{
			// Read file line and Submit Jobs
			fscanf(fin, "%s %s", inFile, outFile);
			if (!feof(fin))
			{
				sprintf(cmdLine, "%s %s %s", executable, inFile, outFile);
				do {
					jobid = batch_job_submit_simple(q, cmdLine, inFile, outFile);
					} while (jobid < 0);
				jobs++;
			}
		}

		if (jobs >= 1000 || feof(fin))
		{
			// Wait for Next Job to finish
			result = batch_job_wait(q, &batchinfo);
			if (result > 0)
			{
				completed++;
				submit_to_start = (int)(batchinfo.started-batchinfo.submitted);
				start_to_finish = (int)(batchinfo.finished-batchinfo.started);
				stf_total += start_to_finish;
				total_time = (int)(time(NULL) - starttime);
				printf("JobID: %d Completed\n", result);
				printf("TOTAL TIME: %d\n", total_time);
				printf("%d out of %d Jobs Finished\n\n", completed, total_lines);

				// Output File not Created Successfully:
				if (batchinfo.exit_code == 1) failed++;
				jobs--;
			// WorkQueue is empty, Stop waiting
			} else if (result == 0) break;
		}
	}
	printf("Failed: %d\n", failed);
	fprintf(fout, "First Submission: %d out of %d Failed\n\n", failed, total_lines);

	// IF run_complete == TRUE, begin RESUBMISSIONS
	if (run_complete==1)
	{
	FILE *ffail;
	FILE *ftempout;
	while (failed > 0)
	{
		// Make a failed.out file
		ffail = fopen("failed.out", "w");
		rewind(fin);
		for (i=0;i<total_lines;i++)
		{
			fscanf(fin, "%s %s", inFile, outFile);
			ftempout = fopen(outFile, "r");
			if (ftempout == NULL)
			{
				fprintf(ffail, "%s %s\n", inFile, outFile);
			} else fclose(ftempout);
		}
		fclose(ffail);
		failed = 0;
		jobs = 0;
		completed = 0;
		ffail = fopen("failed.out", "r");
		resubmitted = read_Numberoflines(ffail);
		// Go through each line resubmitting and waiting
		while (1)
		{
			if (jobs < 1000 || feof(ffail))
			{
				// Read file and Submit Jobs
				fscanf(ffail, "%s %s", inFile, outFile);
				if (!feof(ffail))
				{
					sprintf(cmdLine, "%s %s %s", executable, inFile, outFile);
					do {
						jobid = batch_job_submit_simple(q, cmdLine, inFile, outFile);
						} while (jobid < 0);
					jobs++;
				}
			}
			if (jobs >= 1000 || feof(ffail))
			{
				result = batch_job_wait(q, &batchinfo);
				if (result > 0)
				{
					completed++;
					submit_to_start = (int)(batchinfo.started-batchinfo.submitted);
					start_to_finish = (int)(batchinfo.finished-batchinfo.started);
					stf_total += start_to_finish;
					total_time = (int)(time(NULL) - starttime);
					printf("JobID: %d Resubmitted\n", result);
					printf("TOTAL TIME: %d\n", total_time);
					printf("%d out of %d Resubmitted Jobs Finished\n\n", completed, resubmitted);
					if (batchinfo.exit_code == 1) failed++;
					jobs--;
				} else if (result == 0) break;
			}
		}
		fclose(ffail);
		resubmission++;
		fprintf(fout, "Resubmission Group %d\n\t%d out of %d Failed\n\n", resubmission, failed, completed);
	}
	}


	// Print Out Final Calculated Totals:
	total_time = (int)(time(0) - starttime);
	fprintf(fout, "---------------------------------------\n");
	fprintf(fout, "Total Time = %d secs\nRaw Average Time per Job: %f secs\nStart to Finish Total CPU Time: %ld secs\nAverage Total CPU Time Per Job: %f secs", total_time, (float)((float)total_time/(float)total_lines), stf_total, (float)((float)stf_total/(float)total_lines));

	batch_queue_delete(q);

	fclose(fout);
	fclose(fin);
	return 0;
}

