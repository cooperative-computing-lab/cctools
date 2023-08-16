/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_reli.h"
#include "auth_all.h"
#include "debug.h"
#include "chirp_matrix.h"
#include "xxmalloc.h"
#include "username.h"
#include "stringtools.h"
#include "macros.h"

#include <time.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/wait.h>

#define SEPCHARS " \n"
#define SEPCHARS2 "/"

struct chirp_matrix {
	int width;
	int height;
	int element_size;
	int nhosts;
	int nfiles;
	int n_row_per_file;
	struct chirp_file **rfiles;
	struct chirp_bulkio *bulkio;
};


struct chirp_matrix *chirp_matrix_create(const char *host, const char *path, int width, int height, int element_size, int nhosts, time_t stoptime)
{
	char host_file[CHIRP_LINE_MAX];
	int result;
	unsigned int i;
	char **hosts;

	int nfiles = nhosts;
	while(1) {
		INT64_T n_row_per_file = height / nfiles;
		if(height % nfiles)
			n_row_per_file++;
		INT64_T file_size = n_row_per_file * width * element_size;
		if(file_size > GIGABYTE) {
			nfiles *= 2;
			continue;
		} else {
			break;
		}
	}

	char line[CHIRP_LINE_MAX * nfiles];

	FILE *file = NULL;
	if(getenv("CHIRP_HOSTS")) {
		sprintf(host_file, "%s", getenv("CHIRP_HOSTS"));
		file = fopen(host_file, "r");
	}

	if(!file) {
		if(getenv("HOME")) {
			sprintf(host_file, "%s/.chirp/hosts", getenv("HOME"));
			file = fopen(host_file, "r");
		}
	}

	if(!file) {
		sprintf(host_file, "./chirp_hosts");
		file = fopen(host_file, "r");
		if(!file) {
			file = fopen(host_file, "w");
			char hostname[CHIRP_LINE_MAX];
			gethostname(hostname, CHIRP_LINE_MAX - 1);	// get hostname, this may not have domain name, though!
			fprintf(file, "%s\n", hostname);
			fclose(file);
			file = fopen(host_file, "r");
		}
	}


	if(!file) {
		debug(D_NOTICE | D_CHIRP, "matrix: could not open host list in %s: %s\n", host_file, strerror(errno));
		errno = EINVAL;
		return 0;
	}

	hosts = malloc(sizeof(*hosts) * nhosts);

	for(i = 0; (int) i < nhosts; i++) {
		if(!fgets(line, sizeof(line), file)) {
			rewind(file);
			fgets(line, sizeof(line), file);
		}
		hosts[i] = strdup(line);
		int len = strlen(hosts[i]);
		hosts[i][len - 1] = '\0';
	}

	fclose(file);

	sprintf(line, "%d\n%d\n%d\n%d\n%d\n", width, height, element_size, nhosts, nfiles);

	char datapath1[CHIRP_LINE_MAX];
	char datapath2[CHIRP_LINE_MAX];
	char datapath3[CHIRP_LINE_MAX];
	char username[USERNAME_MAX];

	char cookie[16];

	username_get(username);
	string_cookie(cookie, sizeof(cookie));

	sprintf(datapath1, "/%s", username);
	sprintf(datapath2, "/%s/matrixdata", username);
	sprintf(datapath3, "/%s/matrixdata/%s", username, cookie);

	for(i = 0; (int) i < nfiles; i++) {
		const char *datahost = hosts[i % nhosts];
		result = chirp_reli_mkdir(datahost, datapath1, 0700, stoptime);
		result = chirp_reli_mkdir(datahost, datapath2, 0700, stoptime);
		result = chirp_reli_mkdir(datahost, datapath3, 0700, stoptime);

		sprintf(&line[strlen(line)], "%s %s/data.%d\n", datahost, datapath3, i);
	}

	for(i = 0; (int) i < nhosts; i++) {
		free(hosts[i]);
	}
	free(hosts);

	char metapath[CHIRP_LINE_MAX];
	strcpy(metapath, path);
	result = chirp_reli_putfile_buffer(host, path, line, 0700, strlen(line), stoptime);
	if(result < 0) {
		for(i = 1; i < strlen(metapath); i++)
			if(metapath[i] == '/') {
				metapath[i] = '\0';
				result = chirp_reli_mkdir(host, metapath, 0700, stoptime);
				if(result < 0 && errno != EEXIST) {
					debug(D_CHIRP, "matrix: could not build directory /chirp/%s/%s to create metadata file: %s\n", host, metapath, strerror(errno));
					return 0;
				}
				metapath[i] = '/';
			}
		result = chirp_reli_putfile_buffer(host, path, line, 0700, strlen(line), stoptime);
		if(result < 0) {
			debug(D_CHIRP, "matrix: could not create metadata file /chirp/%s/%s: %s\n", host, path, strerror(errno));
			return 0;
		}
	}
	debug(D_CHIRP, "matrix: created matrix %s/%s -- now opening\n", host, path);
	return chirp_matrix_open(host, path, stoptime);
}

struct chirp_matrix *chirp_matrix_open(const char *host, const char *path, time_t stoptime)
{

	int result, i;
	char *line;
	char *tmp;
	struct chirp_matrix *matrix;

	matrix = xxmalloc(sizeof(*matrix));

	result = chirp_reli_getfile_buffer(host, path, &line, stoptime);
	if(result < 0) {
		debug(D_CHIRP, "matrix: could not create metadata file /chirp/%s/%s: %s\n", host, path, strerror(errno));
		return 0;
	}
	tmp = strtok(line, SEPCHARS);
	matrix->width = atoi(tmp);
	tmp = strtok(NULL, SEPCHARS);
	matrix->height = atoi(tmp);
	tmp = strtok(NULL, SEPCHARS);
	matrix->element_size = atoi(tmp);
	tmp = strtok(NULL, SEPCHARS);
	matrix->nhosts = atoi(tmp);
	tmp = strtok(NULL, SEPCHARS);
	matrix->nfiles = atoi(tmp);

	matrix->n_row_per_file = matrix->height / matrix->nfiles;
	if(matrix->height % matrix->nfiles)
		matrix->n_row_per_file++;

	matrix->rfiles = malloc(sizeof(struct chirp_file *) * matrix->nfiles);
	matrix->bulkio = malloc(sizeof(struct chirp_bulkio) * matrix->nfiles);

	for(i = 0; i < matrix->nfiles; i++) {
		char *host = strtok(NULL, SEPCHARS);
		char *path = strtok(NULL, SEPCHARS);
		matrix->rfiles[i] = chirp_reli_open(host, path, O_RDWR | O_CREAT, 0755, stoptime);
		if(!matrix->rfiles[i]) {
			int j;
			for(j = 0; j < i; j++)
				chirp_reli_close(matrix->rfiles[i], stoptime);
			free(line);
			return 0;
		}
	}

	free(line);


	return matrix;
}


int chirp_matrix_width(struct chirp_matrix *a)
{
	return a->width;
}

int chirp_matrix_height(struct chirp_matrix *a)
{
	return a->height;
}

int chirp_matrix_element_size(struct chirp_matrix *a)
{
	return a->element_size;
}

int chirp_matrix_nhosts(struct chirp_matrix *a)
{
	return a->nhosts;
}

int chirp_matrix_nfiles(struct chirp_matrix *a)
{
	return a->nfiles;
}

int chirp_matrix_get(struct chirp_matrix *a, int i, int j, void *data, time_t stoptime)
{
	int index = i / a->n_row_per_file;
	INT64_T offset = ((i % a->n_row_per_file) * a->width + j) * a->element_size;
	return chirp_reli_pread_unbuffered(a->rfiles[index], data, a->element_size, offset, stoptime);
}

int chirp_matrix_get_row(struct chirp_matrix *a, int j, void *data, time_t stoptime)
{
	int index = j / a->n_row_per_file;
	INT64_T offset = (j % a->n_row_per_file) * a->width * a->element_size;
	return chirp_reli_pread_unbuffered(a->rfiles[index], data, a->element_size * a->width, offset, stoptime);
}

int chirp_matrix_set(struct chirp_matrix *a, int i, int j, const void *data, time_t stoptime)
{
	int index = i / a->n_row_per_file;
	INT64_T offset = ((i % a->n_row_per_file) * a->width + j) * a->element_size;
	return chirp_reli_pwrite_unbuffered(a->rfiles[index], data, sizeof(data), offset, stoptime);
}

int chirp_matrix_set_row(struct chirp_matrix *a, int j, const void *data, time_t stoptime)
{
	int index = j / a->n_row_per_file;
	INT64_T offset = (j % a->n_row_per_file) * a->width * a->element_size;
	return chirp_reli_pwrite_unbuffered(a->rfiles[index], data, a->element_size * a->width, offset, stoptime);
}

int chirp_matrix_set_range(struct chirp_matrix *a, int x, int y, int width, int height, const void *data, time_t stoptime)
{
	if(x < 0 || y < 0 || width < 1 || height < 1 || (x + width) > a->width || (y + height) > a->height) {
		errno = EINVAL;
		return -1;
	}

	int j = 0;
	const char *cdata = data;

	for(j = 0; j < height; j++) {
		int index = (y + j) / a->n_row_per_file;
		INT64_T file_offset = (x + ((y + j) % a->n_row_per_file) * a->width) * a->element_size;
		INT64_T buffer_offset = (j * width) * a->element_size;
		INT64_T length = width * a->element_size;
		INT64_T result = chirp_reli_pwrite_unbuffered(a->rfiles[index], &cdata[buffer_offset], length, file_offset, stoptime);
		if(result != length)
			return -1;
	}

	return j * width * a->element_size;
}

int chirp_matrix_get_range(struct chirp_matrix *a, int x, int y, int width, int height, void *data, time_t stoptime)
{
	if(x < 0 || y < 0 || width < 1 || height < 1 || (x + width) > a->width || (y + height) > a->height) {
		errno = EINVAL;
		return -1;
	}

	int j = 0;
	char *cdata = data;

	for(j = 0; j < height; j++) {
		int index = (y + j) / a->n_row_per_file;
		INT64_T file_offset = (x + ((y + j) % a->n_row_per_file) * a->width) * a->element_size;
		INT64_T buffer_offset = (j * width) * a->element_size;
		INT64_T length = width * a->element_size;
		INT64_T result = chirp_reli_pread_unbuffered(a->rfiles[index], &cdata[buffer_offset], length, file_offset, stoptime);
		if(result != length)
			return -1;
	}

	return j * width * a->element_size;
}

int chirp_matrix_get_col(struct chirp_matrix *a, int i, void *data, time_t stoptime)
{
	INT64_T offset = i * a->element_size;
	int length = a->element_size * a->n_row_per_file;
	int j;

	for(j = 0; j < a->nfiles; j++) {
		struct chirp_bulkio *b = &a->bulkio[j];
		b->type = CHIRP_BULKIO_SREAD;
		b->file = a->rfiles[j];
		b->buffer = (char *) data + j * length;	//Previously void arithmetic!
		b->length = a->n_row_per_file * a->element_size;
		b->stride_length = a->element_size;
		b->stride_skip = a->element_size * a->width;
		b->offset = offset;
	}

	return chirp_reli_bulkio(a->bulkio, a->nfiles, stoptime);
}


int chirp_matrix_set_col(struct chirp_matrix *a, int i, const void *data, time_t stoptime)
{
	INT64_T offset = i * a->element_size;
	int length = a->element_size * a->n_row_per_file;
	int j;

	for(j = 0; j < a->nfiles; j++) {
		struct chirp_bulkio *b = &a->bulkio[j];
		b->type = CHIRP_BULKIO_SWRITE;
		b->file = a->rfiles[j];
		b->buffer = (char *) data + j * length;
		b->length = a->n_row_per_file * a->element_size;
		b->stride_length = a->element_size;
		b->stride_skip = a->element_size * a->width;
		b->offset = offset;
	}

	return chirp_reli_bulkio(a->bulkio, a->nfiles, stoptime);
}

int chirp_matrix_setacl(const char *host, const char *path, const char *subject, const char *rights, time_t stoptime)
{

	int result, i, j;
	char *line;
	char *tmp;
	struct chirp_matrix *matrix;

	matrix = xxmalloc(sizeof(*matrix));

	result = chirp_reli_getfile_buffer(host, path, &line, stoptime);
	if(result < 0)
		return 0;
	tmp = strtok(line, SEPCHARS);
	matrix->width = atoi(tmp);
	tmp = strtok(NULL, SEPCHARS);
	matrix->height = atoi(tmp);
	tmp = strtok(NULL, SEPCHARS);
	matrix->element_size = atoi(tmp);
	tmp = strtok(NULL, SEPCHARS);
	matrix->nhosts = atoi(tmp);
	tmp = strtok(NULL, SEPCHARS);
	matrix->nfiles = atoi(tmp);

	matrix->n_row_per_file = matrix->height / matrix->nfiles;
	if(matrix->height % matrix->nfiles)
		matrix->n_row_per_file++;

	char *matpathdir = xxmalloc((strlen(path) + 1) * sizeof(char));
	strcpy(matpathdir, path);
	for(j = 1; j < (int) strlen(matpathdir); j++)
		if(matpathdir[j] == '/') {
			matpathdir[j] = '\0';
			result = chirp_reli_setacl(host, matpathdir, subject, rights, stoptime);
			if(result < 0) {
				debug(D_CHIRP, "matrix: setting acl for /chirp/%s/%s failed: %s\n", host, matpathdir, strerror(errno));
				return result;
			}
			matpathdir[j] = '/';
		}
	free(matpathdir);
	matpathdir = NULL;

	for(i = 0; i < matrix->nfiles; i++) {
		char *fhost = strtok(NULL, SEPCHARS);
		char *fpath = strtok(NULL, SEPCHARS);
		matpathdir = xxmalloc((strlen(fpath) + 1) * sizeof(char));
		strcpy(matpathdir, fpath);
		for(j = 1; j < (int) strlen(matpathdir); j++)
			if(matpathdir[j] == '/') {
				matpathdir[j] = '\0';
				result = chirp_reli_setacl(fhost, matpathdir, subject, rights, stoptime);
				if(result < 0) {
					debug(D_CHIRP, "matrix: setting acl for /chirp/%s/%s failed: %s\n", fhost, matpathdir, strerror(errno));
					return result;
				}
				matpathdir[j] = '/';
			}
		free(matpathdir);
		matpathdir = NULL;
	}

	return 0;
}

void chirp_matrix_fsync(struct chirp_matrix *a, time_t stoptime)
{
	int i;
	for(i = 0; i < a->nfiles; i++) {
		struct chirp_bulkio *b = &a->bulkio[i];
		b->type = CHIRP_BULKIO_FSYNC;
		b->file = a->rfiles[i];
	}
	chirp_reli_bulkio(a->bulkio, a->nfiles, stoptime);
}

void chirp_matrix_close(struct chirp_matrix *a, time_t stoptime)
{
	int i;
	for(i = 0; i < a->nfiles; i++)
		chirp_reli_close(a->rfiles[i], stoptime);
	free(a->bulkio);
	free(a->rfiles);
	free(a);
}

int chirp_matrix_delete(const char *host, const char *path, time_t stoptime)
{

	char *tmp;
	int nfiles;
	int i;
	int result;
	char *line;

	result = chirp_reli_getfile_buffer(host, path, &line, stoptime);
	if(result < 0)
		return -1;

	tmp = strtok(line, SEPCHARS);
	tmp = strtok(NULL, SEPCHARS);
	tmp = strtok(NULL, SEPCHARS);
	tmp = strtok(NULL, SEPCHARS);
	tmp = strtok(NULL, SEPCHARS);

	nfiles = atoi(tmp);

	for(i = 0; i < nfiles; i++) {
		char *dhost = strtok(NULL, SEPCHARS);
		char *dpath = strtok(NULL, SEPCHARS);
		char *s = strrchr(dpath, '/');
		if(s)
			*s = 0;
		chirp_reli_rmall(dhost, dpath, stoptime);
	}

	return chirp_reli_unlink(host, path, stoptime);
}

/* vim: set noexpandtab tabstop=8: */
