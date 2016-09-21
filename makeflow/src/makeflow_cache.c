/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "makeflow_cache.h"
#include "dag.h"
#include "dag_file.h"
#include "dag_node.h"
#include "sha1.h"
#include "list.h"
#include "set.h"
#include "xxmalloc.h"
#include "stringtools.h"
#include "batch_job.h"
#include "debug.h"

#include <stdio.h>
#include <stdlib.h>

void makeflow_cache_generate_id(struct dag_node *n, char *command, struct list*inputs) {
  struct dag_file *f;
  char *cache_id = NULL;
  unsigned char digest[SHA1_DIGEST_LENGTH];

  // add checksum of the node's input files together
  list_first_item(inputs);
  while((f = list_next_item(inputs))) {
    sha1_file(f->filename, digest);
    cache_id = string_combine(cache_id, sha1_string(digest));
  }

  sha1_buffer(command, strlen(command), digest);
  cache_id = string_combine(cache_id, sha1_string(digest));
  sha1_buffer(cache_id, strlen(cache_id), digest);
  n -> cache_id = xxstrdup(sha1_string(digest));

  free(cache_id);
}

void makeflow_cache_populate(struct dag *d, struct dag_node *n, struct list *outputs, struct batch_queue *queue) {
  char *filename;
  char *caching_file_path, *output_file_path, *source_makeflow_file_path, *ancestor_file_path;
  char *ancestor_cache_id_string = NULL;
  struct dag_node *ancestor;
  struct dag_file *f;
  int sucess;
  FILE *fp;

  caching_file_path = xxstrdup(d->caching_directory);
  caching_file_path = string_combine_multi(caching_file_path, n->cache_id, "/outputs", 0);
  sucess = batch_fs_mkdir(queue,caching_file_path, 0777, 1);
  if (!sucess) {
    fatal("Could not create caching directory %s\n", caching_file_path);
  }

  list_first_item(outputs);
  while((f = list_next_item(outputs))) {
    output_file_path = xxstrdup(d->caching_directory);
    filename = f->filename;
    output_file_path = string_combine_multi(output_file_path, n->cache_id, "/outputs/" , filename, 0);
    sucess = batch_fs_putfile(queue, filename, output_file_path);
    if (!sucess) {
      fatal("Could not cache output file %s\n", output_file_path);
    }
  }

  source_makeflow_file_path = xxstrdup(d->caching_directory);
  source_makeflow_file_path = string_combine_multi(source_makeflow_file_path, n->cache_id, "/source_makeflow", 0);
  sucess = batch_fs_putfile(queue, d->filename, source_makeflow_file_path);
  if (!sucess) {
    fatal("Could not cache source makeflow file %s\n", source_makeflow_file_path);
  }

  set_first_element(n->ancestors);
  while ((ancestor = set_next_element(n->ancestors))) {
      ancestor_cache_id_string = string_combine_multi(ancestor_cache_id_string, ancestor->cache_id, "\n", 0);
  }
  ancestor_file_path= xxstrdup(d->caching_directory);
  ancestor_file_path= string_combine_multi(ancestor_file_path, n->cache_id, "/ancestors", 0);

  fp = fopen(ancestor_file_path, "w");
  if (fp == NULL) {
    fatal("could not cache ancestor node cache ids");
  } else {
    fprintf(fp, "%s\n", ancestor_cache_id_string);
  }
  free(caching_file_path);
  free(output_file_path);
  free(source_makeflow_file_path);
  free(ancestor_file_path);
  free(ancestor_cache_id_string);
  fclose(fp);
}

int makeflow_cache_is_preserved(struct dag *d, struct dag_node *n, char *command, struct list *inputs, struct list *outputs, struct batch_queue *queue) {
  char *filename;
  struct dag_file *f;
  struct stat buf;
  int file_exists = -1;

  makeflow_cache_generate_id(n, command, inputs);

  list_first_item(outputs);
  while ((f=list_next_item(outputs))) {
    filename = xxstrdup(d->caching_directory);
    filename = string_combine_multi(filename, n->cache_id, "/outputs/", f-> filename, 0);
    file_exists = batch_fs_stat(queue, filename, &buf);
    if (file_exists == -1) {
      return 0;
    }
  }

  /* all output files exist, replicate preserved files and update state for node and dag_files */
  makeflow_cache_copy_preserved_files(d, n, outputs, queue);
  n->state = DAG_NODE_STATE_RUNNING;
  list_first_item(n->target_files);
  while((f = list_next_item(n->target_files))) {
    makeflow_log_file_state_change(d, f, DAG_FILE_STATE_EXISTS);
  }
  makeflow_log_state_change(d, n, DAG_NODE_STATE_COMPLETE);

  free(filename);
  return 1;
}

int makeflow_cache_copy_preserved_files(struct dag *d, struct dag_node *n, struct list *outputs, struct batch_queue *queue) {
  char * filename;
  struct dag_file *f;
  int sucess;
  char *output_file_path;

  list_first_item(outputs);
  while((f = list_next_item(outputs))) {
    output_file_path = xxstrdup(d->caching_directory);
    filename = xxstrdup("./");
    output_file_path = string_combine_multi(output_file_path, n->cache_id, "/outputs/" , f->filename, 0);
    filename = string_combine(filename, f->filename);
    sucess = batch_fs_putfile(queue, output_file_path, filename);
    if (!sucess) {
      fatal("Could not reproduce output file %s\n", output_file_path);
    }
  }
  free(output_file_path);
  return 0;
}