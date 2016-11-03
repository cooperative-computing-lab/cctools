/*
Copyright (C) 2016- The University of Notre Dame
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
#include "makeflow_log.h"
#include "create_dir.h"
#include "copy_stream.h"
#include "timestamp.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>

void makeflow_cache_generate_id(struct dag_node *n, char *command, struct list*inputs) {
  struct dag_file *f;
  char *cache_id = NULL;
  unsigned char digest[SHA1_DIGEST_LENGTH];

  // add checksum of the node's input files together
  list_first_item(inputs);
  while((f = list_next_item(inputs))) {
    if (f->cache_id == NULL) {
      generate_file_cache_id(f);
    }
    cache_id = string_combine(cache_id, f->cache_id);
  }

  sha1_buffer(command, strlen(command), digest);
  cache_id = string_combine(cache_id, sha1_string(digest));
  sha1_buffer(cache_id, strlen(cache_id), digest);
  n -> cache_id = xxstrdup(sha1_string(digest));

  free(cache_id);
}

void makeflow_cache_populate(struct dag *d, struct dag_node *n, struct list *outputs, struct batch_job_info *info) {
  char *caching_file_path = NULL, *output_file_path = NULL, *source_makeflow_file_path = NULL, *ancestor_file_path = NULL;
  char *ancestor_cache_id_string = NULL, *ancestor_directory_path = NULL;
  char caching_prefix[3] = "";
  char *ancestor_output_file_path = NULL;
  char *input_file = NULL;
  char *descendant_directory_path = NULL;
  struct dag_node *ancestor;
  struct dag_file *f;
  int sucess;
  FILE *fp;
  strncpy(caching_prefix, n->cache_id, 2);

  caching_file_path = xxstrdup(d->caching_directory);
  caching_file_path = string_combine_multi(caching_file_path, "/jobs/", caching_prefix, "/", n->cache_id, "/outputs", 0);
  sucess = create_dir(caching_file_path, 0777);
  if (!sucess) {
    fatal("Could not create caching directory %s\n", caching_file_path);
  }

  caching_file_path = xxstrdup(d->caching_directory);
  caching_file_path = string_combine_multi(caching_file_path, "/jobs/", caching_prefix, "/", n->cache_id, "/input_files", 0);
  sucess = create_dir(caching_file_path, 0777);
  if (!sucess) {
    fatal("Could not create input_files directory %s\n", source_makeflow_file_path);
  }
  descendant_directory_path = xxstrdup(d->caching_directory);
  descendant_directory_path = string_combine_multi(descendant_directory_path, "/jobs/", caching_prefix, "/", n->cache_id, "/descendants", 0);
  create_dir(descendant_directory_path, 0777);
  if (!sucess) {
    fatal("Could not create descendant directory %s\n", descendant_directory_path);
  }

  ancestor_directory_path = xxstrdup(d->caching_directory);
  ancestor_directory_path = string_combine_multi(ancestor_directory_path, "/jobs/", caching_prefix, "/", n->cache_id, "/ancestors", 0);
  create_dir(ancestor_directory_path, 0777);
  if (!sucess) {
    fatal("Could not create ancestor directory %s\n", ancestor_directory_path);
  }

  caching_file_path = xxstrdup(d->caching_directory);
  caching_file_path = string_combine_multi(caching_file_path, "/jobs/", caching_prefix, "/", n->cache_id, 0);
  makeflow_write_run_info(d, n, caching_file_path, info);
  list_first_item(outputs);
  while((f = list_next_item(outputs))) {
    output_file_path = xxstrdup(d->caching_directory);
    output_file_path = string_combine_multi(output_file_path, "/jobs/", caching_prefix, "/", n->cache_id, 0);
    makeflow_write_file_checksum(d, f, output_file_path);
    output_file_path = string_combine_multi(output_file_path, "/outputs/" , f->filename, 0);
    sucess = copy_file_to_file(f->filename, output_file_path);
    if (!sucess) {
      fatal("Could not cache output file %s\n", output_file_path);
    } else {
      f->cache_path = xxstrdup(output_file_path);
    }
  }
  /* only preserve Makeflow workflow instructions if node is a root node */
  if (set_size(n->ancestors) == 0) {
    source_makeflow_file_path = xxstrdup(d->caching_directory);
    source_makeflow_file_path = string_combine_multi(source_makeflow_file_path, "/jobs/", caching_prefix, "/", n->cache_id, "/source_makeflow", 0);
    sucess = copy_file_to_file(d->filename, source_makeflow_file_path);
    if (!sucess) {
      fatal("Could not cache source makeflow file %s\n", source_makeflow_file_path);
    }
  }

  set_first_element(n->ancestors);
  while ((ancestor = set_next_element(n->ancestors))) {
      write_ancestor_links(d, n, ancestor);
  }

  /* create links to input files */
  list_first_item(n->source_files);
  while ((f=list_next_item(n->source_files))) {
    ancestor = f->created_by;
    if (f->created_by == 0 && f->cache_path == NULL) {
      strncpy(caching_prefix, n->cache_id, 2);
      input_file= xxstrdup(d->caching_directory);
      input_file= string_combine_multi(input_file, "/jobs/", caching_prefix, "/", n->cache_id, "/input_files/", f->filename, 0);
      sucess = copy_file_to_file(f->filename, input_file);
      f->cache_path = xxstrdup(input_file);
      if (!sucess) {
        fatal("Could not cache input file %s\n", source_makeflow_file_path);
      }
    } else {
      if (f->cache_path != NULL) {
        ancestor_output_file_path = xxstrdup(f->cache_path);
      } else {
        strncpy(caching_prefix, ancestor->cache_id, 2);
        ancestor_output_file_path= xxstrdup(d->caching_directory);
        ancestor_output_file_path= string_combine_multi(ancestor_output_file_path, "/jobs/", caching_prefix, "/", ancestor->cache_id, 0);
        ancestor_output_file_path = string_combine_multi(ancestor_output_file_path, "/outputs/", f->filename, 0);
      }
      write_descendant_link(d, n, ancestor);
      strncpy(caching_prefix, n->cache_id, 2);
      input_file= xxstrdup(d->caching_directory);
      input_file= string_combine_multi(input_file, "/jobs/", caching_prefix, "/", n->cache_id, "/input_files/", f->filename, 0);

      sucess = symlink(ancestor_output_file_path, input_file);
    }
  }

  free(ancestor_output_file_path);
  free(input_file);
  free(caching_file_path);
  free(output_file_path);
  free(source_makeflow_file_path);
  free(ancestor_file_path);
  free(ancestor_cache_id_string);
}

int makeflow_cache_copy_preserved_files(struct dag *d, struct dag_node *n, struct list *outputs) {
  char * filename;
  struct dag_file *f;
  int sucess;
  char *output_file_path;
  char caching_prefix[3] = "";
  strncpy(caching_prefix, n->cache_id, 2);

  list_first_item(outputs);
  while((f = list_next_item(outputs))) {
    output_file_path = xxstrdup(d->caching_directory);
    filename = xxstrdup("./");
    output_file_path = string_combine_multi(output_file_path, "/jobs/", caching_prefix, "/", n->cache_id, "/outputs/" , f->filename, 0);
    filename = string_combine(filename, f->filename);
    sucess = copy_file_to_file(output_file_path, filename);
    if (!sucess) {
      fatal("Could not reproduce output file %s\n", output_file_path);
    }
  }
  free(filename);
  free(output_file_path);
  return 0;
}

int makeflow_cache_is_preserved(struct dag *d, struct dag_node *n, char *command, struct list *inputs, struct list *outputs) {
  char *filename = NULL;
  struct dag_file *f;
  struct stat buf;
  int file_exists = -1;
  char caching_prefix[3] = "";

  makeflow_cache_generate_id(n, command, inputs);
  strncpy(caching_prefix, n->cache_id, 2);

  list_first_item(outputs);
  while ((f=list_next_item(outputs))) {
    filename = xxstrdup(d->caching_directory);
    filename = string_combine_multi(filename, "/jobs/", caching_prefix, "/", n->cache_id, "/outputs/", f-> filename, 0);
    file_exists = stat(filename, &buf);
    if (file_exists == -1) {
      return 0;
    }
  }

  /* all output files exist, replicate preserved files and update state for node and dag_files */
  makeflow_cache_copy_preserved_files(d, n, outputs);
  n->state = DAG_NODE_STATE_RUNNING;
  list_first_item(n->target_files);
  while((f = list_next_item(n->target_files))) {
    makeflow_log_file_state_change(d, f, DAG_FILE_STATE_EXISTS);
  }
  makeflow_log_state_change(d, n, DAG_NODE_STATE_COMPLETE);

  free(filename);
  return 1;
}

void makeflow_write_run_info(struct dag *d, struct dag_node *n, char *cache_path, struct batch_job_info *info) {
  // write timestamp
  char *run_info_path = NULL;
  FILE *fp;
  run_info_path = string_combine_multi(run_info_path, cache_path, "/run_info", 0);
  fp = fopen(run_info_path, "w");
  if (fp == NULL) {
    fatal("could not cache ancestor node cache ids");
  } else {
    fprintf(fp, "%s\n", n->command);
    fprintf(fp, "%d\n", (int) info->submitted);
    fprintf(fp, "%d\n", (int) info->started);
    fprintf(fp, "%d\n", (int) info->finished);
    fprintf(fp, "%d\n", info->exited_normally);
    fprintf(fp, "%d\n", info->exit_code);
    fprintf(fp, "%d\n", info->exit_signal);
  }
  free(run_info_path);
}

void makeflow_write_file_checksum(struct dag *d, struct dag_file *f, char *job_cache_path) {
  char *file_cache_path;
  int success;
  char caching_prefix[5] = "";

  if (f->cache_id == NULL) {
    generate_file_cache_id(f);
  }

  strncpy(caching_prefix, f->cache_id, 4);
  file_cache_path = xxstrdup(d->caching_directory);
  file_cache_path = string_combine_multi(file_cache_path, "/files/", caching_prefix, 0);
  success = create_dir(file_cache_path, 0777);
  if (!success) {
    fatal("Could not create file caching directory %s\n", file_cache_path);
  }

  file_cache_path = string_combine_multi(file_cache_path, "/", f->cache_id, 0);
  success = symlink(job_cache_path, file_cache_path);

  free(file_cache_path);
}

void generate_file_cache_id(struct dag_file *f) {
  unsigned char digest[SHA1_DIGEST_LENGTH];
  sha1_file(f->filename, digest);
  f->cache_id = xxstrdup(sha1_string(digest));
}

void write_descendant_link(struct dag *d, struct dag_node *current_node, struct dag_node *ancestor_node) {
  char *descendant_job_path = xxstrdup(d->caching_directory), *ancestor_link_path = xxstrdup(d->caching_directory);
  char current_node_caching_prefix[5] = "";
  char ancestor_node_caching_prefix[5] = "";

  strncpy(current_node_caching_prefix, current_node->cache_id, 2);
  strncpy(ancestor_node_caching_prefix, ancestor_node->cache_id, 2);

  descendant_job_path = string_combine_multi(descendant_job_path, "/jobs/", current_node_caching_prefix, "/", current_node->cache_id, 0);
  ancestor_link_path = string_combine_multi(ancestor_link_path, "/jobs/", ancestor_node_caching_prefix, "/", ancestor_node->cache_id, "/descendants/", current_node->cache_id, 0);

  symlink(descendant_job_path, ancestor_link_path);
}

void write_ancestor_links(struct dag *d, struct dag_node *current_node, struct dag_node *ancestor_node) {
  char *ancestor_job_path = xxstrdup(d->caching_directory), *current_node_descendant_path = xxstrdup(d->caching_directory);
  char current_node_caching_prefix[5] = "";
  char ancestor_node_caching_prefix[5] = "";

  strncpy(current_node_caching_prefix, current_node->cache_id, 2);
  strncpy(ancestor_node_caching_prefix, ancestor_node->cache_id, 2);

  ancestor_job_path = string_combine_multi(ancestor_job_path, "/jobs/", ancestor_node_caching_prefix, "/", ancestor_node->cache_id, 0);
  current_node_descendant_path = string_combine_multi(current_node_descendant_path, "/jobs/", current_node_caching_prefix, "/", current_node->cache_id, "/ancestors/", ancestor_node->cache_id, 0);

  symlink(ancestor_job_path, current_node_descendant_path);

}

