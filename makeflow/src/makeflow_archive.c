/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "makeflow_archive.h"
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

void makeflow_archive_node_generate_id(struct dag_node *n, char *command, struct list*inputs) {
  if (n->archive_id != NULL) {
    /* node archive id already exists */
    return;
  }
  struct dag_file *f;
  char *archive_id = NULL;
  unsigned char digest[SHA1_DIGEST_LENGTH];

  /* add checksum of the node's input files together */
  list_first_item(inputs);
  while((f = list_next_item(inputs))) {
    if (f->archive_id == NULL) {
      generate_file_archive_id(f);
    }
    archive_id = string_combine(archive_id, f->archive_id);
  }
  sha1_buffer(command, strlen(command), digest);

  archive_id = string_combine(archive_id, sha1_string(digest));
  sha1_buffer(archive_id, strlen(archive_id), digest);

  n->archive_id = xxstrdup(sha1_string(digest));

  free(archive_id);
}

void makeflow_archive_populate(struct dag *d, struct dag_node *n, char *command, struct list *inputs, struct list *outputs, struct batch_job_info *info) {
  char *archiving_file_path = NULL, *output_file_path = NULL, *source_makeflow_file_path = NULL, *ancestor_file_path = NULL;
  char *output_directory_path = NULL, *input_directory_path = NULL;
  char *ancestor_directory_path = NULL;
  char archiving_prefix[3] = "";
  char *ancestor_output_file_path = NULL;
  char *input_file = NULL;
  char *descendant_directory_path = NULL;
  char *archive_directory_path = NULL;
  struct dag_node *ancestor;
  struct dag_file *f;
  int success, symlink_failure;

  /* in --archive-write mode, we haven't yet generated a node's archive_id, so need to generate it here */
  makeflow_archive_node_generate_id(n, command, inputs);
  strncpy(archiving_prefix, n->archive_id, 2);

  archive_directory_path = string_combine_multi(NULL, d->archive_directory, "/jobs/", archiving_prefix, "/", n->archive_id + 2, 0);

  /* We create all the sub directories upfront for convenience */
  output_directory_path = string_combine_multi(NULL, archive_directory_path, "/outputs", 0);
  success = create_dir(output_directory_path, 0777);
  if (!success) {
    fatal("Could not create archiving directory %s\n", output_directory_path);
  }

  input_directory_path = string_combine_multi(NULL, archive_directory_path, "/input_files", 0);
  success = create_dir(input_directory_path, 0777);
  if (!success) {
    fatal("Could not create input_files directory %s\n", source_makeflow_file_path);
  }

  descendant_directory_path = string_combine_multi(NULL, archive_directory_path, "/descendants", 0);
  success = create_dir(descendant_directory_path, 0777);
  if (!success) {
    fatal("Could not create descendant directory %s\n", descendant_directory_path);
  }

  ancestor_directory_path = string_combine_multi(NULL, archive_directory_path, "/ancestors", 0);
  success = create_dir(ancestor_directory_path, 0777);
  if (!success) {
    fatal("Could not create ancestor directory %s\n", ancestor_directory_path);
  }

  makeflow_write_run_info(d, n, archive_directory_path, info, command);

  list_first_item(outputs);
  while((f = list_next_item(outputs))) {
    /* Convenient to write the file to job symlink here */
    makeflow_write_file_checksum(d, f, archive_directory_path);
    output_file_path = string_combine_multi(NULL, archive_directory_path, "/outputs/" , f->filename, 0);
    success = copy_file_to_file(f->filename, output_file_path);
    if (!success) {
      fatal("Could not archive output file %s\n", output_file_path);
    } else {
      f->archive_path = xxstrdup(output_file_path);
    }
    free(output_file_path);
  }
  /* only preserve Makeflow workflow instructions if node is a root node */
  if (set_size(n->ancestors) == 0) {
    source_makeflow_file_path = string_combine_multi(NULL, archive_directory_path, "/source_makeflow", 0);
    success = copy_file_to_file(d->filename, source_makeflow_file_path);
    if (!success) {
      fatal("Could not archive source makeflow file %s\n", source_makeflow_file_path);
    }
    free(source_makeflow_file_path);
  }
  /* Here we write both the ancestor link for the current node and the descendant link for the ancestor node.
     Note that we are only writing 'upwards', that is we create the links only between the current node and ancestor
     Thus we will not mistakenly write an ancestor link for a root node or a descendant link for a leaf */
  set_first_element(n->ancestors);
  while ((ancestor = set_next_element(n->ancestors))) {
      write_ancestor_links(d, n, ancestor);
      write_descendant_link(d, n, ancestor);
  }
  /* create links to input files */
  list_first_item(n->source_files);
  while ((f=list_next_item(n->source_files))) {
    ancestor = f->created_by;
    if (f->created_by == 0 && f->archive_path == NULL) {
      /* file not created by workflow and we haven't generated the archive path yet.
         Archive the file and then store it's output path. If any other nodes use this file,
         a link will be created pointing towards the archive path set here */
      strncpy(archiving_prefix, n->archive_id, 2);
      input_file= string_combine_multi(NULL, archive_directory_path, "/input_files/", f->filename, 0);
      success = copy_file_to_file(f->filename, input_file);
      f->archive_path = xxstrdup(input_file);
      if (!success) {
        fatal("Could not archive input file %s\n", source_makeflow_file_path);
      }
    } else {
      if (f->archive_path != NULL) {
        ancestor_output_file_path = xxstrdup(f->archive_path);
      } else {
        strncpy(archiving_prefix, ancestor->archive_id, 2);
        ancestor_output_file_path= string_combine_multi(NULL, d->archive_directory, "/jobs/", archiving_prefix, "/", ancestor->archive_id + 2, 0);
        ancestor_output_file_path = string_combine_multi(ancestor_output_file_path, "/outputs/", f->filename, 0);
        f->archive_path = ancestor_output_file_path;
      }

      strncpy(archiving_prefix, n->archive_id, 2);
      input_file= string_combine_multi(NULL, archive_directory_path, "/input_files/", f->filename, 0);

      symlink_failure = symlink(ancestor_output_file_path, input_file);
      if (symlink_failure && errno != EEXIST) {
        fatal("Could not create symlink %s pointing to %s\n", input_file, ancestor_output_file_path);
      }
      free(ancestor_output_file_path);
    }
    free(input_file);
  }

  free(output_directory_path);
  free(input_directory_path);
  free(ancestor_directory_path);
  free(descendant_directory_path);
  free(archiving_file_path);
  free(ancestor_file_path);
  free(archive_directory_path);
}

int makeflow_archive_copy_preserved_files(struct dag *d, struct dag_node *n, struct list *outputs) {
  char * filename;
  struct dag_file *f;
  int success;
  char *output_file_path;
  char archiving_prefix[3] = "";

  strncpy(archiving_prefix, n->archive_id, 2);

  list_first_item(outputs);
  while((f = list_next_item(outputs))) {
    output_file_path = string_combine_multi(NULL, d->archive_directory, "/jobs/", archiving_prefix, "/", n->archive_id + 2, "/outputs/" , f->filename, 0);
    filename = string_combine_multi(NULL, "./", f->filename, 0);
    success = copy_file_to_file(output_file_path, filename);
    if (!success) {
      fatal("Could not reproduce output file %s\n", output_file_path);
    }
    free(filename);
    free(output_file_path);
  }

  return 0;
}

int makeflow_archive_is_preserved(struct dag *d, struct dag_node *n, char *command, struct list *inputs, struct list *outputs) {
  char *filename = NULL;
  struct dag_file *f;
  struct stat buf;
  int file_exists = -1;
  char archiving_prefix[3] = "";

  makeflow_archive_node_generate_id(n, command, inputs);
  strncpy(archiving_prefix, n->archive_id, 2);

  list_first_item(outputs);
  while ((f=list_next_item(outputs))) {
    filename = string_combine_multi(NULL, d->archive_directory, "/jobs/", archiving_prefix, "/", n->archive_id + 2, "/outputs/", f->filename, 0);
    file_exists = stat(filename, &buf);
    free(filename);
    if (file_exists == -1) {
      return 0;
    }
  }

  return 1;
}

void makeflow_write_run_info(struct dag *d, struct dag_node *n, char *archive_path, struct batch_job_info *info, char *command) {
  char *run_info_path = NULL;
  FILE *fp;
  run_info_path = string_combine_multi(run_info_path, archive_path, "/run_info", 0);
  fp = fopen(run_info_path, "w");
  if (fp == NULL) {
    fatal("could not archive ancestor node archive ids");
  } else {
    fprintf(fp, "%s\n", n->command); // unwrapped command
    fprintf(fp, "%s\n", command); // wrapped command
    fprintf(fp, "%d\n", (int) info->submitted);
    fprintf(fp, "%d\n", (int) info->started);
    fprintf(fp, "%d\n", (int) info->finished);
    fprintf(fp, "%d\n", info->exited_normally);
    fprintf(fp, "%d\n", info->exit_code);
    fprintf(fp, "%d\n", info->exit_signal);
  }
  free(run_info_path);
  fclose(fp);
}

void makeflow_write_file_checksum(struct dag *d, struct dag_file *f, char *job_archive_path) {
  char *file_archive_path;
  int success, symlink_failure;
  char archiving_prefix[3] = "";

  if (f->archive_id == NULL) {
    generate_file_archive_id(f);
  }

  strncpy(archiving_prefix, f->archive_id, 2);
  file_archive_path = string_combine_multi(NULL, d->archive_directory, "/files/", archiving_prefix, 0);
  success = create_dir(file_archive_path, 0777);
  if (!success) {
    fatal("Could not create file archiving directory %s\n", file_archive_path);
  }

  file_archive_path = string_combine_multi(file_archive_path, "/", f->archive_id + 2, 0);
  symlink_failure = symlink(job_archive_path, file_archive_path);
  if (symlink_failure && errno != EEXIST) {
    fatal("Could not create symlink %s pointing to %s\n", file_archive_path, job_archive_path);
  }
  free(file_archive_path);
}

void generate_file_archive_id(struct dag_file *f) {
  unsigned char digest[SHA1_DIGEST_LENGTH];
  sha1_file(f->filename, digest);
  f->archive_id = xxstrdup(sha1_string(digest));
}

void write_descendant_link(struct dag *d, struct dag_node *current_node, struct dag_node *ancestor_node) {
  char *descendant_job_path = NULL, *ancestor_link_path = NULL;
  char current_node_archiving_prefix[3] = "";
  char ancestor_node_archiving_prefix[3] = "";
  int symlink_failure;

  strncpy(current_node_archiving_prefix, current_node->archive_id, 2);
  strncpy(ancestor_node_archiving_prefix, ancestor_node->archive_id, 2);

  descendant_job_path = string_combine_multi(NULL, d->archive_directory, "/jobs/", current_node_archiving_prefix, "/", current_node->archive_id + 2, 0);
  ancestor_link_path = string_combine_multi(NULL, d->archive_directory, "/jobs/", ancestor_node_archiving_prefix, "/", ancestor_node->archive_id + 2, "/descendants/", current_node->archive_id, 0);

  symlink_failure = symlink(descendant_job_path, ancestor_link_path);
  if (symlink_failure && errno != EEXIST) {
    fatal("Could not create symlink %s pointing to %s\n", ancestor_link_path, descendant_job_path);
  }
  free(descendant_job_path);
  free(ancestor_link_path);

}

void write_ancestor_links(struct dag *d, struct dag_node *current_node, struct dag_node *ancestor_node) {
  char *ancestor_job_path = NULL, *current_node_descendant_path = NULL;
  char current_node_archiving_prefix[3] = "";
  char ancestor_node_archiving_prefix[3] = "";
  int symlink_failure;

  strncpy(current_node_archiving_prefix, current_node->archive_id, 2);
  strncpy(ancestor_node_archiving_prefix, ancestor_node->archive_id, 2);

  ancestor_job_path = string_combine_multi(NULL, d->archive_directory, "/jobs/", ancestor_node_archiving_prefix, "/", ancestor_node->archive_id + 2, 0);
  current_node_descendant_path = string_combine_multi(NULL, d->archive_directory, "/jobs/", current_node_archiving_prefix, "/", current_node->archive_id + 2, "/ancestors/", ancestor_node->archive_id, 0);

  symlink_failure = symlink(ancestor_job_path, current_node_descendant_path);
  if (symlink_failure && errno != EEXIST) {
    fatal("Could not create symlink %s pointing to %s\n", current_node_descendant_path, ancestor_job_path);
  }
  free(ancestor_job_path);
  free(current_node_descendant_path);

}
