/*
Copyright (C) 2011- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_SEARCH_H
#define PFS_SEARCH_H

#include <stdlib.h>

struct parrot_search_args {
  char callsite[64];
  const char *paths;
  const char *pattern;
  char *buffer;
  size_t buffer_length;
  struct stat *stats;
  size_t stats_length;
  int flags;
};

#define PFS_SEARCH_DELIMITER   ':'
#define PFS_SEARCH_DEPTH_MAX   200

#define PFS_SEARCH_STOPATFIRST (1<<0)
#define PFS_SEARCH_RECURSIVE   (1<<1)
#define PFS_SEARCH_METADATA    (1<<2)
#define PFS_SEARCH_INCLUDEROOT (1<<3)
#define PFS_SEARCH_PERIOD      (1<<4)
#define PFS_SEARCH_R_OK        (1<<5)
#define PFS_SEARCH_W_OK        (1<<6)
#define PFS_SEARCH_X_OK        (1<<7)

#endif
