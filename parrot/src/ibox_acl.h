/*
Copyright (C) 2011- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef IBOX_ACL_H
#define IBOX_ACL_H

#include <stdio.h>

#define IBOX_ACL_BASE_NAME ".__acl"
#define IBOX_ACL_BASE_LENGTH (strlen(IBOX_ACL_BASE_NAME))

#define IBOX_ACL_READ           (1<<0)
#define IBOX_ACL_WRITE          (1<<1)
#define IBOX_ACL_LIST           (1<<2)
#define IBOX_ACL_DELETE         (1<<3)
#define IBOX_ACL_ADMIN          (1<<4)
#define IBOX_ACL_EXECUTE        (1<<5)
#define IBOX_ACL_PUT            (1<<6)
#define IBOX_ACL_RESERVE_READ   (1<<7)
#define IBOX_ACL_RESERVE_WRITE  (1<<8)
#define IBOX_ACL_RESERVE_LIST   (1<<9)
#define IBOX_ACL_RESERVE_DELETE (1<<10)
#define IBOX_ACL_RESERVE_PUT    (1<<11)
#define IBOX_ACL_RESERVE_ADMIN  (1<<12)
#define IBOX_ACL_RESERVE_RESERVE (1<<13)
#define IBOX_ACL_RESERVE_EXECUTE (1<<14)
#define IBOX_ACL_RESERVE         (1<<15)
#define IBOX_ACL_ALL             (~0)

int ibox_acl_check(const char *filename, const char *subject, int flags);
int ibox_acl_check_dir(const char *dirname, const char *subject, int flags);

FILE *ibox_acl_open(const char *filename);
int ibox_acl_read(FILE * aclfile, char *subject, int *flags);
void ibox_acl_close(FILE * aclfile);

int ibox_acl_text_to_flags(const char *text);
int ibox_acl_from_access_flags(int flags);
int ibox_acl_from_open_flags(int flags);

int ibox_acl_init_copy(const char *path);
int ibox_acl_init_reserve(const char *path, const char *subject);
int ibox_acl_rmdir(const char *path);

#endif
