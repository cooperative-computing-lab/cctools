/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/
#ifndef CHIRP_ACL_H
#define CHIRP_ACL_H

#include <stdio.h>

#define CHIRP_ACL_BASE_NAME ".__acl"
#define CHIRP_ACL_BASE_LENGTH (strlen(CHIRP_ACL_BASE_NAME))

#define CHIRP_ACL_READ           (1<<0)
#define CHIRP_ACL_WRITE          (1<<1)
#define CHIRP_ACL_LIST           (1<<2)
#define CHIRP_ACL_DELETE         (1<<3)
#define CHIRP_ACL_ADMIN          (1<<4)
#define CHIRP_ACL_EXECUTE        (1<<5)
#define CHIRP_ACL_PUT            (1<<6)
#define CHIRP_ACL_RESERVE_READ   (1<<7)
#define CHIRP_ACL_RESERVE_WRITE  (1<<8)
#define CHIRP_ACL_RESERVE_LIST   (1<<9)
#define CHIRP_ACL_RESERVE_DELETE (1<<10)
#define CHIRP_ACL_RESERVE_PUT    (1<<11)
#define CHIRP_ACL_RESERVE_ADMIN  (1<<12)
#define CHIRP_ACL_RESERVE_RESERVE (1<<13)
#define CHIRP_ACL_RESERVE_EXECUTE (1<<14)
#define CHIRP_ACL_RESERVE         (1<<15)
#define CHIRP_ACL_ALL             (~0)

int          chirp_acl_check( const char *filename, const char *subject, int flags );
int          chirp_acl_check_dir( const char *dirname, const char *subject, int flags );
int          chirp_acl_check_link( const char *linkname, const char *subject, int flags );

int          chirp_acl_set( const char *filename, const char *subject, int flags, int reset_acl );

FILE *       chirp_acl_open( const char *filename );
int          chirp_acl_read( FILE *aclfile, char *subject, int *flags );
void         chirp_acl_close( FILE *aclfile);

const char * chirp_acl_flags_to_text( int flags );
int          chirp_acl_text_to_flags( const char *text );
int          chirp_acl_from_access_flags( int flags );
int          chirp_acl_from_open_flags( int flags );

void         chirp_acl_force_readonly();
void         chirp_acl_timeout_set( int t );
int          chirp_acl_timeout_get();
void	     chirp_acl_default( const char *aclpath );

int          chirp_acl_init_root( const char *path );
int	     chirp_acl_init_copy( const char *path );
int	     chirp_acl_init_reserve( const char *path, const char *subject );

#endif
