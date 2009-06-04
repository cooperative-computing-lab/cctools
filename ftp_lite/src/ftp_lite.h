/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef FTP_LITE_H
#define FTP_LITE_H

#include <stdio.h>

typedef long long ftp_lite_off_t;
typedef long long ftp_lite_size_t;

#define FTP_LITE_LINE_MAX 32768
#define FTP_LITE_DEFAULT_PORT 21
#define FTP_LITE_GSS_DEFAULT_PORT 2811
#define FTP_LITE_WHOLE_FILE ((ftp_lite_size_t)-1)

struct ftp_lite_server * ftp_lite_open_and_auth( const char *host, int port );

struct ftp_lite_server * ftp_lite_open( const char *host, int port );
void ftp_lite_close( struct ftp_lite_server *server );

int ftp_lite_auth_anonymous( struct ftp_lite_server *s );
int ftp_lite_auth_userpass( struct ftp_lite_server *s, const char *user, const char *password );
int ftp_lite_auth_globus( struct ftp_lite_server *s );

FILE * ftp_lite_get( struct ftp_lite_server *s, const char *path, ftp_lite_off_t offset );
FILE * ftp_lite_put( struct ftp_lite_server *s, const char *path, ftp_lite_off_t offset, ftp_lite_size_t size );
FILE * ftp_lite_list( struct ftp_lite_server *s, const char *path );

int ftp_lite_done( struct ftp_lite_server *s );

int ftp_lite_rename( struct ftp_lite_server *s, const char *oldname, const char *newname );
int ftp_lite_delete( struct ftp_lite_server *s, const char *path );
ftp_lite_size_t ftp_lite_size( struct ftp_lite_server *s, const char *path );

int ftp_lite_change_dir( struct ftp_lite_server *s, const char *dir );
int ftp_lite_make_dir( struct ftp_lite_server *s, const char *dir );
int ftp_lite_delete_dir( struct ftp_lite_server *s, const char *dir );
int ftp_lite_current_dir( struct ftp_lite_server *s, char *dir );

int ftp_lite_nop( struct ftp_lite_server *s );

int ftp_lite_third_party_transfer( struct ftp_lite_server *source, const char *source_file, struct ftp_lite_server *target, const char *target_file );

ftp_lite_size_t ftp_lite_stream_to_stream( FILE *input, FILE *output );
ftp_lite_size_t ftp_lite_stream_to_buffer( FILE *input, char **buffer );

int ftp_lite_login( const char *prompt, char *name, int namelen, char *pass, int passlen );

#endif

