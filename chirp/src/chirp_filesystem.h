/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_FILESYSTEM_H
#define CHIRP_FILESYSTEM_H

#include "link.h"

#include "chirp_types.h"

#include <sys/types.h>

typedef struct CHIRP_FILE CHIRP_FILE;

CHIRP_FILE *cfs_fopen(const char *path, const char *mode);
int cfs_fclose(CHIRP_FILE *file);
int cfs_fflush(CHIRP_FILE *file);
void cfs_fprintf(CHIRP_FILE *file, const char *format, ...);
char *cfs_fgets(char *s, int n, CHIRP_FILE *file);
size_t cfs_fwrite(const void *ptr, size_t size, size_t nitems, CHIRP_FILE *f);
size_t cfs_fread(void *ptr, size_t size, size_t nitems, CHIRP_FILE *f);
int cfs_ferror(CHIRP_FILE *file);

int cfs_create_dir(const char *path, int mode);

/* See chirp_local.h for variable names, etc. */
struct chirp_filesystem {
    INT64_T (*init)	(const char *);
    INT64_T (*destroy) (void);

    INT64_T (*open)	(const char *, INT64_T, INT64_T);
    INT64_T (*close)	(int);
    INT64_T (*pread)	(int, void *, INT64_T, INT64_T);
    INT64_T (*pwrite)	(int, const void *, INT64_T, INT64_T);
    INT64_T (*sread)	(int, void *, INT64_T, INT64_T, INT64_T, INT64_T);
    INT64_T (*swrite)	(int, const void *, INT64_T, INT64_T, INT64_T, INT64_T);
    INT64_T (*fstat)	(int, struct chirp_stat *);
    INT64_T (*fstatfs)	(int, struct chirp_statfs *);
    INT64_T (*fchown)	(int, INT64_T, INT64_T);
    INT64_T (*fchmod)	(int, INT64_T);
    INT64_T (*ftruncate)	(int, INT64_T);
    INT64_T (*fsync)	(int);

    void *  (*opendir)	(const char *);
    char *  (*readdir)	(void *);
    void    (*closedir)	(void *);

    INT64_T (*getfile)	(const char *, struct link *, time_t);
    INT64_T (*putfile)	(const char *, struct link *, INT64_T, INT64_T, time_t);

    INT64_T (*mkfifo)   (const char *);
    INT64_T (*unlink)   (const char *);
    INT64_T (*rename)   (const char *, const char *);
    INT64_T (*link)   (const char *, const char *);
    INT64_T (*symlink)   (const char *, const char *);
    INT64_T (*readlink)   (const char *, char *, INT64_T);
    INT64_T (*chdir)	(const char *);
    INT64_T (*mkdir)   (const char *, INT64_T);
    INT64_T (*rmdir)   (const char *);
    INT64_T (*stat)   (const char *, struct chirp_stat *);
    INT64_T (*lstat)   (const char *, struct chirp_stat *);
    INT64_T (*statfs)   (const char *, struct chirp_statfs *);
    INT64_T (*access)   (const char *, INT64_T);
    INT64_T (*chmod)   (const char *, INT64_T);
    INT64_T (*chown)   (const char *, INT64_T, INT64_T);
    INT64_T (*lchown)   (const char *, INT64_T, INT64_T);
    INT64_T (*truncate)   (const char *, INT64_T);
    INT64_T (*utime)   (const char *, time_t, time_t);
    INT64_T (*md5)   (const char *, unsigned char [16]);

    /* These don't exist! */
    //INT64_T (*lsalloc)   (const char *, char *, INT64_T *, INT64_T *);
    //INT64_T (*mkalloc)   (const char *, INT64_T, INT64_T);

    INT64_T (*file_size)   (const char *);
    INT64_T (*fd_size)   (int);
};

extern struct chirp_filesystem *cfs;

#endif /* CHIRP_FILESYSTEM_H */
