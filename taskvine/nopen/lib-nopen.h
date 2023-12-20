#pragma once

#define _GNU_SOURCE
#include <unistd.h>
#include <sys/syscall.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>

struct stat {
               dev_t     st_dev;
               ino_t     st_ino;
               mode_t    st_mode;
               nlink_t   st_nlink;
               uid_t     st_uid;
               gid_t     st_gid; 
               dev_t     st_rdev;
               off_t     st_size;
               blksize_t st_blksize;
               blkcnt_t  st_blocks;

               struct timespec st_atim;
               struct timespec st_mtim;
               struct timespec st_ctim;

           #define st_atime st_atim.tv_sec
           #define st_mtime st_mtim.tv_sec
           #define st_ctime st_ctim.tv_sec
};

enum {
    NOPEN_0 = 1<<0,
    NOPEN_R = 1<<1,
    NOPEN_W = 1<<2,
    NOPEN_D = 1<<3,
    NOPEN_S = 1<<4,
    NOPEN_N = 1<<5
};

enum {
    NOPEN_EXIT,
    NOPEN_ENOENT,
    NOPEN_LOG
};

int _file_permission(const char *pathname);
int open(const char *pathname, int flags);
int stat(const char *pathname, struct stat *statbuf);
int unlink(const char *pathname);
void read_env_vars();
int exit_handler();

/* vim: set sts=4 sw=4 ts=8 expandtab ft=c: */

