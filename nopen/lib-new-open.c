#define _GNU_SOURCE
#include <unistd.h>
#include <sys/syscall.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>

#define LOGGING 0


int file_permission(const char *pathname);

int open(const char *pathname, int flags) {
    int file_descriptor;
    
    if (file_permission(pathname)) {
        errno = ENOENT;
        return -1;
    }
    file_descriptor = syscall(SYS_open, pathname, flags);
    return file_descriptor;
     
}

int file_permission(const char *pathname) {
    char log_path[BUFSIZ] = "./log.txt";
    int fd_log = syscall(SYS_open, log_path, 1025);
    FILE* fp_log = fdopen(fd_log, "a");
     
    FILE* file_pointer;
    int file_descriptor;
    char rule[BUFSIZ];
    char a_path[BUFSIZ];
    char rule_path[BUFSIZ] = "rules.txt";
    char pathname_tmp[BUFSIZ];

    char slash[2];
    slash[0] = '/';
    slash[1] = 0;

    getcwd(a_path, sizeof(a_path));

    if (strspn(pathname,"./") == 2) {
        memmove(pathname_tmp, pathname+1, strlen(pathname));
        strcat(a_path,pathname_tmp);
    } else if (pathname[0] != '/' ) {
        strcat(a_path,slash);
        strcat(a_path,pathname);
    } else {
        strcpy(a_path,pathname);
    }

    if (getenv("OPEN_RULES")) strcpy(rule_path, getenv("OPEN_RULES"));

    file_descriptor = syscall(SYS_open, rule_path, 0);
    file_pointer = fdopen(file_descriptor, "r");
    
    if (LOGGING) {
        if (file_pointer == NULL) {
            fprintf(fp_log,"ENOENT\n");
            return 0;
        } else {
            fprintf(fp_log,"fp %d\n",file_descriptor);
        }  
    }

    while(fgets(rule,BUFSIZ,file_pointer) != NULL) {
        
        if (strcmp(rule,"\n") == 0) continue;
        rule[strcspn(rule, "\n")] = '\0';

        if (strstr(a_path,rule)) {
            if (LOGGING) fprintf(fp_log, "access granted:   '%s'\n",a_path);
            return 0;
        } 

    }

    fprintf(stderr, "access forbidden: '%s' not in allow list\n",a_path);
    if (LOGGING) fprintf(fp_log, "access forbidden: '%s' not in allow list\n",a_path);
    return 1;
}

/* vim: set sts=4 sw=4 ts=8 expandtab ft=c: */
