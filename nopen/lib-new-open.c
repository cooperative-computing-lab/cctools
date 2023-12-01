#include "lib-new-open.h"

#define LOGGING 0

int open(const char *pathname, int flags) {
    
    int perms = _file_permission(pathname);

    if (!perms) {
        fprintf(stderr,"File not in allow list : %s\n", pathname);
        errno = ENOENT;
        exit(EXIT_FAILURE);
        return -1;
    }

    if ((flags & 64) && !(perms & NOPEN_N)) {
        fprintf(stderr,"Program terminated : open : File in allow list but does not have permissions to create files : %s\n", pathname);
        errno = EACCES;
        exit(EXIT_FAILURE);
    }

    if ((flags & 2) && (perms & NOPEN_R) && (perms & NOPEN_W)) {
        return syscall(SYS_open, pathname, flags);
    }
    if ((flags & 1) && (perms & NOPEN_W) && !(flags)) {
        return syscall(SYS_open, pathname, flags);
    }
    if (!(flags & 1) && !(flags & 2) && ( perms & NOPEN_R)) {
        return syscall(SYS_open, pathname, flags);
    }

    fprintf(stderr,"Program terminated : open : File in allow list but does not have permission: %s\n", pathname);
     
    errno = EACCES;
    exit(EXIT_FAILURE);
    return -1;
}

int __xstat(int ver, const char *pathname, struct stat *statbuf) {

    int perms = _file_permission(pathname);
    
    if (!perms) {
        fprintf(stderr,"File not in allow list : %s\n", pathname);
        errno = ENOENT;
        exit(EXIT_FAILURE);
        return -1;
    }

    if (perms & NOPEN_S) {
        return syscall(SYS_stat, pathname, statbuf);
    }

    fprintf(stderr,"Program terminated : stat : File in allow list but does not have action permissions : %s\n", pathname);
    errno = EACCES;
    exit(EXIT_FAILURE);
}

int unlink(const char *pathname) {
    int perms = _file_permission(pathname);
    
    if (!perms) {
        fprintf(stderr,"File not in allow list : %s\n", pathname);
        errno = ENOENT;
        exit(EXIT_FAILURE);
        return -1;
    }

    if (perms & NOPEN_D) {
        return syscall(SYS_unlink, pathname);
    }

    fprintf(stderr,"Program terminated : unlink : File in allow list but does not have action permissions : %s\n", pathname);
    errno = EACCES;
    exit(EXIT_FAILURE);
}

int _file_permission(const char *pathname) {
     
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

    if (getenv("NOPEN_RULES")) strcpy(rule_path, getenv("NOPEN_RULES"));

    file_descriptor = syscall(SYS_open, rule_path, 0);
    file_pointer = fdopen(file_descriptor, "r");
    
    char rule_file[BUFSIZ];
    char rule_perm[6];
    int rule_mask = NOPEN_0;

    while(fgets(rule,BUFSIZ,file_pointer) != NULL) {
        
        if (strcmp(rule,"\n") == 0) 
            continue;
        rule[strcspn(rule, "\n")] = '\0';

        sscanf(rule, "%s %s", rule_file, rule_perm);

        if (!strcmp(rule_file, "."))
            strcpy(rule_file,a_path);

        if (strstr(a_path,rule_file)) {
            if (strchr(rule_perm, 'R'))
                rule_mask = rule_mask | NOPEN_R;
            if (strchr(rule_perm, 'W'))
                rule_mask = rule_mask | NOPEN_W;
            if (strchr(rule_perm, 'D'))
                rule_mask = rule_mask | NOPEN_D;
            if (strchr(rule_perm, 'S'))
                rule_mask = rule_mask | NOPEN_S;
            if (strchr(rule_perm, 'N'))
                rule_mask = rule_mask | NOPEN_N;

            if (LOGGING) fprintf(stdout, "file found in rules: %s    '%s'\n",rule_perm, a_path);
            return rule_mask;
        } 

        strcpy(rule_perm,"00000");

    }

    fprintf(stdout, "Program terminated : access forbidden: '%s' not in allow list\n",a_path);

    exit(EXIT_FAILURE);
    return 0;
}

/* vim: set sts=4 sw=4 ts=8 expandtab ft=c: */
