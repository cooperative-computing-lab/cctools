#include "lib-new-open.h"

#define LOGGING 0 

int Handler = NOPEN_EXIT;
int Stat_handler = NOPEN_LOG;

int exit_handler() {
    switch (Handler) {

        case NOPEN_ENOENT:
            errno = ENOENT;
            return -1;

        case NOPEN_EXIT:
            exit(EXIT_FAILURE);
    }
    exit(EXIT_FAILURE);
}

void read_env_vars() {
    
    char env_tmp[BUFSIZ];

    if (getenv("NOPEN_HANDLE")) {

        strcpy(env_tmp, getenv("NOPEN_HANDLE"));

        if (!strcmp("exit", env_tmp)) {
            Handler = NOPEN_EXIT;
        } else if (!strcmp("enoent", env_tmp)) {
            Handler = NOPEN_ENOENT;
        } else if (!strcmp("log", env_tmp)) {
            Handler = NOPEN_LOG;
        } else {
            Handler = NOPEN_EXIT;
        }
     } else {
        Handler = NOPEN_EXIT;
     }   

    if (getenv("NOPEN_HANDLE_STAT")) {

        strcpy(env_tmp, getenv("NOPEN_HANDLE_STAT"));

        if (!strcmp("exit", env_tmp)) {
            Stat_handler = NOPEN_EXIT;
        } else if (!strcmp("enoent", env_tmp)) {
            Stat_handler = NOPEN_ENOENT;
        } else if (!strcmp("log", env_tmp)) {
            Stat_handler = NOPEN_LOG;
        } else {
            Stat_handler = NOPEN_LOG;
        }
     } else {
        Stat_handler = NOPEN_EXIT;
     }   
}

int open(const char *pathname, int flags) {
    
    int perms = _file_permission(pathname);

    if (!perms) {
        fprintf(stderr,"File not in allow list : %s\n", pathname);
        if (Handler != NOPEN_LOG) {
            return exit_handler();
        }
    } else if ((flags & 64) && !(perms & NOPEN_N)) {
        fprintf(stderr,"Program terminated : open : File in allow list but does not have permissions to create files : %s\n", pathname);

        if (Handler != NOPEN_LOG) {
            return exit_handler();
        }
    } else if ((flags & 2) && !((perms & NOPEN_R) && (perms & NOPEN_W))) {
        fprintf(stderr,"Program terminated : open : File in allow list but does not have permission : read/write : %s\n", pathname);
         
        if (Handler != NOPEN_LOG) {
            return exit_handler();
        }
    } else if ((flags & 1) && !((perms & NOPEN_W)) && !(flags)) {
        fprintf(stderr,"Program terminated : open : File in allow list but does not have permission : write : %s\n", pathname);
         
        if (Handler != NOPEN_LOG) {
            return exit_handler();
        }
    } else if (!(flags & 1) && !(flags & 2) && !(( perms & NOPEN_R))) {
        fprintf(stderr,"Program terminated : open : File in allow list but does not have permission : read : %s\n", pathname);
         
        if (Handler != NOPEN_LOG) {
            return exit_handler();
        }
    }
    return syscall(SYS_open, pathname, flags);
}

int __xstat(int ver, const char *pathname, struct stat *statbuf) {

    int perms = _file_permission(pathname);
    
    if (!perms) {
        fprintf(stderr,"File not in allow list : %s\n", pathname);
        switch (Stat_handler) {
            case NOPEN_EXIT:
                exit(EXIT_FAILURE);
            case NOPEN_ENOENT:
                return -1;
            case NOPEN_LOG:
                break; 
        }
    }

    if (!(perms & NOPEN_S)) {
        fprintf(stderr,"Program terminated : stat : File in allow list but does not have action permissions : %s\n", pathname);
        
        switch (Stat_handler) {
            case NOPEN_EXIT:
                exit(EXIT_FAILURE);
            case NOPEN_ENOENT:
                return -1;
            case NOPEN_LOG:
                break; 
        }
    }

    return syscall(SYS_stat, pathname, statbuf);
}

int unlink(const char *pathname) {
    int perms = _file_permission(pathname);
    
    if (!perms) {
        fprintf(stderr,"File not in allow list : %s\n", pathname);
        if (Handler != NOPEN_LOG) {
            return exit_handler();
        }
    }

    if (!(perms & NOPEN_D)) {
        fprintf(stderr,"Program terminated : unlink : File in allow list but does not have action permissions : %s\n", pathname);
        if (Handler != NOPEN_LOG) {
            return exit_handler();
        }
    }

    return syscall(SYS_unlink, pathname);
}

int _file_permission(const char *pathname) {
     
    FILE* file_pointer;

    int file_descriptor;

    char rule[BUFSIZ];
    char a_path[BUFSIZ];
    char cwd[BUFSIZ];
    char rule_path[BUFSIZ] = "rules.txt";
    char pathname_tmp[BUFSIZ];

    char slash[2];
    slash[0] = '/';
    slash[1] = 0;

    getcwd(cwd, sizeof(cwd));


    // Reading Environment Variables

    if (getenv("NOPEN_RULES")) {
        strcpy(rule_path, getenv("NOPEN_RULES"));
    }

    read_env_vars();

    file_descriptor = syscall(SYS_open, rule_path, 0);
    file_pointer = fdopen(file_descriptor, "r");
    
    char rule_file[BUFSIZ];
    char rule_perm[6];
    int rule_mask = NOPEN_0;

    // Making all paths absolute
    
    strcpy(a_path,cwd);
    
    if (strspn(pathname,"./") == 2) {
        memmove(pathname_tmp, pathname+1, strlen(pathname));
        strcat(a_path,pathname_tmp);
    } else if (pathname[0] != '/' ) {
        strcat(a_path,slash);
        strcat(a_path,pathname);
    } else {
        strcpy(a_path,pathname);
    }

    while(fgets(rule,BUFSIZ,file_pointer) != NULL) {
        
        if (strcmp(rule,"\n") == 0) 
            continue;
        rule[strcspn(rule, "\n")] = '\0';

        sscanf(rule, "%s %s", rule_file, rule_perm);

        if (!strcmp(rule_file, "."))
            strcpy(rule_file,cwd);

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

            if (LOGGING) fprintf(stdout, "file found in rules: %s : %s \n - '%s'\n",rule_perm, rule_file, a_path);
            return rule_mask;
        } 

        strcpy(rule_perm,"00000");

    }

    fprintf(stdout, "Program terminated : access forbidden: '%s' not in allow list\n",a_path);
    
    return 0;
}

/* vim: set sts=4 sw=4 ts=8 expandtab ft=c: */
