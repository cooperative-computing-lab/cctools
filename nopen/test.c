#include <stdio.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

int main(void){
    FILE *fp;
    int fd;
    char c;

    fd = open("rules.txt", O_RDONLY);
    fp = fdopen(fd, "r");
    if (fp == NULL) {
        printf("Cannot open file \n");
        return 1;
    }
    // Read contents from file
    printf("\nReading file rules...\n");
    c = fgetc(fp);
    while (c != EOF) {
        printf ("%c", c);
        c = fgetc(fp);
    } 
    // stat
    struct stat sb;
    if (stat("test.c",&sb))
        return 0;
    printf("\nFile size: %ld \n", sb.st_size); 
    // Creating a file
    fd = open("hello.txt", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG);
    fp = fdopen(fd, "w+");
    fprintf(fp,"hello world");
    printf("Created file...\n");
    //delete
    unlink("hello.txt");
    printf("Deleted file...\n");
    return 0;
}
