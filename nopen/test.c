#include <stdio.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int main(void){
    FILE *fp;
    int fd;
    char c;

    fd = open("test.c", O_RDONLY);
    fp = fdopen(fd, "r");
    if (fp == NULL) 
    {
        printf("Cannot open file \n");
        return 1;
    }
    // Read contents from file
    c = fgetc(fp);
    while (c != EOF) {
        printf ("%c", c);
        c = fgetc(fp);
    } 
    return 0;
}