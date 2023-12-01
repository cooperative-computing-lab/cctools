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

    struct stat sb;

    stat("test.c",&sb);
    printf("%ld\n",sb.st_size);
    
    fd = open("hello.txt", O_RDWR);
    fp = fdopen(fd, "w+");

    fprintf(fp,"hello world");
    return 0;
}
