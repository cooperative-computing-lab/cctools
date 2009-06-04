#include <stdio.h>

int main(int argc, char** argv) {

    if(argc < 2)
    {
	fprintf(stderr,"Give two files on the command line\n");
	return 1;
    }

    FILE* f1 = fopen(argv[1],"r");
    FILE* f2 = fopen(argv[2],"r");
    if(!f1 || !f2) {
	fprintf(stderr,"Give two readable files on the command line\n");
	return 1;
    }

    int i1,i2;
    if(fscanf(f1,"%i", &i1) != 1) {
	fprintf(stderr,"Give two readable files each containing an integer on the command line\n");
	return 1;
    }

    if(fscanf(f2,"%i", &i2) != 1) {
	fprintf(stderr,"Give two readable files each containing an integer on the command line\n");
	return 1;
    }

    printf("%i",i1+i2);
    return 0;
}
