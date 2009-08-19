#include <stdio.h>

long int my_pow(long int m, long int n) {
    long int i;
    long int p=1;
    
    for(i=0; i<n; i++) {
	p*=m;
    }
    
    return p;
}

int main() {

    long int m;
    long int n;

    if(scanf("%i",&m) != 1)
    {
	fprintf(stderr,"Invalid value for m.\n");
	return 1;
    }

    if(scanf("%i",&n) != 1)
    {
	fprintf(stderr,"Invalid value for n.\n");
	return 1;
    }

    if(n>=0)
	printf("my_pow(%i,%i)=%i\n",m,n,my_pow(m,n));
    else
	printf("my_pow(%i,%i)=1/%i\n",m,n,my_pow(m,n));
    return 0;

}
