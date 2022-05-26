#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>

#include <sys/select.h>

double eps = 1e-10;

double msqrt(double n, double eps)
{
	int max = 1000000000;
	double x = n;

	while(fabs(x*x - n) > eps && max > 0)
	{
		max--;
		x = 0.5 * (x + (n/x));
	}

	return x;
}

int main(int argc, char **argv)
{
	int    n = 200;
	double r = 10000;

	if( argc > 1 )
		n = atoi(argv[1]);

	while(n--)
	{
		r *= 1.2;

		if(fork())
		{
			double x = msqrt(r, eps);
			printf("child %d %lf %lf %lf\n", getpid(), r, x, x*x - r);
			wait(NULL);
			exit(0);
		}

		struct timeval ts = {.tv_sec = 0, .tv_usec = 100000};
		select(0, NULL, NULL, NULL, &ts);
	}
}

/* vim: set noexpandtab tabstop=4: */
