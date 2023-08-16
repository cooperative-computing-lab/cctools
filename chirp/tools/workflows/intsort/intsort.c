#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define SWAP(ap, bp)  (*ap = *ap ^ *bp, *bp = *ap ^ *bp, *ap = *ap ^ *bp)

//#define fprintf //

static void merge (uint64_t a[], size_t middle, size_t n)
{
	size_t i;
	size_t k = middle;

	fprintf(stderr, "merge(%p, %zu, %zu)\n", a, middle, n);

	for (i = 0; i < n; i++) {
	next:
		fprintf(stderr, "\t\ti = %zu; k = %zu\n", i, k);
		{size_t i; fprintf(stderr, "\t\t"); for (i = 0; i < n; i++) fprintf(stderr, " %" PRId64, a[i]); fprintf(stderr, "\n");}
		if (i < middle) {
			if (a[k] < a[i] && (k == middle || a[k] < a[middle])) {
		fprintf(stderr, "\t\t\tSWAP(&a[i=%zu]=%" PRId64 ", &a[k=%zu]=%" PRId64 ")\n", i, a[i], k, a[k]);
				SWAP(&a[i], &a[k]);
				//if (k < n-1)
					//k += 1;
				if (a[k] > a[k+1] && k < n-1)
					k += 1;
				/*   [50, 51, 1001, 49, 101, 102, 103, ...]
				 *    ^j            ^k
				 *   [50, 51, 1001, 1002, 101, 102, 103, ...]
				 *    ^j                  ^k
				 */
			} else if (a[middle] < a[i]) {
				size_t j = middle;
		fprintf(stderr, "\t\t\tbig swap\n");
				do {
		fprintf(stderr, "\t\t\tSWAP(&a[i=%zu]=%" PRId64 ", &a[k=%zu]=%" PRId64 ")\n", i, a[i], k, a[k]);
					SWAP(&a[i], &a[k]);
		{size_t i; fprintf(stderr, "\t\t"); for (i = 0; i < n; i++) fprintf(stderr, " %" PRId64, a[i]); fprintf(stderr, "\n");}
					//if (k < n-1)
						//k += 1;
					if (a[k] > a[k+1] && k < n-1)
						k += 1;
					assert(k <= n);
		fprintf(stderr, "\t\t\tSWAP(&a[i=%zu]=%" PRId64 ", &a[j=%zu]=%" PRId64 ")\n", i, a[i], j, a[j]);
					SWAP(&a[i], &a[j]);
		{size_t i; fprintf(stderr, "\t\t"); for (i = 0; i < n; i++) fprintf(stderr, " %" PRId64, a[i]); fprintf(stderr, "\n");}
					assert(a[i-1] <= a[i]);
					j += 1;
					assert(j <= k);
					i += 1;
				} while (i < middle && a[j] < a[i]);
				goto next;
			}
		} else if (i < k) {
			if (a[k] < a[i]) {
		fprintf(stderr, "\t\t\tSWAP(&a[i=%zu]=%" PRId64 ", &a[k=%zu]=%" PRId64 ")\n", i, a[i], k, a[k]);
				SWAP(&a[i], &a[k]);
				if (k < n-1)
					k += 1;
				assert(k <= n);
			}
		} else {
			assert(i == 0 || a[i-1] <= a[i]);
			/* break; */
		}
		assert(i == 0 || a[i-1] <= a[i]);
	}
}

static void mergesort (uint64_t a[], size_t n)
{
	{size_t i; fprintf(stderr, "\t"); for (i = 0; i < n; i++) fprintf(stderr, " %" PRId64, a[i]); fprintf(stderr, "\n");}
	if (n > 1) {
		size_t middle = n/2;

		mergesort(a, middle);
		mergesort(a+middle, n-middle);

		return merge(a, middle, n);
	}
}

static int compare (const void *a, const void *b)
{
  uint64_t ai = *((uint64_t *)a);
  uint64_t bi = *((uint64_t *)b);
  if (ai < bi) return -1;
  else if (ai > bi) return 1;
  else return 0;
}

#undef fprintf
static void test (uint64_t a[], size_t n)
{
	size_t i;
	for (i = 0; i < n; i++)
		fprintf(stderr, " %" PRId64, a[i]);
	fprintf(stderr, " -->");
	mergesort(a, n);
	for (i = 0; i < n; i++)
		fprintf(stderr, " %" PRId64, a[i]);
	fprintf(stderr, "\n");
}

static void testmergesort (void)
{
  {uint64_t a[] = {0}; test(a, sizeof(a)/sizeof(a[0]));}

  {uint64_t a[] = {0,1}; test(a, sizeof(a)/sizeof(a[0]));}
  {uint64_t a[] = {1,0}; test(a, sizeof(a)/sizeof(a[0]));}

  {uint64_t a[] = {0,1,2}; test(a, sizeof(a)/sizeof(a[0]));}
  {uint64_t a[] = {0,2,1}; test(a, sizeof(a)/sizeof(a[0]));}
  {uint64_t a[] = {1,0,2}; test(a, sizeof(a)/sizeof(a[0]));}
  {uint64_t a[] = {1,2,0}; test(a, sizeof(a)/sizeof(a[0]));}
  {uint64_t a[] = {2,1,0}; test(a, sizeof(a)/sizeof(a[0]));}
  {uint64_t a[] = {2,0,1}; test(a, sizeof(a)/sizeof(a[0]));}

  {int i,j,k,l,n=4; for (i = 0; i < n; i++) for (j = 0; j < n; j++) if (j != i) for (k = 0; k < n; k++) if (k != i && k != j) for (l = 0; l < n; l++) if (l != i && l != j && l != k) { uint64_t a[] = {i,j,k,l}; test(a, sizeof(a)/sizeof(a[0]));} }

  {int i,j,k,l,o,n=5; for (i = 0; i < n; i++) for (j = 0; j < n; j++) if (j != i) for (k = 0; k < n; k++) if (k != i && k != j) for (l = 0; l < n; l++) if (l != i && l != j && l != k) for (o = 0; o < n; o++) if (o != i && o != j && o != k && o != l) { uint64_t a[] = {i,j,k,l,o}; test(a, sizeof(a)/sizeof(a[0]));} }

  {int i,j,k,l,o,p,n=6; for (i = 0; i < n; i++) for (j = 0; j < n; j++) if (j != i) for (k = 0; k < n; k++) if (k != i && k != j) for (l = 0; l < n; l++) if (l != i && l != j && l != k) for (o = 0; o < n; o++) if (o != i && o != j && o != k && o != l) for (p = 0; p < n; p++) if (p != i && p != j && p != k && p != l && p != o) { uint64_t a[] = {i,j,k,l,o,p}; test(a, sizeof(a)/sizeof(a[0]));} }

  {int i,j,k,l,o,p,q,n=7; for (i = 0; i < n; i++) for (j = 0; j < n; j++) if (j != i) for (k = 0; k < n; k++) if (k != i && k != j) for (l = 0; l < n; l++) if (l != i && l != j && l != k) for (o = 0; o < n; o++) if (o != i && o != j && o != k && o != l) for (p = 0; p < n; p++) if (p != i && p != j && p != k && p != l && p != o) for (q = 0; q < n; q++) if (q != i && q != j && q != k && q != l && q != o && q != p) { uint64_t a[] = {i,j,k,l,o,p,q}; test(a, sizeof(a)/sizeof(a[0]));} }
}

#define CATCHUNIX(expr) \
	do {\
		rc = (expr);\
		if (rc == -1) {\
			fprintf(stderr, "[%s:%d] unix error: -1 (errno = %d) `%s'\n", __FILE__, __LINE__, errno, strerror(errno));\
			exit(EXIT_FAILURE);\
		}\
	} while (0)

int main (int argc, char *argv[])
{
  int rc;

  (void)testmergesort;

  if (strcmp(argv[1], "assert") == 0) {
	int fd;
	struct stat buf;
	size_t n;
	uint64_t i1, i2;

	CATCHUNIX(fd = open(argv[2], O_RDONLY));
	CATCHUNIX(fstat(fd, &buf));
	assert(buf.st_size % sizeof(uint64_t) == 0);
	FILE *input = fdopen(fd, "r");
	CATCHUNIX(input == NULL ? -1 : 0);
	CATCHUNIX(setvbuf(input, malloc(1LL<<20), _IOFBF, (1LL<<20)));

	i1 = 0;
	while (fread(&i2, sizeof(i2), 1, input) > 0) {
		if (i2 < i1) {
			fprintf(stdout, "for bytes %zu:%zu: not sorted!\n", n, n+1);
			exit(EXIT_FAILURE);
		}
		i1 = i2;
		n += 1;
	}
	assert(feof(input));
	fprintf(stdout, "sorted!\n");
  } else if (strcmp(argv[1], "isort") == 0) {
	int fd;
	struct stat buf;

	/* sort isort <output> <input> */

	CATCHUNIX(fd = open(argv[3], O_RDONLY));
	CATCHUNIX(fstat(fd, &buf));
	void *input = mmap(NULL, (size_t)buf.st_size, PROT_READ, MAP_SHARED, fd, 0);
	CATCHUNIX(input == MAP_FAILED ? -1 : 0);
	CATCHUNIX(close(fd));

	CATCHUNIX(fd = open(argv[2], O_RDWR|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR));
	CATCHUNIX(ftruncate(fd, buf.st_size));
	void *output = mmap(NULL, (size_t)buf.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	CATCHUNIX(output == MAP_FAILED ? -1 : 0);
	CATCHUNIX(close(fd));

	memcpy(output, input, (size_t)buf.st_size);

	qsort(output, buf.st_size/sizeof(uint64_t), sizeof(uint64_t), compare);
  } else if (strcmp(argv[1], "split") == 0) {
	int fd;
	struct stat buf;
	size_t count;

	/* sort split <input> <output1> <output2> */

	CATCHUNIX(fd = open(argv[2], O_RDONLY));
	CATCHUNIX(fstat(fd, &buf));
	assert(buf.st_size % sizeof(uint64_t) == 0);
	assert(buf.st_size/2 % sizeof(uint64_t) == 0);
	FILE *input = fdopen(fd, "r");
	CATCHUNIX(input == NULL ? -1 : 0);
	CATCHUNIX(setvbuf(input, malloc(1LL<<20), _IOFBF, (1LL<<20)));

	FILE *output1 = fopen(argv[3], "w");
	CATCHUNIX(output1 == NULL ? -1 : 0);
	CATCHUNIX(setvbuf(output1, malloc(1LL<<20), _IOFBF, (1LL<<20)));
	for (count = 0; count < buf.st_size/sizeof(uint64_t)/2; count++) {
		uint64_t i;
		size_t n;
		n = fread(&i, sizeof(i), 1, input);
		CATCHUNIX(n == 0 && (errno = ferror(input)) ? -1 : 0);
		assert(n == 1);
		n = fwrite(&i, sizeof(i), 1, output1);
		CATCHUNIX(n == 0 && (errno = ferror(output1)) ? -1 : 0);
	}
	fclose(output1);

	FILE *output2 = fopen(argv[4], "w");
	CATCHUNIX(output2 == NULL ? -1 : 0);
	CATCHUNIX(setvbuf(output2, malloc(1LL<<20), _IOFBF, (1LL<<20)));
	for (count = 0; count < buf.st_size/sizeof(uint64_t)/2; count++) {
		uint64_t i;
		size_t n;
		n = fread(&i, sizeof(i), 1, input);
		CATCHUNIX(n == 0 && (errno = ferror(input)) ? -1 : 0);
		assert(n == 1);
		n = fwrite(&i, sizeof(i), 1, output2);
		CATCHUNIX(n == 0 && (errno = ferror(output2)) ? -1 : 0);
	}
	fclose(output2);

	fclose(input);
  } else if (strcmp(argv[1], "merge") == 0) {
	int fd;
	struct stat buf1, buf2;

	/* sort merge <output> <input1> <input2> */

	CATCHUNIX(fd = open(argv[3], O_RDONLY));
	CATCHUNIX(fstat(fd, &buf1));
	assert(buf1.st_size % sizeof(uint64_t) == 0);
	FILE *input1 = fdopen(fd, "r");
	CATCHUNIX(input1 == NULL ? -1 : 0);
	CATCHUNIX(setvbuf(input1, malloc(1LL<<20), _IOFBF, (1LL<<20)));

	CATCHUNIX(fd = open(argv[4], O_RDONLY));
	CATCHUNIX(fstat(fd, &buf2));
	assert(buf2.st_size % sizeof(uint64_t) == 0);
	FILE *input2 = fdopen(fd, "r");
	CATCHUNIX(input2 == NULL ? -1 : 0);
	CATCHUNIX(setvbuf(input2, malloc(1LL<<20), _IOFBF, (1LL<<20)));

	CATCHUNIX(fd = open(argv[2], O_RDWR|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR));
	assert((buf1.st_size+buf2.st_size) % sizeof(uint64_t) == 0);
	FILE *output = fdopen(fd, "w");
	CATCHUNIX(output == NULL ? -1 : 0);
	CATCHUNIX(setvbuf(output, malloc(1LL<<20), _IOFBF, (1LL<<20)));

	{
		uint64_t i1, i2;
		size_t n;
		n = fread(&i1, sizeof(i1), 1, input1);
		CATCHUNIX(n == 0 && (errno = ferror(input1)) ? -1 : 0);
		n = fread(&i2, sizeof(i2), 1, input2);
		CATCHUNIX(n == 0 && (errno = ferror(input2)) ? -1 : 0);
		while (1) {
			if (i1 <= i2) {
				n = fwrite(&i1, sizeof(i1), 1, output);
				CATCHUNIX(n == 0 && (errno = ferror(output)) ? -1 : 0);
				n = fread(&i1, sizeof(i1), 1, input1);
				CATCHUNIX(n == 0 && (errno = ferror(input1)) ? -1 : 0);
				if (n == 0) {
					while (1) {
						n = fread(&i2, sizeof(i2), 1, input2);
						CATCHUNIX(n == 0 && (errno = ferror(input2)) ? -1 : 0);
						if (n == 1) {
							n = fwrite(&i2, sizeof(i2), 1, output);
							CATCHUNIX(n == 0 && (errno = ferror(output)) ? -1 : 0);
						} else {
							break;
						}
					}
					break;
				}
			} else {
				n = fwrite(&i2, sizeof(i2), 1, output);
				CATCHUNIX(n == 0 && (errno = ferror(output)) ? -1 : 0);
				n = fread(&i2, sizeof(i2), 1, input2);
				CATCHUNIX(n == 0 && (errno = ferror(input2)) ? -1 : 0);
				if (n == 0) {
					while (1) {
						n = fread(&i1, sizeof(i1), 1, input1);
						CATCHUNIX(n == 0 && (errno = ferror(input1)) ? -1 : 0);
						if (n == 1) {
							n = fwrite(&i1, sizeof(i1), 1, output);
							CATCHUNIX(n == 0 && (errno = ferror(output)) ? -1 : 0);
						} else {
							break;
						}
					}
					break;
				}
			}
		}
		assert(feof(input1) && feof(input2));
		fclose(input1);
		fclose(input2);
		fclose(output);
	}
  } else {
	exit(EXIT_FAILURE);
  }

  return 0;
}

/* vim: set noexpandtab tabstop=8: */
