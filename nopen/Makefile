include ../config.mk

CC=			gcc
CFLAGS=		-g -Wall -std=gnu99
LD=			gcc
LDFLAGS=	-L.
LIBS= 		-lstr
AR=			ar
ARFLAGS=	rcs

TARGETS=    lib-new-open.so\
			program

all:		$(TARGETS)

clean:
	rm -f $(TARGETS) *.o

lib-new-open.so:	lib-new-open.o
	$(LD) $(LDFLAGS) -shared -o $@ $^

lib-new-open.o: lib-new-open.c lib-new-open.h
	$(CC) $(CFLAGS) -fPIC -c -o lib-new-open.o lib-new-open.c

program: test.o
	$(LD) $(LDFLAGS) -o $@ $< 

main.o: test.c lib-new-open.h
	$(CC) $(CFLAGS) -c -o test.o test.c

test: lib-new-open.so program
	LD_PRELOAD=./lib-new-open.so ./program

worker:
	vine_worker $$HOSTNAME 9123

worker-nopen:
	vine_worker --nopen $$HOSTNAME 9123

worker-debug:
	vine_worker -d all -o worker.debug $$HOSTNAME 9123
condor-submit:
	vine_submit_workers -T condor $$HOSTNAME 9123 5


