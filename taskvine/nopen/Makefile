include ../../config.mk

CC=			gcc
CFLAGS=		-g -Wall -std=gnu99
LD=			gcc
LDFLAGS=	-L.
LIBS= 		-lstr
AR=			ar
ARFLAGS=	rcs

TARGETS=    lib-nopen.so

all:		$(TARGETS)

clean:
	rm -f $(TARGETS) *.o

lib-nopen.so:	lib-nopen.o
	$(LD) $(LDFLAGS) -shared -o $@ $^

lib-nopen.o: lib-nopen.c lib-nopen.h
	$(CC) $(CFLAGS) -fPIC -c -o lib-nopen.o lib-nopen.c
