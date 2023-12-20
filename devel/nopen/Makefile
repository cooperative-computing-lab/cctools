include ../../config.mk

TARGETS=    lib-nopen.so

all:		$(TARGETS)

clean:
	rm -f $(TARGETS) *.o

lib-nopen.so:	lib-nopen.o
	$(LD) $(LDFLAGS) -shared -o $@ $^

lib-nopen.o: lib-nopen.c lib-nopen.h
	$(CC) $(CFLAGS) -fPIC -c -o lib-nopen.o lib-nopen.c
