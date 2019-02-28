include ../../../config.mk
include ../../../rules.mk

TARGETS = chirp.R chirp_wrap.so

all: $(TARGETS)

chirp_wrap.so: chirp.R chirp_wrap.c
	@PKG_CFLAGS="-I$(realpath ../../../dttools/src) -I$(realpath ../)" PKG_LIBS="../libchirp.a" R CMD SHLIB chirp_wrap.c

chirp.R chirp_wrap.c: chirp.i
	swig -I../../../dttools/src -I../ -r chirp.i

clean:
	-rm CChirp.R chirp_wrap.so chirp_wrap.c chirp_wrap.o

install: 
	@echo Warning: r chirp module needs to be installed manually.

