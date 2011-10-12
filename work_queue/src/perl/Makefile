CCTOOLS_HOME = ../../..

include ${CCTOOLS_HOME}/Makefile.config

ccflags=${CCTOOLS_PERL_CCFLAGS} -I${CCTOOLS_HOME}/dttools/src
ldflags=${CCTOOLS_PERL_LDFLAGS} ${CCTOOLS_HOME}/dttools/src/libdttools.a

all: work_queue.pm work_queue.so

work_queue_wrap.c work_queue.pm: ../work_queue.i
	@echo "SWIG work_queue.i (perl5)"
	@swig -perl5 -I${CCTOOLS_HOME}/dttools/src -o work_queue_wrap.c -outdir . ../work_queue.i

work_queue_wrap.o: work_queue_wrap.c
	${CCTOOLS_CC} -fPIC -Wno-unused-value -Wno-unused-variable -c $< -o $@ ${ccflags}

work_queue.so: work_queue_wrap.o
	${CCTOOLS_LD} -shared $< -o $@ ${ldflags}

test: all

clean:
	rm -fr work_queue.pm work_queue.so work_queue_wrap.*

install: all
