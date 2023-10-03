CCTOOLS_HOME = ../../../

include ${CCTOOLS_HOME}/config.mk
include ${CCTOOLS_HOME}/rules.mk

PROGRAMS = wq_sort make_sample_data sample.txt

LOCAL_LDFLAGS= -lwork_queue -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${PROGRAMS}

wq_sort: wq_sort.o
	${CCTOOLS_LD} wq_sort.o ${LOCAL_LDFLAGS} -o $@

sample.txt: make_sample_data
	./make_sample_data > sample.txt

make_sample_data: make_sample_data.o
	${CCTOOLS_LD} make_sample_data.o -o $@

clean:
	rm -f *~ *.o ${PROGRAMS}

test: all

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/bin
	cp ${PROGRAMS} ${CCTOOLS_INSTALL_DIR}/bin
