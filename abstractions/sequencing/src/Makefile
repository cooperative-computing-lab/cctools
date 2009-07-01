CCTOOLS_HOME = ../../..

include ${CCTOOLS_HOME}/Makefile.config
include ${CCTOOLS_HOME}/Makefile.rules

TARGETS =  assembly_master make_task_data_files

LOCAL_LDFLAGS= -lchirp -ldttools ${CCTOOLS_INTERNAL_LDFLAGS}

all: ${TARGETS}

assembly_master: assembly_master.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 

make_task_data_files: make_task_data_files.o
	${CCTOOLS_LD} -g -o $@ $^ ${LOCAL_LDFLAGS} -lm 

test:

clean:
	rm -f core *~ *.o *.a ${TARGETS} 

install: all
	install -d ${CCTOOLS_INSTALL_DIR}/
	install -d ${CCTOOLS_INSTALL_DIR}/bin
	install assembly_master ${CCTOOLS_INSTALL_DIR}/bin/
	install make_task_data_files ${CCTOOLS_INSTALL_DIR}/bin/

