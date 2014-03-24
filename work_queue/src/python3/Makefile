CCTOOLS_HOME = ../../..

include ${CCTOOLS_HOME}/Makefile.config

# Python always uses 'so' for its modules (even on Darwin)
CCTOOLS_DYNAMIC_SUFFIX=so

all: work_queue.py _work_queue.${CCTOOLS_DYNAMIC_SUFFIX} work_queue_example.py3

work_queue_wrap.c work_queue.py: work_queue.i ../python/work_queue.binding.py
	@echo "SWIG work_queue.i (python)"
	@$(CCTOOLS_SWIG) -o work_queue_wrap.c -python -py3 -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/work_queue/src work_queue.i
	@cp ../python/work_queue.binding.py .
	@${CCTOOLS_PYTHON3_2TO3} -w work_queue.binding.py
	@cat work_queue.binding.py >> work_queue.py

work_queue_wrap.o: work_queue_wrap.c
	$(CCTOOLS_CC) -c -o $@ -fPIC -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/work_queue/src $(CCTOOLS_PYTHON_CCFLAGS) $^

# The odd symlink in the following rule is necessary to overcome a problem
# in the framework search path emitted by the Python configuration on macOS.

_work_queue.$(CCTOOLS_DYNAMIC_SUFFIX): work_queue_wrap.o $(CCTOOLS_HOME)/work_queue/src/libwork_queue.a $(CCTOOLS_HOME)/dttools/src/libdttools.a
	@ln -sf /System/Library/Frameworks/Python.framework .
	$(CCTOOLS_LD) $(CCTOOLS_DYNAMIC_FLAG) -o $@ $^ $(CCTOOLS_INTERNAL_LDFLAGS) $(CCTOOLS_PYTHON_LDFLAGS)
	@rm Python.framework

work_queue_example.py3: ../python/work_queue_example.py
	@cp ../python/work_queue_example.py work_queue_example.py3
	@chmod 755 work_queue_example.py3
	@${CCTOOLS_PYTHON3_2TO3} -w work_queue_example.py3

test: all
	@echo running work queue python binding tests
	@${CCTOOLS_HOME}/dttools/src/test_runner.sh TR_*.sh

clean:
	rm -fr Python.framework _work_queue.${CCTOOLS_DYNAMIC_SUFFIX} work_queue.binding.py work_queue.binding.py.bak work_queue.py work_queue_wrap.* *.pyc work_queue_example.py3*

install: all
	mkdir -p ${CCTOOLS_INSTALL_DIR}/doc
	mkdir -p ${CCTOOLS_INSTALL_DIR}/lib/python${CCTOOLS_PYTHON3_VERSION}/site-packages
	cp _work_queue.${CCTOOLS_DYNAMIC_SUFFIX} ${CCTOOLS_INSTALL_DIR}/lib/python${CCTOOLS_PYTHON3_VERSION}/site-packages
	cp work_queue.py ${CCTOOLS_INSTALL_DIR}/lib/python${CCTOOLS_PYTHON3_VERSION}/site-packages
	cp work_queue_example.py3 ${CCTOOLS_INSTALL_DIR}/doc/work_queue_example.py3
