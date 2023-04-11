include ../../../../config.mk
include $(CCTOOLS_HOME)/rules.mk

# Python always uses 'so' for its modules (even on Darwin)
CCTOOLS_DYNAMIC_SUFFIX = so
# SWIG produces code that causes a lot of warnings, so use -w to turn those off.
LOCAL_CCFLAGS = -w -fPIC -DNDEBUG $(CCTOOLS_PYTHON3_CCFLAGS)
LOCAL_LINKAGE = $(CCTOOLS_PYTHON3_LDFLAGS) -lz $(CCTOOLS_OPENSSL_LDFLAGS)

EXTERNAL_DEPENDENCIES = $(CCTOOLS_HOME)/work_queue/src/libwork_queue.a $(CCTOOLS_HOME)/dttools/src/libdttools.a
WQPYTHONSO = _work_queue.$(CCTOOLS_DYNAMIC_SUFFIX)
LIBRARIES = $(WQPYTHONSO) work_queue.py
OBJECTS = work_queue_wrap.o
TARGETS = $(LIBRARIES)

all: $(TARGETS)

work_queue_wrap.c work_queue.py: work_queue.i work_queue.binding.py StatusDisplay.py
	@echo "SWIG work_queue.i (python)"
	@$(CCTOOLS_SWIG) -o work_queue_wrap.c -python -py3 -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/work_queue/src work_queue.i
	@cat -u work_queue.binding.py >> work_queue.py
	@cat -u StatusDisplay.py >> work_queue.py

$(WQPYTHONSO): work_queue_wrap.o $(EXTERNAL_DEPENDENCIES)

test:

clean:
	rm -rf $(OBJECTS) $(TARGETS) work_queue.py work_queue_wrap.c *.pyc __pycache__

install: all
	mkdir -p $(CCTOOLS_PYTHON3_PATH)
	chmod 755 work_queue.py work_queue_example.py
	cp work_queue.py _work_queue.$(CCTOOLS_DYNAMIC_SUFFIX) $(CCTOOLS_PYTHON3_PATH)/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/cctools/doc/examples/work_queue/python
	cp work_queue_example.py $(CCTOOLS_INSTALL_DIR)/cctools/doc/examples/work_queue/python

