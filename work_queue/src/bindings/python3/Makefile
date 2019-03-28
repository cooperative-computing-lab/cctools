include ../../../../config.mk
include $(CCTOOLS_HOME)/rules.mk

# Python always uses 'so' for its modules (even on Darwin)
CCTOOLS_DYNAMIC_SUFFIX = so
# SWIG produces code that causes a lot of warnings, so use -w to turn those off.
LOCAL_CCFLAGS = -w -fPIC -DNDEBUG $(CCTOOLS_PYTHON3_CCFLAGS)
LOCAL_LINKAGE = $(CCTOOLS_PYTHON3_LDFLAGS)

EXTERNAL_DEPENDENCIES = $(CCTOOLS_HOME)/work_queue/src/libwork_queue.a $(CCTOOLS_HOME)/dttools/src/libdttools.a -lz
WQPYTHONSO = _work_queue.$(CCTOOLS_DYNAMIC_SUFFIX)
LIBRARIES = $(WQPYTHONSO)
OBJECTS = work_queue_wrap.o
TARGETS = $(LIBRARIES) work_queue_example.py3

all: $(TARGETS)

# The odd symlink in the following rule is necessary to overcome a problem
# in the framework search path emitted by the Python configuration on macOS.
work_queue_wrap.c work_queue.py: work_queue.i work_queue.binding.py3
	@echo "SWIG work_queue.i (python)"
	@$(CCTOOLS_SWIG) -o work_queue_wrap.c -python -py3 -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/work_queue/src work_queue.i
	@cat work_queue.binding.py3 >> work_queue.py
	ln -sf /System/Library/Frameworks/Python.framework .

$(WQPYTHONSO): work_queue_wrap.o $(EXTERNAL_DEPENDENCIES)

%.py3: %.py
	cp $< $@
	chmod 755 $@
	$(CCTOOLS_PYTHON3_2TO3) -w $@

work_queue.binding.py3: work_queue.binding.py
work_queue_example.py3: work_queue_example.py

test:

clean:
	rm -f $(OBJECTS) $(TARGETS) Python.framework work_queue.binding.py3* work_queue.py work_queue_wrap.c *.pyc work_queue_example.py3*

install: all
	mkdir -p $(CCTOOLS_PYTHON3_PATH)
	cp work_queue.py _work_queue.$(CCTOOLS_DYNAMIC_SUFFIX) $(CCTOOLS_PYTHON3_PATH)/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	cp work_queue_example.py3 $(CCTOOLS_INSTALL_DIR)/doc/
