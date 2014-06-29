include ../../../config.mk
include ../../../rules.mk

PYTHON_INSTALL_DIR = $(CCTOOLS_INSTALL_DIR)/lib/python$(CCTOOLS_PYTHON3_VERSION)/site-packages

# Python always uses 'so' for its modules (even on Darwin)
CCTOOLS_DYNAMIC_SUFFIX = so
# SWIG produces code that causes a lot of warnings, so use -w to turn those off.
LOCAL_CCFLAGS = -w -fPIC $(CCTOOLS_PYTHON3_CCFLAGS)
LOCAL_LINKAGE = $(CCTOOLS_PYTHON3_LDFLAGS)

EXTERNAL_DEPENDENCIES = ../libwork_queue.a ../../../dttools/src/libdttools.a
WQPYTHONSO = _work_queue.$(CCTOOLS_DYNAMIC_SUFFIX)
LIBRARIES = $(WQPYTHONSO)
OBJECTS = work_queue_wrap.o
TARGETS = $(LIBRARIES) work_queue_example.py3

all: $(TARGETS)

# The odd symlink in the following rule is necessary to overcome a problem
# in the framework search path emitted by the Python configuration on macOS.
work_queue_wrap.c work_queue.py: work_queue.i work_queue.binding.py3
	@echo "SWIG work_queue.i (python)"
	@$(CCTOOLS_SWIG) -o work_queue_wrap.c -python -py3 -I../../../dttools/src -I../ work_queue.i
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
	mkdir -p $(PYTHON_INSTALL_DIR)
	cp work_queue.py _work_queue.$(CCTOOLS_DYNAMIC_SUFFIX) $(PYTHON_INSTALL_DIR)/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	cp work_queue_example.py3 $(CCTOOLS_INSTALL_DIR)/doc/
