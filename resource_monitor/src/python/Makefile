include ../../../config.mk
include ../../../rules.mk

# Python always uses 'so' for its modules (even on Darwin)
CCTOOLS_DYNAMIC_SUFFIX = so
# SWIG produces code that causes a lot of warnings, so use -w to turn those off.
LOCAL_CCFLAGS = -fPIC -w $(CCTOOLS_PYTHON_CCFLAGS)
LOCAL_LINKAGE = $(CCTOOLS_PYTHON_LDFLAGS)

EXTERNAL_DEPENDENCIES = ../../../dttools/src/libdttools.a
RMPYTHONSO = _cResourceMonitor.$(CCTOOLS_DYNAMIC_SUFFIX)
LIBRARIES = $(RMPYTHONSO) cResourceMonitor.py ResourceMonitor.py
OBJECTS = rmonitor_wrap.o
TARGETS = $(LIBRARIES)

all: $(TARGETS)

# The odd symlink in the following rule is necessary to overcome a problem
# in the framework search path emitted by the Python configuration on macOS.
rmonitor_wrap.c cResourceMonitor.py: rmonitor.i
	@echo "SWIG rmonitor.i (python)"
	@$(CCTOOLS_SWIG) -o rmonitor_wrap.c -python -I../../../dttools/src/ rmonitor.i
	ln -sf /System/Library/Frameworks/Python.framework .

$(RMPYTHONSO): rmonitor_wrap.o $(EXTERNAL_DEPENDENCIES)

clean:
	rm -f $(OBJECTS) $(TARGETS) Python.framework rmonitor_wrap.c *.so *.pyc

test:

install: all
	mkdir -p $(CCTOOLS_PYTHON_PATH)
	chmod 755 ResourceMonitor.py
	cp $(TARGETS) $(CCTOOLS_PYTHON_PATH)/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	cp rmonitor_allocations_example.py $(CCTOOLS_INSTALL_DIR)/doc/


