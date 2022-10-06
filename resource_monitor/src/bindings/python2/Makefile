include ../../../../config.mk
include $(CCTOOLS_HOME)/rules.mk

# Python always uses 'so' for its modules (even on Darwin)
CCTOOLS_DYNAMIC_SUFFIX = so
# SWIG produces code that causes a lot of warnings, so use -w to turn those off.
LOCAL_CCFLAGS = -fPIC -w $(CCTOOLS_PYTHON2_CCFLAGS)
LOCAL_LINKAGE = $(CCTOOLS_PYTHON2_LDFLAGS)

EXTERNAL_DEPENDENCIES = $(CCTOOLS_HOME)/dttools/src/libdttools.a
RMPYTHONSO = _resource_monitor.$(CCTOOLS_DYNAMIC_SUFFIX)
LIBRARIES = $(RMPYTHONSO) resource_monitor.py
OBJECTS = rmonitor_wrap.o
TARGETS = $(LIBRARIES)

all: $(TARGETS)

rmonitor_wrap.c resource_monitor.py: rmonitor.i
	@echo "SWIG rmonitor.i (python)"
	@$(CCTOOLS_SWIG) -o rmonitor_wrap.c -python -I$(CCTOOLS_HOME)/dttools/src/ rmonitor.i
	@cat resource_monitor.bindings.py >> resource_monitor.py

$(RMPYTHONSO): rmonitor_wrap.o $(EXTERNAL_DEPENDENCIES)

clean:
	rm -f $(OBJECTS) $(TARGETS) rmonitor_wrap.c *.so *.pyc

test:

install: all
	mkdir -p $(CCTOOLS_PYTHON2_PATH)
	chmod 755 resource_monitor.py
	cp resource_monitor.py $(TARGETS) $(CCTOOLS_PYTHON2_PATH)/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc


