include ../../../../config.mk
include $(CCTOOLS_HOME)/rules.mk

# Python always uses 'so' for its modules (even on Darwin)
CCTOOLS_DYNAMIC_SUFFIX = so
# SWIG produces code that causes a lot of warnings, so use -w to turn those off.
LOCAL_CCFLAGS = -fPIC -w $(CCTOOLS_PYTHON3_CCFLAGS)
LOCAL_LINKAGE = $(CCTOOLS_PYTHON3_LDFLAGS)

EXTERNAL_DEPENDENCIES = $(CCTOOLS_HOME)/dttools/src/libdttools.a
RMPYTHONSO = ndcctools/_cresource_monitor.$(CCTOOLS_DYNAMIC_SUFFIX)
LIBRARIES = $(RMPYTHONSO)
OBJECTS = rmonitor_wrap.o
TARGETS = $(LIBRARIES)

all: $(TARGETS)

rmonitor_wrap.c: rmonitor.i
	@echo "SWIG rmonitor.i (python)"
	@$(CCTOOLS_SWIG) -o rmonitor_wrap.c -outdir ndcctools/ -python -I$(CCTOOLS_HOME)/dttools/src/ rmonitor.i

$(RMPYTHONSO): rmonitor_wrap.o $(EXTERNAL_DEPENDENCIES)

clean:
	rm -rf $(TARGETS) $(OBJECTS) ndcctools/cresource_monitor.py rmonitor_wrap.c *.so *.pyc __pycache__ ndcctools/__pycache__

test:

install: all
	mkdir -p $(CCTOOLS_PYTHON3_PATH)/ndcctools/
	cp ndcctools/resource_monitor.py ndcctools/cresource_monitor.py ndcctools/__init__.py $(TARGETS) $(CCTOOLS_PYTHON3_PATH)/ndcctools/

