include ../../../../config.mk
include $(CCTOOLS_HOME)/rules.mk

# Python always uses 'so' for its modules (even on Darwin)
CCTOOLS_DYNAMIC_SUFFIX = so
# SWIG produces code that causes a lot of warnings, so use -w to turn those off.
LOCAL_CCFLAGS = -w -fPIC -DNDEBUG $(CCTOOLS_PYTHON3_CCFLAGS) -I ../../manager
LOCAL_LINKAGE = $(CCTOOLS_PYTHON3_LDFLAGS) -lz $(CCTOOLS_OPENSSL_LDFLAGS) $(CCTOOLS_HOME)/dataswarm/src/manager/libdataswarm.a $(CCTOOLS_HOME)/dttools/src/libdttools.a

DSPYTHONSO = _dataswarm.$(CCTOOLS_DYNAMIC_SUFFIX)
LIBRARIES = $(DSPYTHONSO) dataswarm.py
OBJECTS = ds_wrap.o
TARGETS = $(LIBRARIES)

all: $(TARGETS)

# The odd symlink in the following rule is necessary to overcome a problem
# in the framework search path emitted by the Python configuration on macOS.
ds_wrap.c dataswarm.py: dataswarm.i dataswarm.binding.py
	@echo "SWIG dataswarm.i (python)"
	@$(CCTOOLS_SWIG) -o ds_wrap.c -python -py3 -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/dataswarm/src/manager dataswarm.i
	@cat dataswarm.binding.py >> dataswarm.py
	ln -sf /System/Library/Frameworks/Python.framework .

$(DSPYTHONSO): ds_wrap.o $(EXTERNAL_DEPENDENCIES)

test:

clean:
	rm -f $(OBJECTS) $(TARGETS) Python.framework dataswarm.py ds_wrap.c *.pyc

install: all
	mkdir -p $(CCTOOLS_PYTHON3_PATH)
	chmod 755 dataswarm.py ds_example.py
	cp dataswarm.py ds_futures.py _dataswarm.$(CCTOOLS_DYNAMIC_SUFFIX) $(CCTOOLS_PYTHON3_PATH)/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	cp ds_example.py ds_futures_example.py $(CCTOOLS_INSTALL_DIR)/doc/

