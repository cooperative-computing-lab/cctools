include ../../../../config.mk
include $(CCTOOLS_HOME)/rules.mk

# Python always uses 'so' for its modules (even on Darwin)
CCTOOLS_DYNAMIC_SUFFIX = so
# SWIG produces code that causes a lot of warnings, so use -w to turn those off.
LOCAL_CCFLAGS = -w -fPIC -DNDEBUG $(CCTOOLS_PYTHON3_CCFLAGS) -I ../../manager
LOCAL_LINKAGE = $(CCTOOLS_PYTHON3_LDFLAGS) -lz $(CCTOOLS_OPENSSL_LDFLAGS) $(CCTOOLS_HOME)/taskvine/src/manager/libtaskvine.a $(CCTOOLS_HOME)/dttools/src/libdttools.a

DSPYTHONSO = taskvine/_cvine.$(CCTOOLS_DYNAMIC_SUFFIX)
LIBRARIES = $(DSPYTHONSO) taskvine/cvine.py
OBJECTS = vine_wrap.o
TARGETS = $(LIBRARIES)

all: $(TARGETS)

vine_wrap.c taskvine/cvine.py: taskvine.i
	@echo "SWIG taskvine.i (python)"
	@$(CCTOOLS_SWIG) -o vine_wrap.c -python -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/taskvine/src/manager taskvine.i
	mv cvine.py taskvine

$(DSPYTHONSO): vine_wrap.o $(EXTERNAL_DEPENDENCIES)

test:

clean:
	rm -rf $(OBJECTS) $(TARGETS) vine_wrap.c vine_wrap.o *.pyc __pycache__

install: all
	mkdir -p $(CCTOOLS_PYTHON3_PATH)/taskvine
	cp taskvine/* $(CCTOOLS_PYTHON3_PATH)/taskvine
