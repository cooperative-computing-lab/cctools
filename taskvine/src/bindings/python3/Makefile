include ../../../../config.mk
include ../../../../rules.mk

# Python always uses 'so' for its modules (even on Darwin)
CCTOOLS_DYNAMIC_SUFFIX = so
# SWIG produces code that causes a lot of warnings, so use -w to turn those off.
LOCAL_CCFLAGS = -w -fPIC -DNDEBUG $(CCTOOLS_PYTHON3_CCFLAGS) -I ../../manager
LOCAL_LINKAGE = $(CCTOOLS_PYTHON3_LDFLAGS) -lz $(CCTOOLS_OPENSSL_LDFLAGS) $(CCTOOLS_HOME)/taskvine/src/manager/libtaskvine.a $(CCTOOLS_HOME)/dttools/src/libdttools.a

CCTOOLS_FLAKE8_IGNORE_FILES = "cvine.py"

DSPYTHONSO = ndcctools/taskvine/_cvine.$(CCTOOLS_DYNAMIC_SUFFIX)
LIBRARIES = $(DSPYTHONSO)
OBJECTS = vine_wrap.o
TARGETS = $(LIBRARIES)

all: $(TARGETS)

vine_wrap.c: taskvine.i
	@echo "SWIG taskvine.i (python)"
	@$(CCTOOLS_SWIG) -o vine_wrap.c -outdir ndcctools/taskvine -python -I$(CCTOOLS_HOME)/dttools/src -I$(CCTOOLS_HOME)/taskvine/src/manager taskvine.i

$(DSPYTHONSO): vine_wrap.o $(EXTERNAL_DEPENDENCIES)

test:

lint:
	flake8 --ignore=$(CCTOOLS_FLAKE8_IGNORE_ERRORS) --exclude=$(CCTOOLS_FLAKE8_IGNORE_FILES) ndcctools/taskvine/

clean:
	rm -rf $(OBJECTS) $(TARGETS) ndcctools/taskvine/cvine.py vine_wrap.c vine_wrap.o *.pyc __pycache__

install: all
	mkdir -p $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/compat
	cp ndcctools/taskvine/*.py $(DSPYTHONSO) $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine
	cp ndcctools/taskvine/compat/*.py $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/compat
	cp taskvine.py $(CCTOOLS_PYTHON3_PATH)/