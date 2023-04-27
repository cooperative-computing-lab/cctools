include ../../../../config.mk
include $(CCTOOLS_HOME)/rules.mk

# Python always uses 'so' for its modules (even on Darwin)
CCTOOLS_DYNAMIC_SUFFIX = so
# SWIG produces code that causes a lot of warnings, so use -w to turn those off.
LOCAL_CCFLAGS = -fPIC -w -DNDEBUG $(CCTOOLS_PYTHON3_CCFLAGS)
LOCAL_LINKAGE = $(CCTOOLS_PYTHON3_LDFLAGS) $(CCTOOLS_GLOBUS_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)

EXTERNAL_DEPENDENCIES = $(CCTOOLS_HOME)/chirp/src/libchirp.a $(CCTOOLS_HOME)/dttools/src/libdttools.a
CHIRPPYTHONSO = ndcctools/_cchirp.$(CCTOOLS_DYNAMIC_SUFFIX)
LIBRARIES = $(CHIRPPYTHONSO)
OBJECTS = chirp_wrap.o $(CCTOOLS_HOME)/chirp/src/chirp_swig_wrap.o
TARGETS = ndcctools/cchirp.py $(LIBRARIES)

all: $(TARGETS)

# The odd symlink in the following rule is necessary to overcome a problem
# in the framework search path emitted by the Python configuration on macOS.
chirp_wrap.c ndcctools/cchirp.py: chirp.i
	@echo "SWIG chirp.i (python)"
	@$(CCTOOLS_SWIG) -o chirp_wrap.c -python -I$(CCTOOLS_HOME)/dttools/src/ -I$(CCTOOLS_HOME)/chirp/src chirp.i
	mv cchirp.py ndcctools/

$(CHIRPPYTHONSO): $(OBJECTS) $(EXTERNAL_DEPENDENCIES)

clean:
	rm -rf $(OBJECTS) $(TARGETS) chirp_wrap.c *.pyc __pycache__ ndcctools/__pycache__

test:

install: all
	mkdir -p $(CCTOOLS_PYTHON3_PATH)/ndcctools
	cp ndcctools/chirp.py ndcctools/__init__.py $(TARGETS) $(CCTOOLS_PYTHON3_PATH)/ndcctools
	cp chirp.py $(CCTOOLS_PYTHON3_PATH)
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc/cctools/examples/chirp/python
	cp chirp_python_example.py chirp_jobs_python_example.py $(CCTOOLS_INSTALL_DIR)/doc/cctools/examples/chirp/python
