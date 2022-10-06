include ../../../../config.mk
include $(CCTOOLS_HOME)/rules.mk

# Python always uses 'so' for its modules (even on Darwin)
CCTOOLS_DYNAMIC_SUFFIX = so
# SWIG produces code that causes a lot of warnings, so use -w to turn those off.
LOCAL_CCFLAGS = -fPIC -w $(CCTOOLS_PYTHON2_CCFLAGS)
LOCAL_LINKAGE = $(CCTOOLS_PYTHON2_LDFLAGS) $(CCTOOLS_GLOBUS_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)

EXTERNAL_DEPENDENCIES = $(CCTOOLS_HOME)/chirp/src/libchirp.a $(CCTOOLS_HOME)/dttools/src/libdttools.a
CHIRPPYTHONSO = _CChirp.$(CCTOOLS_DYNAMIC_SUFFIX)
LIBRARIES = $(CHIRPPYTHONSO)
OBJECTS = chirp_wrap.o $(CCTOOLS_HOME)/chirp/src/chirp_swig_wrap.o
TARGETS = Chirp.py $(LIBRARIES)

all: $(TARGETS)

Chirp.py: chirp_wrap.c CChirp.py

chirp_wrap.c CChirp.py: chirp.i chirp.binding.py
	@echo "SWIG chirp.i (python)"
	@$(CCTOOLS_SWIG) -o chirp_wrap.c -python -I$(CCTOOLS_HOME)/dttools/src/ -I$(CCTOOLS_HOME)/chirp/src chirp.i
	@cat CChirp.py > Chirp.py
	@cat chirp.binding.py >> Chirp.py

$(CHIRPPYTHONSO): $(OBJECTS) $(EXTERNAL_DEPENDENCIES)

clean:
	rm -f $(OBJECTS) $(TARGETS) Chirp.py CChirp.py chirp_wrap.c *.pyc

test:

install: all
	mkdir -p $(CCTOOLS_PYTHON2_PATH)
	cp Chirp.py $(CHIRPPYTHONSO) $(CCTOOLS_PYTHON2_PATH)/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/doc
	cp chirp_python_example.py chirp_jobs_python_example.py $(CCTOOLS_INSTALL_DIR)/doc/
