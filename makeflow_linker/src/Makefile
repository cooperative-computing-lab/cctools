include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES = ../../dttools/src/libdttools.a
SCRIPTS = makeflow_linker_python_driver makeflow_linker_perl_driver
PROGRAMS = makeflow_linker
TARGETS = $(PROGRAMS)

all: $(TARGETS)

$(PROGRAMS): $(EXTERNAL_DEPENDENCIES)

clean:
	rm -f $(OBJECTS) $(TARGETS)

test: all

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/
