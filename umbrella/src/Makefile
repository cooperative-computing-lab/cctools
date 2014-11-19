include ../../config.mk
include ../../rules.mk

EXTERNAL_DEPENDENCIES = 
LIBRARIES = 
OBJECTS =
PROGRAMS = 
SCRIPTS = umbrella sw_db_create.py sw_db_select.py ec2_db_create.py ec2_db_select.py
TARGETS = $(PROGRAMS) $(LIBRARIES)

all: $(TARGETS)

clean:
	rm -f $(OBJECTS) $(TARGETS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	chmod 755 $(SCRIPTS)
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/
	cp software.db ec2.db $(CCTOOLS_INSTALL_DIR)/bin/

test: all

.PHONY: all clean install test
