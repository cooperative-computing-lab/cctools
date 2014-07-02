include ../../config.mk
include ../../rules.mk

SCRIPTS = resource_monitor_visualizer.py
STATIC_RESOURCES = resource_monitor_visualizer_static

all:

clean:

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp -r $(STATIC_RESOURCES) $(CCTOOLS_INSTALL_DIR)/lib/

test:

uninstall:
	rm -rf $(CCTOOLS_INSTALL_DIR)/lib/$(STATIC_RESOURCES)
	cd $(CCTOOLS_INSTALL_DIR) && rm -f $(SCRIPTS)

.PHONY: all clean install uninstall test
