include ../../../config.mk
include ../../../rules.mk

SUBDIRS = vinedag

all clean install test lint format: $(SUBDIRS)

$(SUBDIRS): %:
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: all clean install test lint format $(SUBDIRS)


