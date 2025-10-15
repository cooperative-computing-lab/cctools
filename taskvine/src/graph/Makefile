include ../../../config.mk
include ../../../rules.mk

SUBDIRS = GraphInvoker

all clean install test lint format: $(SUBDIRS)

$(SUBDIRS): %:
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: all clean install test lint format $(SUBDIRS)


