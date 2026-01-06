include ../../../config.mk
include ../../../rules.mk

SUBDIRS = dagvine

all clean install test lint format: $(SUBDIRS)

$(SUBDIRS): %:
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: all clean install test lint format $(SUBDIRS) example

example:
	PYTHONPATH=../../../ python -m taskvine.src.graph.example_blueprint
