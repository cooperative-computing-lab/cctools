include ../Makefile.config
include ../Makefile.rules

all install clean test:
	for p in src; do cd $$p && $(MAKE) $@ && cd .. ; done

.PHONY: all test install clean

