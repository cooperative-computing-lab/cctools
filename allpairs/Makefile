include ../Makefile.config
include ../Makefile.rules

all test install clean:
	for p in src test doc; do cd $$p && $(MAKE) $@ && cd .. ; done

.PHONY: all install test clean
