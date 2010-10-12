include ../Makefile.config
include ../Makefile.rules

all test install clean:
	for p in src doc; do cd $$p && $(MAKE) $@ && cd .. ; done
