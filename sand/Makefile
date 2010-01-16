include ../Makefile.config
include ../Makefile.rules

all test install clean:
	for p in alignment filtering formatting ; do cd $$p && $(MAKE) $@ && cd .. ; done
