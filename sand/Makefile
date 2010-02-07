include ../Makefile.config
include ../Makefile.rules

all test install clean:
	for p in doc sandtools alignment filtering formatting ; do cd $$p && $(MAKE) $@ && cd .. ; done
