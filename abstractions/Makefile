include ../Makefile.config
include ../Makefile.rules

all test install clean:
	for p in allpairs map mapreduce sequencing wavefront makeflow; do cd $$p && $(MAKE) $@ && cd .. ; done
