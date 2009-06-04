include ../Makefile.config
include ../Makefile.rules

all test install clean:
	for p in allpairs mapreduce sequencing wavefront; do cd $$p && $(MAKE) $@ && cd .. ; done
