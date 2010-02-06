include ../Makefile.config
include ../Makefile.rules

all test install clean:
	for p in allpairs sequencing wavefront makeflow work_queue_pow_example; do cd $$p && $(MAKE) $@ && cd .. ; done
