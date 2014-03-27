include ../Makefile.config
include ../Makefile.rules

all test clean:
	@for p in wq_replica_exchange; do cd $$p && $(MAKE) $@ && cd .. ; done

install:

.PHONY: all test install clean
