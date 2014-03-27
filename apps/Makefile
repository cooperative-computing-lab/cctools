include ../Makefile.config
include ../Makefile.rules

all test install clean:
	@for p in wq_replica_exchange; do cd $$p && $(MAKE) $@ && cd .. ; done

.PHONY: all test install clean
