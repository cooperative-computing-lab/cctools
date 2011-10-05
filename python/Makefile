include ../Makefile.config
include ../Makefile.rules

all test install clean:
	@for p in python-workqueue; do cd $$p && $(MAKE) $@ && cd .. ; done

.PHONY: all test install clean
