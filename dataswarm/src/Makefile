include ../../config.mk
include ../../rules.mk

TARGETS=manager worker tools scripts bindings

all:
	for d in $(TARGETS); do $(MAKE) -C $$d; done

install:
	for d in $(TARGETS); do $(MAKE) -C $$d install; done

clean:
	for d in $(TARGETS); do $(MAKE) -C $$d clean; done

.PHONY: all clean install test
