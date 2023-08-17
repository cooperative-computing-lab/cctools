include ../../config.mk
include ../../rules.mk

TARGETS=manager worker tools bindings examples

all: $(TARGETS)

worker: manager
bindings: manager
tools: manager
examples: manager worker tools bindings

$(TARGETS): %:
	$(MAKE) -C $@

install:
	for d in $(TARGETS); do $(MAKE) -C $$d install; done

clean:
	for d in $(TARGETS); do $(MAKE) -C $$d clean; done

test:
	for d in $(TARGETS); do $(MAKE) -C $$d test; done

.PHONY: all clean install test $(TARGETS)
