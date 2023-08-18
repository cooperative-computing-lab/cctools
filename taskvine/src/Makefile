include ../../config.mk
include ../../rules.mk

TARGETS=manager worker tools bindings examples

all: $(TARGETS)

worker: manager
bindings: manager
tools: manager
examples: manager worker tools bindings

$(TARGETS): %:
	$(MAKE) -C $@ $(MAKECMDGOALS)

install:
	for d in $(TARGETS); do $(MAKE) -C $$d install; done

clean:
	for d in $(TARGETS); do $(MAKE) -C $$d clean; done

lint: $(TARGETS)

format: $(TARGETS)

.PHONY: all clean install test lint format $(TARGETS)
