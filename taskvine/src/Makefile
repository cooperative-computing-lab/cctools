include ../../config.mk
include ../../rules.mk

TARGETS=manager worker tools bindings examples vine_graph

all: $(TARGETS)

worker: manager
vine_graph: manager
bindings: manager vine_graph
tools: manager
examples: manager worker tools bindings

# Backward-compatible alias for callers that still use `make graph`.
graph: vine_graph

$(TARGETS): %:
	$(MAKE) -C $@ $(if $(filter $(TARGETS) graph,$(MAKECMDGOALS)),,$(MAKECMDGOALS))

install:
	for d in $(TARGETS); do $(MAKE) -C $$d install; done

clean:
	for d in $(TARGETS); do $(MAKE) -C $$d clean; done

lint: $(TARGETS)

format: $(TARGETS)

.PHONY: all clean install test lint format graph $(TARGETS)
