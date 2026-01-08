include ../../../../config.mk
include ../../../../rules.mk

MODULE_DIR := $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/dagvine
SUBDIRS    := vine_graph blueprint_graph

.PHONY: all install clean lint format $(SUBDIRS)

all: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@ all

install: all
	mkdir -p $(MODULE_DIR)
	cp dagvine.py $(MODULE_DIR)
	cp __init__.py $(MODULE_DIR)
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir install; \
	done

clean:
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir clean; \
	done
	rm -rf build

lint:
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir lint; \
	done

format:
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir format; \
	done
