include ../../../../config.mk

MODULE_DIR  := $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/dagvine
SUBDIRS     := executor test
PY_SOURCES  := __init__.py dagvine.py utils.py workflow.py
PY_PACKAGES := dask_adaptor task_runner
PY_PACKAGE_SOURCES := $(foreach pkg,$(PY_PACKAGES),$(wildcard $(pkg)/*.py))

.PHONY: all install clean lint format test

all format test:
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@ || exit $$?; \
	done

install: all
	mkdir -p $(MODULE_DIR)
	cp $(PY_SOURCES) $(MODULE_DIR)
	@for pkg in $(PY_PACKAGES); do \
		mkdir -p $(MODULE_DIR)/$$pkg; \
		cp $$pkg/*.py $(MODULE_DIR)/$$pkg || exit $$?; \
	done
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir install || exit $$?; \
	done

clean:
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir clean || exit $$?; \
	done
	rm -rf __pycache__
	@for pkg in $(PY_PACKAGES); do \
		rm -rf $$pkg/__pycache__; \
	done
	$(MAKE) -C test clean

lint:
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir lint || exit $$?; \
	done
	flake8 --ignore=$(CCTOOLS_FLAKE8_IGNORE_ERRORS) \
		--exclude=$(CCTOOLS_FLAKE8_IGNORE_FILES) \
		$(PY_SOURCES) $(PY_PACKAGE_SOURCES)
