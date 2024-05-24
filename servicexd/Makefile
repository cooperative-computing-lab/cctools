# Variables
CONFIG_PATH = test/bashapp/program-config.yml
PYTHON = python3
MAIN_SCRIPT = src/main.py
PROGRAM_NAME = sxd

all: test

run:
	@echo "Running the main script with config file..."
	$(PYTHON) $(MAIN_SCRIPT) $(CONFIG_PATH)

test: run

compile:
	@echo "Compiling all Python code into a single file..."
	$(PYTHON) py_source_combiner.py src $(PROGRAM_NAME).py
clean:
	@echo "Cleaning up..."
	rm -rf test/*.log test/*.json