# Variables
CONFIG_PATH = test/program-config.yml
PYTHON = python3
MAIN_SCRIPT = src/main.py

all: test

run:
	@echo "Running the main script with config file..."
	$(PYTHON) $(MAIN_SCRIPT) $(CONFIG_PATH)

test: run

compile:
	@echo "Compiling all Python code into a single file..."
	$(PYTHON) concat_sources.py
clean:
	@echo "Cleaning up..."
	rm -rf test/*.log test/*.json