#Todo: This file is incomplete

CONFIG_PATH = test/bashapp/program-config.yml
PYTHON = python3
MAIN_SCRIPT = src/main.py
PROGRAM_NAME = shepherd
SRC_DIR=shepherd
SRC=config_loader.py log_monitor.py program_executor.py service_manager.py shepherd.py
FLAGS =

all: test

run:
	@echo "Running the main script with config file..."
	$(PYTHON) $(MAIN_SCRIPT) $(CONFIG_PATH)

test: run

onefile:
	@echo "Compiling all Python code into a single file..."
	$(PYTHON) py_source_combiner.py $(SRC_DIR) $(PROGRAM_NAME).py ${FLAGS} $(SRC)
clean:
	@echo "Cleaning up..."
	find test -type f \( -name "*.json" -or -name "*log" \) -delete