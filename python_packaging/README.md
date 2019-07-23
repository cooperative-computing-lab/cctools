# PYTHON PACKAGE ANALYZE

This script parses the Python script text to find all `import` and `from` statements, thus collecting all top-level module dependencies for the script. It then checks the list against a list of standard library modules, and if a module is not within the standard library, it is kept in the dependencies list, otherwise it is discarded. The script also analyzes the Python interpreter to find its version. It then dumps the Python version and dependencies list into the specified output file in JSON format.

### Possible Tune-ups
1. Utilize `ModuleFinder` library to get complete list of modules that are used by the Python script
- Provides more comprehensive list of modules used, including system-level modules
- Takes longer to run compared to the currently-implemented parsing algorithm
2. Use `pip freeze` to find all modules that are installed within the machine
- Instead of seeing if the module is not a system module, just see if it is installed on the machine
- Requires that the module be installed on the master machine
- Also misses cases where a module is installed to the machine, but not by pip
- The advantage to this option is that `pip freeze` includes versions, so you can add version numbers for module dependencies to get more accurate pip installations into the virtual environment

# PYTHON PACKAGE CREATE

This script takes the JSON file created by `python_package_analyze`, parses it, and creates a Conda environment with all necessary dependencies and the appropriate python version inside. It then uses conda_pack to pack the environment into a tarball, overwriting the same environment if necessary.

### Possible Tune-ups
1. Figure out alternative to using `subprocess.call()` to create the Conda environment 
- Most of the execution occurs within the subprocess call, so basically a Bash script, but easy to use Python to parse the JSON file and write to the requirement file
- Perhaps use a JSON parsing command line utility within Bash script instead, such as `jq`
- If a Conda environment API for Python is ever created, it would be very useful here
2. Remove requirement to redirect all output to `/dev/null`
- All output from the subprocess call is removed for organization purposes, but some commands like `pip install` might be useful for the user to see

# PYTHON PACKAGE RUN

This script is a wrapper script for the task to be run, running the task within the isolated Conda environment that contains all the necessary dependencies. This script is run at the worker machine, and it extracts the tarball, activates the environment, and then evaluates the task string. It includes the appropriate error handling for the tarball extraction and environment activation.

### Possible Tune-ups

1. Do protection checking against dangerous shell commands, as the script runs the command line argument directly
- The program directly runs the task string that is passed in, which means the user could send a task that is harmful to the worker machine
- Perhaps WorkQueue already uses protection checking for the task strings, in which case it is not necessary


### HOW TO TEST FUNCTIONALITY

Example script to run: `hi.py`

1. `./python_package_analyze hi.py output.json`
- Generates the appropriate JSON file in the current working directory
2. `./python_package_create output.json venv`
- Will create a Conda environment in the Conda "envs" folder, and will create a packed tarball of the environment named `venv.tar.gz` in the current working directory
- To more easily debug, remove the redirected output to /dev/null in the subprocess call to see all output of the environment creation and module installation
3. `./python_package_run venv "python3 hi.py"
- Runs the `python3 hi.py` task command within the activated `venv` Conda environment
