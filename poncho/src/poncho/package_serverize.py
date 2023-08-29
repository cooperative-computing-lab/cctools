#!/usr/bin/env python3

# Copyright (C) 2022 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


from ndcctools.poncho import package_analyze as analyze
from ndcctools.poncho import package_create as create
from ndcctools.poncho.wq_network_code import wq_network_code
from ndcctools.poncho.library_network_code import library_network_code

import argparse
import json
import os
import stat
import ast
import tarfile
import hashlib
import inspect

shebang = "#! /usr/bin/env python3\n\n"

default_name_func = \
'''def name():
	return "my_coprocess"

'''
init_function = \
'''if __name__ == "__main__":
	main()

'''

def create_library_code(path, funcs, dest, version):
	import_modules = []
	function_source_code = []
	name_source_code = ""
	absolute_path = os.path.abspath(path)
	# open the source file, parse the code into an ast, and then unparse the ast import statements and functions back into python code
	with open(absolute_path, 'r') as source:
		code = ast.parse(source.read(), filename=absolute_path)
		for stmt in ast.walk(code):
			if isinstance(stmt, ast.Import) or isinstance(stmt, ast.ImportFrom):
				import_modules.append(ast.unparse(stmt))
			if isinstance(stmt, ast.FunctionDef):
				if stmt.name == "name":
					name_source_code = ast.unparse(stmt)
				elif stmt.name in funcs:
					function_source_code.append(ast.unparse(stmt))
					funcs.remove(stmt.name)
	if name_source_code == "":
		print("No name function found, defaulting to my_coprocess")
		name_source_code = default_name_func
	for func in funcs:
		print(f"No function found named {func}, skipping")
	# create output file
	output_file = open(dest, "w")
	# write shebang to file
	output_file.write(shebang)
	# write imports to file
	for import_module in import_modules:
		output_file.write(f"{import_module}\n")
	# write network code into it
	if version == "work_queue":
		raw_source_fnc = wq_network_code
	elif version == "taskvine":
		raw_source_fnc = library_network_code
	raw_source_code = inspect.getsource(raw_source_fnc)
	network_code = "\n".join([line[4:] for line in raw_source_code.split("\n")[1:]])
	output_file.write(network_code)
	# write name function code into it
	output_file.write(f"{name_source_code}\n")
	# iterate over every function the user requested and attempt to put it into the library code
	for function_code in function_source_code:
		output_file.write("@remote_execute\n")
		output_file.write(function_code)
		output_file.write("\n")
	output_file.write(init_function)
	output_file.close()
	st = os.stat(dest)
	os.chmod(dest, st.st_mode | stat.S_IEXEC)

def sort_spec(spec):
    sorted_spec = json.load(spec)
    conda_deps = []
    nested_deps = []
    for dep in sorted_spec["dependencies"]:
        if not isinstance(dep, dict):
            conda_deps.append(dep)
        else:
            nested_deps.append(dep)
    for dep in nested_deps:
        for key in dep.keys():
            dep[key] = dep[key].sort()
    return json.dumps(sorted(conda_deps) + nested_deps, sort_keys=True).encode("utf-8")

def search_env_for_spec(envpath):
    env_spec = None
    if os.path.exists(envpath) and envpath.endswith(".tar.gz"):
        print("Cached environment found, checking if it is compatiable with new library code")
        with tarfile.open(envpath) as env_tar:
            for member in env_tar:
                if member.name == "conda_spec.yml":
                    with env_tar.extractfile(member) as f:
                        env_spec = hashlib.md5(sort_spec(f)).digest()
                    break
            if env_spec is None:
                print("Error, could not find conda_spec.yml in cached environment, creating new environment")
    else:
        print("No environment found at output path, creating new environment")
    return env_spec

def pack_library_code(path, envpath):
    prev_env_spec = search_env_for_spec(envpath)

    new_env = analyze.create_spec(path)
    with open("/tmp/tmp.json", "w") as f:
        json.dump(new_env, f, indent=4, sort_keys=True)
    create.create_conda_spec("/tmp/tmp.json", "/tmp/", create._find_local_pip())
    with open("/tmp/conda_spec.yml", "rb") as f:
        new_env_spec = hashlib.md5(sort_spec(f)).digest()
    if prev_env_spec == new_env_spec:
        print("Cached environment still usable")
    else:
        print("Cached package is out of date, rebuilding")
        create.pack_env("/tmp/tmp.json", envpath)

# Combine function names and function bodies to create a unique hash of the functions.
# Note that these functions must have source code, so dynamic functions generated from
# Python's exec or Jupyter Notebooks won't work here.
# @param functions  A list of functions to generate the hash value from.
# @return           a string of hex characters resulted from hashing the contents and names of functions.
def generate_functions_hash(functions: list) -> str:
    source_code = ""
    for fnc in functions:
        try:
            source_code += fnc.__name__
            source_code += inspect.getsource(fnc)
        except OSError as e:
            print(f"Can't retrieve source code of function {fnc.__name__}.")
            raise
    return hashlib.md5(source_code.encode("utf-8")).hexdigest()

# Create a library file and a poncho environment tarball from a list of functions as needed.
# The functions in the list must have source code for this code to work.
# @param path       path to directory to create the library python file and the environment tarball.
# @param functions  list of functions to include in the 
def serverize_library_from_code(path, functions, name, need_pack=True):
    tmp_library_path = f"{path}/tmp_library.py"

    # write out functions into a temporary python file
    with open(tmp_library_path, "w") as temp_source_file:
        temp_source_file.write("".join([inspect.getsource(fnc) for fnc in functions]))
        temp_source_file.write(f"def name():\n\treturn '{name}'")

    # create the final library code from that temporary file
    create_library_code(tmp_library_path, [fnc.__name__ for fnc in functions], path + "/library_code.py", "taskvine")

    # remove the temp library file
    os.remove(tmp_library_path)

    # and pack it into an environment, if needed
    if need_pack:
        pack_library_code(path + "/library_code.py", path + "/library_env.tar.gz")
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
