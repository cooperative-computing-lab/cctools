#!/usr/bin/env python3

# Copyright (C) 2022 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


from poncho import package_analyze as analyze
from poncho import package_create as create
import argparse
import json
import os
import ast
import tarfile
import hashlib

shebang = "#! /usr/bin/env python\n\n"

network_code = \
'''
import socket
import json
import os
import sys
import threading
import queue

def remote_execute(func):
    def remote_wrapper(event, q=None):
        if q:
            event = json.loads(event)
        kwargs = event["fn_kwargs"]
        args = event["fn_args"]
        try:
            response = {
                "Result": func(*args, **kwargs),
                "StatusCode": 200
            }
        except Exception as e:
            response = { 
                "Result": str(e),
                "StatusCode": 500 
            }
        if not q:
            return response
        q.put(response)
    return remote_wrapper
    
read, write = os.pipe() 

def send_configuration(config):
    config_string = json.dumps(config)
    config_cmd = f"{len(config_string) + 1}\\n{config_string}\\n"
    sys.stdout.write(config_cmd)
    sys.stdout.flush()

def main():
    config = {
        "name": name(),
    }
    send_configuration(config)
    while True:
        while True:
            # wait for message from worker about what function to execute
            line = input()
            print(f"Network function received task: {line}", file=sys.stderr, flush=True)
            if len(line) >= 0:
                function_name, event_size = line.split(" ")
                if event_size:
                    # receive the bytes containing the event and turn it into a string
                    event_str = input()
                    if len(event_str) != int(event_size):
                        print(event_str, len(event_str), event_size, file=sys.stderr)
                        print("Size of event does not match what was sent: exiting", file=sys.stderr)
                        exit(0)
                    # turn the event into a python dictionary
                    event = json.loads(event_str)
                    # see if the user specified an execution method
                    exec_method = event.get("remote_task_exec_method", None)
                    print('Network function: recieved event: {}'.format(event), file=sys.stderr)
                    if exec_method == "thread":
                        # create a forked process for function handler
                        q = queue.Queue()
                        p = threading.Thread(target=globals()[function_name], args=(event_str, q))
                        p.start()
                        p.join()
                        response = json.dumps(q.get())
                    elif exec_method == "direct":
                        response = json.dumps(globals()[function_name](event))
                    else:
                        p = os.fork()
                        if p == 0:
                            response =globals()[function_name](event)
                            os.write(write, json.dumps(response).encode("utf-8"))
                            os._exit(-1)
                        elif p < 0:
                            print('Network function: unable to fork', file=sys.stderr)
                            response = { 
                                "Result": "unable to fork",
                                "StatusCode": 500 
                            }
                        else:
                            chunk = os.read(read, 65536).decode("utf-8")
                            all_chunks = [chunk]
                            while (len(chunk) >= 65536):
                                chunk = os.read(read, 65536).decode("utf-8")
                                all_chunks.append(chunk)
                            response = "".join(all_chunks)
                            os.waitid(os.P_PID, p, os.WEXITED)

                    print(response, flush=True)

                    break
            else:
                print("Network function could not read from worker\n", file=sys.stderr)
    return 0

'''
default_name_func = \
'''def name():
	return "my_coprocess"

'''
init_function = \
'''if __name__ == "__main__":
	main()

'''

def create_network_function(path, funcs, dest):
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
	output_file.write(network_code)
	# write name function code into it
	output_file.write(f"{name_source_code}\n")
	# iterate over every function the user requested and attempt to put it into the network function
	for function_code in function_source_code:
		output_file.write("@remote_execute\n")
		output_file.write(function_code)
		output_file.write("\n")
	output_file.write(init_function)
	output_file.close()

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
    if os.path.exists(envpath) and envpath.endswith(".tar.gz"):
        print("Cached environment found, checking if it is compatiable with new network function")
        with tarfile.open(envpath) as env_tar:
            env_spec = None
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

def pack_network_function(path, envpath):
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

