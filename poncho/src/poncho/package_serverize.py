#!/usr/bin/env python3

# Copyright (C) 2022 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


from ndcctools.poncho import package_analyze as analyze
from ndcctools.poncho import package_create as create

import argparse
import json
import os
import stat
import ast
import tarfile
import hashlib
import inspect

shebang = "#! /usr/bin/env python3\n\n"

def library_network_code():
    import json
    import os
    import sys

    def remote_execute(func):
        def remote_wrapper(event):
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
            return response
        return remote_wrapper

    read, write = os.pipe()

    def send_configuration(config):
        config_string = json.dumps(config)
        config_cmd = f"{len(config_string) + 1}\n{config_string}\n"
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
                try:
                    line = input()
                # if the worker closed the pipe connected to the input of this process, we should just exit
                except EOFError:
                    sys.exit(0)
                function_name, event_size, function_sandbox = line.split(" ", maxsplit=2)
                if event_size:
                    # receive the bytes containing the event and turn it into a string
                    event_str = input()
                    if len(event_str) != int(event_size):
                        print(event_str, len(event_str), event_size, file=sys.stderr)
                        print("Size of event does not match what was sent: exiting", file=sys.stderr)
                        sys.exit(1)
                    # turn the event into a python dictionary
                    event = json.loads(event_str)
                    # see if the user specified an execution method
                    exec_method = event.get("remote_task_exec_method", None)
                    if exec_method == "direct":
                        library_sandbox = os.getcwd()
                        try:
                            os.chdir(function_sandbox)
                            response = json.dumps(globals()[function_name](event))
                        except Exception as e:
                            print(f'Library code: Function call failed due to {e}', file=sys.stderr)
                            sys.exit(1)
                        finally:
                            os.chdir(library_sandbox)
                    else:
                        p = os.fork()
                        if p == 0:
                            os.chdir(function_sandbox)
                            response = globals()[function_name](event)
                            os.write(write, json.dumps(response).encode("utf-8"))
                            os._exit(0)
                        elif p < 0:
                            print(f'Library code: unable to fork to execute {function_name}', file=sys.stderr)
                            response = {
                                "Result": "unable to fork",
                                "StatusCode": 500
                            }
                        else:
                            max_read = 65536
                            chunk = os.read(read, max_read).decode("utf-8")
                            all_chunks = [chunk]
                            while (len(chunk) >= max_read):
                                chunk = os.read(read, max_read).decode("utf-8")
                                all_chunks.append(chunk)
                            response = "".join(all_chunks)
                            os.waitpid(p, 0)
                    print(response, flush=True)
        return 0

def wq_network_code():
    import socket
    import json
    import os
    import sys
    def remote_execute(func):
        def remote_wrapper(event):
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
            return response
        return remote_wrapper

    read, write = os.pipe()
    def send_configuration(config):
        config_string = json.dumps(config)
        config_cmd = f"{len(config_string) + 1}\n{config_string}\n"
        sys.stdout.write(config_cmd)
        sys.stdout.flush()
    def main():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            # modify the port argument to be 0 to listen on an arbitrary port
            s.bind(('localhost', 0))
        except Exception as e:
            s.close()
            print(e, file=sys.stderr)
            sys.exit(1)
        # information to print to stdout for worker
        config = {
                "name": name(),
                "port": s.getsockname()[1],
                }
        send_configuration(config)
        while True:
            s.listen()
            conn, addr = s.accept()
            print('Network function: connection from {}'.format(addr), file=sys.stderr)
            while True:
                # peek at message to find newline to get the size
                event_size = None
                line = conn.recv(100, socket.MSG_PEEK)
                eol = line.find(b'\n')
                if eol >= 0:
                    size = eol+1
                    # actually read the size of the event
                    input_spec = conn.recv(size).decode('utf-8').split()
                    function_name = input_spec[0]
                    task_id = int(input_spec[1])
                    event_size = int(input_spec[2])
                try:
                    if event_size:
                        # receive the bytes containing the event and turn it into a string
                        event_str = conn.recv(event_size).decode("utf-8")
                        # turn the event into a python dictionary
                        event = json.loads(event_str)
                        # see if the user specified an execution method
                        exec_method = event.get("remote_task_exec_method", None)
                        os.chdir(f"t.{task_id}")
                        if exec_method == "direct":
                            response = json.dumps(globals()[function_name](event)).encode("utf-8")
                        else:
                            p = os.fork()
                            if p == 0:
                                response =globals()[function_name](event)
                                os.write(write, json.dumps(response).encode("utf-8"))
                                os._exit(0)
                            elif p < 0:
                                print(f'Network function: unable to fork to execute {function_name}', file=sys.stderr)
                                response = {
                                    "Result": "unable to fork",
                                    "StatusCode": 500
                                }
                            else:
                                max_read = 65536
                                chunk = os.read(read, max_read).decode("utf-8")
                                all_chunks = [chunk]
                                while (len(chunk) >= max_read):
                                    chunk = os.read(read, max_read).decode("utf-8")
                                    all_chunks.append(chunk)
                                response = "".join(all_chunks).encode("utf-8")
                                os.waitpid(p, 0)
                        response_size = len(response)
                        size_msg = "{}\n".format(response_size)
                        # send the size of response
                        conn.sendall(size_msg.encode('utf-8'))
                        # send response
                        conn.sendall(response)
                        os.chdir("..")
                        break
                except Exception as e:
                    print("Network function encountered exception ", str(e), file=sys.stderr)
        return 0

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

def generate_functions_hash(functions):
    # combine function names and function bodies to create a unique hash of the functions
    source_code = "".join([inspect.getsource(fnc) for fnc in functions]) + "".join([fnc.__name__ for fnc in functions])
    return hashlib.md5(source_code.encode("utf-8")).hexdigest()

def serverize_library_from_code(path, functions, name, need_pack=True):
    tmp_library_path = f"{path}/tmp_library.py"

    # write out functions into a temporary python file
    with open(tmp_library_path, "w") as temp_source_file:
        temp_source_file.write("".join([inspect.getsource(fnc) for fnc in functions]))
        temp_source_file.write(f"def name():\n\treturn '{name}'")

    # create the final library code from that temporary file
    create_library_code(tmp_library_path, [fnc.__name__ for fnc in functions], path + "/library_code.py", "taskvine")

    # and pack it into an environment, if needed
    if need_pack:
        pack_library_code(path + "/library_code.py", path + "/library_env.tar.gz")
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
