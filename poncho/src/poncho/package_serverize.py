#!/usr/bin/env python3

# Copyright (C) 2022 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


from ndcctools.poncho import package_analyze as analyze
from ndcctools.poncho import package_create as create
from ndcctools.poncho.wq_network_code import wq_network_code
from ndcctools.poncho.library_network_code import library_network_code

import json
import os
import sys
import stat
import ast
import types
import tarfile
import hashlib
import inspect

import cloudpickle

shebang = "#! /usr/bin/env python3\n\n"

default_name_func = """def name():
    return "my_coprocess"

"""
init_function = """if __name__ == "__main__":
    main()

"""


# Generates a list of import statements based on the given argument.
# @param hoisting_modules  A list of modules imported at the preamble of library, including packages, functions and classes.
def generate_hoisting_code(hoisting_modules):
    if not hoisting_modules:
        return

    if not isinstance(hoisting_modules, list):
        raise ValueError("Expected 'hoisting_modules' to be a list.")

    hoisting_code_list = []
    for module in hoisting_modules:
        if isinstance(module, types.ModuleType):
            hoisting_code_list.append(f"import {module.__name__}")
        elif inspect.isfunction(module):
            source_code = inspect.getsource(module)
            hoisting_code_list.append(source_code)
        elif inspect.isclass(module):
            source_code = inspect.getsource(module)
            hoisting_code_list.append(source_code)

    return hoisting_code_list


# Create the library driver code that will be run as a normal task
# on workers and execute function invocations upon workers' instructions.
# @param path            Path to the temporary Python script containing functions.
# @param funcs           A list of relevant function names.
# @param dest            Path to the final library script.
# @param version         Whether this is for workqueue or taskvine serverless code.
# @param hoisting_modules  A list of modules imported at the preamble of library, including packages, functions and classes.
def create_library_code(path, funcs, dest, version, hoisting_modules=None):
    # create output file
    with open(dest, "w") as output_file:
        # write shebang to file
        output_file.write(shebang)
        # write imports to file
        hoisting_code_list = generate_hoisting_code(hoisting_modules)
        if hoisting_code_list:
            for hoisting_code in hoisting_code_list:
                output_file.write(f"{hoisting_code}\n")

        function_source_code = []
        name_source_code = ""
        absolute_path = os.path.abspath(path)
        # open the source file, parse the code into an ast, and then unparse functions back into python code
        with open(absolute_path, "r") as source:
            code = ast.parse(source.read(), filename=absolute_path)
            for stmt in ast.walk(code):
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
        print(
            "Cached environment found, checking if it is compatiable with new library code"
        )
        with tarfile.open(envpath) as env_tar:
            for member in env_tar:
                if member.name == "conda_spec.yml":
                    with env_tar.extractfile(member) as f:
                        env_spec = hashlib.md5(sort_spec(f)).digest()
                    break
            if env_spec is None:
                print(
                    "Error, could not find conda_spec.yml in cached environment, creating new environment"
                )
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


# Generate a hash value from all information about a library
# @param library_name   The name of the library
# @param function_list  A list of functions in the library
# @param poncho_env     The name of an already prepared poncho environment
# @param init_command   A string describing a shell command to execute before the library task is run
# @param add_env         Whether to automatically create and/or add environment to the library
# @param hoisting_modules  A list of modules imported at the preamble of library, including packages, functions and classes.
# @param exec_mode       Execution mode that the library should use to run function calls. Either 'direct' or 'fork'
# @param library_context_info   A list containing [library_context_func, library_context_args, library_context_kwargs]. Used to create the library context on remote nodes.
# @return               A hash value.
def generate_library_hash(library_name, function_list, poncho_env, init_command, add_env, hoisting_modules, exec_mode, library_context_info):
    library_info = [library_name]
    function_list = list(function_list)
    function_names = set()

    if library_context_info:
        function_list += library_context_info[0]

    for function in function_list:
        if function.__name__ is None:
            raise ValueError('A function must have a name.')
        if function.__name__ in function_names:
            raise ValueError('A library cannot have two functions with the same name.')
        else:
            function_names.add(function.__name__)
        
        try:
            library_info.append(inspect.getsource(function))
        except OSError:
            # process the function's code object
            function_co = function.__code__
            library_info.append(str(func_co.co_name))
            library_info.append(str(func_co.co_argcount))
            library_info.append(str(func_co.co_posonlyargcount))
            library_info.append(str(func_co.co_kwonlyargcount))
            library_info.append(str(func_co.co_nlocals))
            library_info.append(str(func_co.co_varnames))
            library_info.append(str(func_co.co_cellvars))
            library_info.append(str(func_co.co_freevars))
            library_info.append(str(func_co.co_code))
            library_info.append(str(func_co.co_consts))
            library_info.append(str(func_co.co_names))
            library_info.append(str(func_co.co_stacksize))

    library_info.append(str(poncho_env))
    library_info.append(str(init_command))
    library_info.append(str(add_env))
    library_info.append(str(hoisting_modules))
    library_info.append(str(exec_mode))

    if library_context_info:
        if isinstance(library_context_info[1], list):
            for arg in library_context_info[1]:
                library_info.append(str(arg))
        if isinstance(library_context_info[2], dict):
            for kwarg in library_context_info[2]:
                library_info.append(str(kwarg))
                library_info.append(str(library_context_info[2][kwarg]))
    
    library_info = ''.join(library_info)    # linear time complexity
    msg = hashlib.sha1()
    msg.update(library_info.encode('utf-8'))
    return msg.hexdigest()


# Combine function names and function bodies to create a unique hash of the functions.
# Note that these functions must have source code, so dynamic functions generated from
# Python's exec or Jupyter Notebooks won't work here.
# @param functions  A list of functions to generate the hash value from.
# @return           a string of hex characters resulted from hashing the contents and names of functions.
def generate_functions_hash(functions: list, hoisting_modules=None) -> str:
    source_code = []
    if hoisting_modules:
        source_code.extend(["import " + module.__name__ + "\n" for module in hoisting_modules])

    for fnc in functions:
        try:
            source_code.append(inspect.getsource(fnc))
        except OSError as e:
            print(
                f"Can't retrieve source code of function {fnc.__name__}.",
                file=sys.stderr,
            )
            raise
    
    return hashlib.md5(" ".join(source_code).encode("utf-8")).hexdigest()


def generate_taskvine_library_code(library_path, hoisting_modules=None):
    # create output file
    with open(library_path, "w") as output_file:
        # write shebang to file
        output_file.write(shebang)
        # write imports to file
        hoisting_code_list = generate_hoisting_code(hoisting_modules)
        if hoisting_code_list:
            for hoisting_code in hoisting_code_list:
                output_file.write(f"{hoisting_code}\n")

        raw_source_code = inspect.getsource(library_network_code)
        network_code = "\n".join([line[4:] for line in raw_source_code.split("\n")[1:]])
        output_file.write(network_code)
        output_file.write(init_function)

    st = os.stat(library_path)
    os.chmod(library_path, st.st_mode | stat.S_IEXEC)


# Create a library file and a poncho environment tarball from a list of functions as needed.
# @param    library_cache_path      path to directory to create the library python file and the environment tarball.
# @param    library_code_path       path to the to-be-created library code.
# @param    library_env_path        path to the to-be-created poncho environment tarball.
# @param    library_info_path       path to the to-be-created library information in serialized format.
# @param    functions               list of functions to include in the library
# @param    library_name            name of the library
# @param    need_pack               whether to create a poncho environment tarball
# @param    hoisting_modules        a list of modules to be imported at the preamble of library
# @param    exec_mode               whether to execute invocations directly or by forking
# @param    library_context_info    a list containing a library's context to be created remotely
# @return   name of the file containing serialized information about the library
def generate_library(library_cache_path,
                     library_code_path,
                     library_env_path,
                     library_info_path,
                     functions,
                     library_name,
                     need_pack=True,
                     hoisting_modules=None,
                     library_exec_mode='direct',
                     library_context_info=None
):
    # create library_info.clpk
    library_info = {}
    library_info['function_list'] = {}
    for func in functions:
        library_info['function_list'][func.__name__] = cloudpickle.dumps(func)
    library_info['library_name'] = library_name
    library_info['exec_mode'] = library_exec_mode
    library_info['context_info'] = cloudpickle.dumps(library_context_info)
    with open(library_info_path, 'wb') as f:
        cloudpickle.dump(library_info, f) 

    # create library_code.py
    generate_taskvine_library_code(library_code_path, hoisting_modules=hoisting_modules)

    # pack environment
    if need_pack:
        pack_library_code(library_code_path, library_env_path)


# Create a library file and a poncho environment tarball from a list of functions as needed.
# @param    path                    path to directory to create the library python file and the environment tarball.
# @param    functions               list of functions to include in the library
# @param    need_pack               whether to create a poncho environment tarball
# @param    hoisting_modules        a list of modules to be imported at the preamble of library
# @param    exec_mode               whether to execute invocations directly or by forking
# @param    library_context_info    a list containing a library's context to be created remotely
# @return   name of the file containing serialized information about functions
def serverize_library_from_code(
    path, functions, name, need_pack=True, hoisting_modules=None, exec_mode='direct', library_context_info=None
):
    library_info = {}
    library_info['function_list'] = {}
    for func in functions:
        library_info['function_list'][func.__name__] = cloudpickle.dumps(func)

    library_info['library_name'] = name
    library_info['hoisting_modules'] = hoisting_modules
    library_info['exec_mode'] = exec_mode
    library_info['context_info'] = cloudpickle.dumps(library_context_info)

    with open(f'{path}/library_info.clpk', 'wb') as f:
        cloudpickle.dump(library_info, f)

    tmp_library_path = f"{path}/tmp_library.py"

    # Write out functions into a temporary python file.
    # Also write a helper function to identify the name of the library.
    with open(tmp_library_path, "w") as temp_source_file:
        temp_source_file.write("".join([inspect.getsource(fnc) for fnc in functions]))
        temp_source_file.write(f"def name():\n\treturn '{name}'")

    # create the final library code from that temporary file
    create_library_code(
        tmp_library_path,
        [fnc.__name__ for fnc in functions],
        path + "/library_code.py",
        "taskvine",
        hoisting_modules=hoisting_modules,
    )
    # remove the temp library file
    os.remove(tmp_library_path)

    # and pack it into an environment, if needed
    if need_pack:
        pack_library_code(path + "/library_code.py", path + "/library_env.tar.gz")


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
