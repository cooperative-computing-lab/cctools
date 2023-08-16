#!/usr/bin/env python3

# Copyright (C) 2022 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os
import ast
import sys
import json
import glob
import argparse
import subprocess
import importlib
import email.parser

PKG_MAP = {}

def scan_pkgs():
    pkgs = json.loads(subprocess.check_output(['conda', 'list', '--json']))

    for pkg in [a for a in pkgs if a['channel'] == 'pypi']:
        parser = email.parser.BytesParser()
        m = parser.parsebytes(subprocess.check_output(['pip', 'show', '-f', pkg['name']]))
        prefix = m.get('Location')
        files = [x.strip() for x in m.get('Files').splitlines()]
        for f in files:
            PKG_MAP[os.path.abspath(os.path.join(prefix, f))] = pkg['name']

    for pkg in [a for a in pkgs if a['channel'] != 'pypi']:
        with open(os.path.join(sys.prefix, 'conda-meta', pkg['dist_name'] + '.json')) as f:
            for a in json.load(f)['files']:
                PKG_MAP[os.path.abspath(os.path.join(sys.prefix, a))] = pkg['name']

def strip_dots(pkg):
    if pkg.startswith('.'):
        raise ImportError('On {}, imports from the current module are not supported'.format(pkg))
    return pkg.split('.')[0]

def get_stmt_imports(stmt):
    imports = []
    if isinstance(stmt, ast.Import):
        for a in stmt.names:
            imports.append(strip_dots(a.name))
    elif isinstance(stmt, ast.ImportFrom):
        if stmt.level != 0:
            raise ImportError('On {}, imports from the current module are not supported'.format(stmt.module or '.'))
        imports.append(strip_dots(stmt.module))
    return imports

def analyze_toplevel(module):
    deps = []
    for stmt in module.body:
        deps += get_stmt_imports(stmt)
    return deps

def analyze_full(module):
    deps = []
    for stmt in ast.walk(module):
        deps += get_stmt_imports(stmt)
    return deps

def analyze_function(module, func_name):
    for stmt in ast.walk(module):
        if isinstance(stmt, ast.FunctionDef) and stmt.name == func_name:
            return analyze_full(stmt)

def choose_dep(conda_env, pip_env, conda_pkgs, pip_pkgs, pkg, required=True):
    for a in conda_env:
        if a.startswith(pkg + '='):
            conda_pkgs.add(a)
            return
    for a in pip_env:
        if a.startswith(pkg + '='):
            pip_pkgs.add(a)
            return
    if required:
        raise ImportError("Couldn't match {} to a conda/pip package".format(pkg))

def search_pkg(overrides, conda_env, pip_env, conda_pkgs, pip_pkgs, pkg):
    # Don't try to pack up modules built into the interpreter.
    if pkg in sys.builtin_module_names:
        return

    # Check if the user provided a package name
    if pkg in overrides:
        choose_dep(conda_env, pip_env, conda_pkgs, pip_pkgs, overrides[pkg])
        return

    # If there's a conda/pip package with the exact name, that's
    # probably what we want
    try:
        choose_dep(conda_env, pip_env, conda_pkgs, pip_pkgs, pkg)
        return
    except ImportError:
        pass

    # See who provides the file, falling back to the literal name
    lookup = PKG_MAP.get(importlib.import_module(pkg).__file__, pkg)
    choose_dep(conda_env, pip_env, conda_pkgs, pip_pkgs, lookup)

def export_env(overrides, imports, extra):
    imports = set(imports)
    extra = set(extra)
    env = json.loads(subprocess.check_output(['conda', 'env', 'export', '--json']))
    conda_env = env.pop('dependencies', [])
    pip_env = []
    for i in range(len(conda_env)):
        if isinstance(conda_env[i], dict):
            pip_env = conda_env.pop(i)['pip']

    conda_pkgs = set()
    pip_pkgs = set()
    for pkg in imports:
        search_pkg(overrides, conda_env, pip_env, conda_pkgs, pip_pkgs, pkg)

    for pkg in extra:
        choose_dep(conda_env, pip_env, conda_pkgs, pip_pkgs, pkg)

    # Always include python and pip if present
    choose_dep(conda_env, pip_env, conda_pkgs, pip_pkgs, 'python', required=False)
    choose_dep(conda_env, pip_env, conda_pkgs, pip_pkgs, 'pip', required=False)

    conda_pkgs = sorted(list(conda_pkgs))
    pip_pkgs = sorted(list(pip_pkgs))

    channels = env.pop('channels', [])

    if conda_pkgs:
        env['conda'] = {'channels': channels, 'dependencies': conda_pkgs}
        if pip_pkgs:
            pip_set = {'pip':pip_pkgs}
            env['conda']['dependencies'].append(pip_set)

    return env

def create_spec(filename, out=None, toplevel=False, function=None, pkg_mapping=[], extra_pkg=[]):

    overrides = {}
    for a in pkg_mapping:
        (i, n) = a.split('=')
        overrides[i] = n

    imports = []
    source = open(filename, 'r')
    code = ast.parse(source.read(), filename=filename)

    scan_pkgs()
    print('done scanning')

    if toplevel:
        imports += analyze_toplevel(code)
    elif function:
        imports += analyze_function(code, function)
    else:
        imports += analyze_full(code)

    env = export_env(overrides, imports, extra_pkg)

    return env  

# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
