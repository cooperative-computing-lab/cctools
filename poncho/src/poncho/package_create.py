#!/usr/bin/env python3

# Copyright (C) 2022 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os
import sys
import tempfile
import argparse
import subprocess
import json
import conda_pack
import pathlib
import hashlib
import shutil
import logging
import re
from platform import uname
from packaging import version

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter(fmt='%(asctime)s:%(levelname)s:%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

conda_exec = 'conda'

def _find_conda_executable(conda_executable, env_dir, download_micromamba=True):
    if conda_executable:
        return conda_executable

    candidate = shutil.which('mamba')
    if candidate:
        logger.info('found mamba')
        return (candidate, False)

    if 'CONDA_EXE' in os.environ:
        candidate = shutil.which(os.environ['CONDA_EXE'])
        if candidate:
            logger.info('found conda via CONDA_EXE')
            return (candidate, False)

    candidate = shutil.which('conda')
    if candidate:
        logger.info('found conda')
        return (candidate, False)

    candidate = shutil.which('micromamba')
    if candidate:
        logger.info('found micromamba')
        return (candidate, True)

    if download_micromamba:
        candidate = _download_micromamba(env_dir)
        if candidate:
            logger.info('installed micromamba')
            return (candidate, True)

    raise FileNotFoundError('could not find a working conda executable')


def _download_micromamba(env_dir):
    # mimics what src/install.sh does in micromamba release, but install
    # micromamba to a custom directory
    logger.info('downloading micromamba...')

    arch = uname().machine
    osys = uname().system

    if osys == "Linux":
        platform = "linux"
        if arch not in ["aarch64", "ppc64le"]:
            arch = "64"
    elif osys == "Darwin":
        platform = "osx"
        if arch not in ["arm64"]:
            arch = "64"

    os.mkdir(f"{env_dir}/bin")
    try:
        subprocess.run(f"curl -Ls https://micro.mamba.pm/api/micromamba/{platform}-{arch}/latest | tar xj -C {env_dir}/bin --strip-components=1 bin/micromamba",
                shell=True, check=True, capture_output=True)
    except subprocess.CalledProcessError:
        logger.error("could not install micromamba.")

    return os.path.join(env_dir, "bin", "micromamba")

def _copy_run_in_env(env_dir):
    candidate = shutil.which('poncho_package_run')
    if not candidate:
        logger.error('could not find poncho_package_run. Please add it to the PATH.')
        sys.exit(1)

    shutil.copy(candidate, f'{env_dir}/env/bin/poncho_package_run')
    with open(f'{env_dir}/env/bin/run_in_env', "w") as f:
        f.write('#! /bin/sh\n')
        f.write('env_dir=$(dirname $( cd -- "$( dirname -- "$0" )" > /dev/null 2>&1 && pwd ))\n')
        f.write('exec "${env_dir}"/bin/poncho_package_run -e ${env_dir} "$@"\n')
    os.chmod(f'{env_dir}/env/bin/run_in_env', 0o755)

def _pack_env_with_conda_dir(spec, output, ignore_editable_packages=False):
    # remove trailing slash if present
    spec = spec[:-1] if spec[-1] == '/' else spec 
    try:
        logger.info('packaging the environment...')
        os.makedirs(f'{spec}/bin/', exist_ok=True)
        os.makedirs(f'{spec}/env/bin/', exist_ok=True)
        _copy_run_in_env(spec)
        os.rename(f'{spec}/env/bin/poncho_package_run', f'{spec}/bin/poncho_package_run')
        os.rename(f'{spec}/env/bin/run_in_env', f'{spec}/bin/run_in_env')
        
        conda_pack.pack(prefix=f'{spec}', output=str(output), force=True, ignore_missing_files=True, ignore_editable_packages=ignore_editable_packages)
        logger.info('to activate environment run poncho_package_run -e {} <command>'.format(output))
        return output
    except Exception as e:
        raise Exception(f"Error when packing a conda directory.\n{e}")

def sort_spec(spec):
    spec = dict(sorted(spec.items()))
    for key in spec:
        if isinstance(spec[key], dict):
            spec[key] = sort_spec(spec[key])
        elif isinstance(spec[key], list):
            spec[key].sort()
    return spec
def dict_to_env(spec, conda_executable=None, download_micromamba=False, ignore_editable_packages=False, cache=True, cache_path=None, force=False):
    if not isinstance(spec, dict):
        spec = json.loads(spec)
    spec = sort_spec(spec)
    md5 = hashlib.md5()
    md5.update(str(spec).encode('utf-8'))
    output = "env-md5-" + md5.hexdigest() + ".tar.gz"
    if not cache_path:
        cache_path = '.poncho_cache'
    if not force and os.path.isfile(f'{cache_path}/{output}'):
        logger.info('Matching env found in cache...') 
        return f'{cache_path}/{output}'
    pack_env_from_dict(spec, output, conda_executable, download_micromamba, ignore_editable_packages)
    if cache:
        logger.info(f'copying env into cache at {cache_path}/{output}...') 
        if not os.path.exists(cache_path):
            os.makedirs(cache_path)
        shutil.copy(output,f'{cache_path}/{output}')
    return output

def pack_env_from_dict(spec, output, conda_executable=None, download_micromamba=False, ignore_editable_packages=False):
    # record packages installed as editable from pip
    local_pip_pkgs = _find_local_pip()

    with tempfile.TemporaryDirectory(prefix="poncho_env") as env_dir:
        logger.info('creating temporary environment in {}'.format(env_dir))

        global conda_exec
        (conda_exec, needs_confirmation) = _find_conda_executable(conda_executable, env_dir, download_micromamba)

        logger.info(f'using conda executable {conda_exec}')
        # creates conda spec file from poncho spec file
        logger.info('converting spec file...')
        conda_spec = create_conda_spec(spec, env_dir, local_pip_pkgs)

        # fetch data via git and https
        logger.info('fetching git data...')
        git_data(spec, env_dir)

        logger.info('fetching http data...')
        http_data(spec, env_dir)

        # create conda environment in temp directory
        logger.info('populating environment...')
        _run_conda_command(env_dir, needs_confirmation, 'env create', '--file', env_dir + '/conda_spec.yml')

        logger.info('adding local packages...')
        for (name, path) in conda_spec['pip_local'].items():
            _install_local_pip(env_dir, name, path)

        logger.info('copying spec to environment...')
        shutil.copy(f'{env_dir}/conda_spec.yml', f'{env_dir}/env/conda_spec.yml')

        _copy_run_in_env(env_dir)

        logger.info('generating environment file...')

        # Bug breaks bundling common packages (e.g. python).
        # ignore_missing_files may be safe to remove in the future.
        # https://github.com/conda/conda-pack/issues/145
        if ignore_editable_packages is not True:
            ignore_editable_packages = False
        conda_pack.pack(prefix=f'{env_dir}/env', output=str(output), force=True, ignore_missing_files=True, ignore_editable_packages=ignore_editable_packages)

        logger.info('to activate environment run poncho_package_run -e {} <command>'.format(output))

    return output

def pack_env(spec, output, conda_executable=None, download_micromamba=False, ignore_editable_packages=False):
    # pack a conda directory directly
    if os.path.isdir(spec):
        _pack_env_with_conda_dir(spec, output, ignore_editable_packages)

    # else if spec is a file or from stdin
    elif os.path.isfile(spec) or spec == '-':
        f = open(spec, 'r')
        poncho_spec = json.load(f)
        pack_env_from_dict(poncho_spec, output, conda_executable, download_micromamba, ignore_editable_packages)

    # else pack from a conda environment name
    # this thus assumes conda executable is in the current shell executable path
    else:
        conda_env_dir = _get_conda_env_dir_by_name(spec)
        _pack_env_with_conda_dir(conda_env_dir, output, ignore_editable_packages)

def _get_conda_env_dir_by_name(env_name):
    command = f"conda env list --json"
    result = subprocess.run(command, capture_output=True, text=True, shell=True)
    
    if result.returncode == 0:
        env_list = json.loads(result.stdout.strip())
        for env in env_list["envs"]:
            path = env.strip()
            name = path.split("/")[-1]
            if name == env_name:
                return path
    raise Exception(f'Cannot find the conda environment named {env_name}. Try checking the spelling or if conda is in your path.')

def _run_conda_command(environment, needs_confirmation, command, *args):
    all_args = [conda_exec] + command.split()
    if needs_confirmation:
        all_args.append('--yes')

    all_args = all_args + ['--prefix={}/env'.format(str(environment))] + list(args)

    try:
        subprocess.check_output(all_args)
    except subprocess.CalledProcessError as e:
        logger.warning("error executing: {}".format(' '.join(all_args)))
        print(e.output.decode())
        sys.exit(1)

def _find_local_pip():
    edit_raw = subprocess.check_output([sys.executable, '-m' 'pip', 'list', '--editable']).decode()

    # drop first two lines, which are just a header
    edit_raw = edit_raw.split('\n')[2:]

    path_of = {}
    for line in edit_raw:
        if not line:
            # skip empty lines
            continue
        # we are only interested in the path information of the package, which
        # is in the last column
        (pkg, version, location) = line.split()
        path_of[pkg] = location
    return path_of


def git_data(data, out_dir):

    if 'git' in data:
        for git_dir in data['git']:

            git_repo = None
            ref = None

            if 'remote' in data['git'][git_dir]:
                git_repo = data['git'][git_dir]['remote']
            if 'ref' in data['git'][git_dir]:
                ref = data['git'][git_dir]['ref']

            if git_repo:
                # clone repo
                path = '{}/{}'.format(out_dir, git_dir)

                subprocess.check_call(['git', 'clone', git_repo, path])

                if not os.path.exists(out_dir + '/poncho'):
                    os.mkdir(out_dir + '/poncho')

                # add to script
                gd = 'export {}=$1/{}\n'.format(git_dir, git_dir)
                with open(out_dir + '/poncho/set_env', 'a') as f:
                    f.write(gd)


def _install_local_pip(env_dir, pip_name, pip_path):
    logger.info("installing {} from editable pip".format(pip_path))
    # TODO GET pip version
    pip_exec = shutil.which('pip')
    process = subprocess.Popen([pip_exec, '-V'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    pip_version = out.decode('utf-8').split()[1]
    if version.parse(pip_version) < version.parse('22.1'):
        _run_conda_command(env_dir, False, 'run', 'pip', 'install', '--use-feature=in-tree-build', pip_path)
    else:
        _run_conda_command(env_dir, False, 'run', 'pip', 'install', pip_path)


def http_data(data, out_dir):

    if 'http' in data:
        for filename in data['http']:

            file_type = None
            compression = None
            url = None

            if 'type' in data['http'][filename]:
                file_type = data['http'][filename]['type']
            if 'compression' in data['http'][filename]:
                compression = data['http'][filename]['compression']
            if 'url' in data['http'][filename]:
                url = data['http'][filename]['url']

            if url:
                # curl datai
                path = '{}/{}'.format(out_dir, filename)

                if file_type == 'tar' and compression == 'gzip':
                    tgz = path + '.tar.gz'
                    subprocess.check_call(['curl', url, '--output', tgz])
                    os.mkdir(path)
                    subprocess.check_call(['tar', '-xzf', tgz, '-C', path])

                elif file_type == 'tar':
                    tar = path + '.tar'
                    subprocess.check_call(['curl', url, '--output', tar])
                    os.mkdir(path)
                    subprocess.check_call(['tar', '-xf', tar, '-C', path])

                elif compression == 'gzip':
                    gz = path + '.gz'
                    subprocess.check_call(['curl', url, '--output', gz])
                    subprocess.check_call(['gzip', '-d', gz])

                else:
                    subprocess.check_call(['curl', url, '--output', path])
                if not os.path.exists(out_dir + '/poncho'):
                    os.mkdir(out_dir + '/poncho')

                gd = 'export {}=$1/{}\n'.format(filename, filename)
                with open(out_dir + '/poncho/set_env', 'a') as f:
                    f.write(gd)


def create_conda_spec(poncho_spec, out_dir, local_pip_pkgs):

    conda_spec = {}
    conda_spec['channels'] = []
    conda_spec['dependencies'] = {}
    conda_spec['name'] = 'base'

    # packages in the spec that are installed in the current environment with
    # pip --editable
    local_reqs = set()

    if 'conda' in poncho_spec:

        conda_spec['channels'] = poncho_spec['conda'].get('channels', ['conda-forge', 'defaults'])

        if 'dependencies' in poncho_spec['conda']:

            conda_spec['dependencies'] = poncho_spec['conda'].get('dependencies', [])

            for dep in list(conda_spec['dependencies']):
                if isinstance(dep, dict) and 'pip' in dep:
                    for pip_dep in list(dep['pip']):
                        only_name = re.sub("[!~=<>].*$", "", pip_dep)  # remove possible version from spec
                        if only_name in local_pip_pkgs:
                            local_reqs.add(only_name)
                            dep['pip'].remove(pip_dep)
                else:
                    only_name = re.sub("[!~=<>].*$", "", dep)  # remove possible version from spec
                    if only_name in local_pip_pkgs:
                        local_reqs.add(only_name)
                        conda_spec['dependencies'].remove(dep)

            conda_spec['dependencies'] = list(conda_spec['dependencies'])
        # OLD FORMAT
        else:

            conda_spec['dependencies'] = set(poncho_spec['conda'].get('packages', []))

            for dep in list(conda_spec['dependencies']):
                only_name = re.sub("[!~=<>].*$", "", dep)  # remove possible version from spec
                if only_name in local_pip_pkgs:
                    local_reqs.add(only_name)
                    conda_spec['dependencies'].remove(dep)
            conda_spec['dependencies'] = list(conda_spec['dependencies'])


            pip_pkgs = set(poncho_spec.get('pip', []))

            for dep in list(pip_pkgs):
                only_name = re.sub("[!~=<>].*$", "", dep)  # remove possible version from spec
                if only_name in local_pip_pkgs:
                    local_reqs.add(only_name)
                    pip_pkgs.remove(dep)

            conda_spec['dependencies'].append({'pip': list(pip_pkgs)})


    for (pip_name, location) in local_pip_pkgs.items():
        if pip_name not in local_reqs:
            logger.warning("pip package {} was found as pip --editable, but it is not part of the spec. Ignoring local installation.".format(pip_name))

    with open(out_dir + '/conda_spec.yml',  'w') as jf:
        json.dump(conda_spec, jf, indent=4)
    with open('./conda_spec.yml',  'w') as jf:
        json.dump(conda_spec, jf, indent=4)

    # adding local pips to the spec after writing file, as conda complains of
    # unknown field.
    conda_spec['pip_local'] = {name:local_pip_pkgs[name] for name in local_reqs}

    return conda_spec
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
