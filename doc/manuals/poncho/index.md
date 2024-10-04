# Poncho Packaging Utilities

The Poncho packaging utilities allow users to easily analyze their Python scripts and create Conda environments that are specifically built to contain the necessary dependencies required for their application to run. In distributed computing systems, it is often difficult to maintain homogenous work environments for their Python applications, as the scripts utilize a large number of outside resources at runtime, such as Python interpreters and imported libraries. The Python packaging collection provides three easy-to-use tools that solve this problem, helping users to analyze their Python programs and build the appropriate Conda environments that ensure consistent runtimes within the Work Queue system. 

## Commands

- [`poncho_package_analyze`](../man_pages/poncho_package_analyze.md)
analyzes a Python script to determine all its top-level module dependencies and the interpreter version it uses. It then generates a concise, human-readable JSON output file containing the necessary information required to build a self-contained Conda virtual environment for the Python script.

- [`poncho_package_create`](../man_pages/poncho_package_create.md) takes a enviornment specification JSON file  and creates this Conda environment, preinstalled with all the necessary libraries and the correct Python interpreter version. It then generates a packaged tarball of the environment that can be easily relocated to a different machine within the system to run the Python task.

- [`poncho_package_run`](../man_pages/poncho_package_run.md) acts as a wrapper script for the Python task, unpacking and activating the Conda environment and running the task within the environment.

## Example

Suppose you have a Python program `example.py` like this:

```python
import os
import sys
import pickle
import matplotlib
import numpy
import uproot


if __name__ == "__main__":
    print("example")
```

To analyze the `example.py` script for its dependencies:

```sh
poncho_package_analyze example.py package.json
```

This will create `package.json` with contents similar to this:


```json
{
    "conda": {
        "channels": [
            "conda-forge"
        ],
        "dependencies": [
            "matplotlib=3.7.1=py311h38be061_0",
            "numpy=1.24.2=py311h8e6699e_0",
            "pip=23.0.1=pyhd8ed1ab_0",
            "python=3.11.0=he550d4f_1_cpython",
            {
                "pip": [
                    "uproot==5.0.5"
                ]
            }
        ]
    }
}


```

Then to create a complete package from the specification:

```
poncho_package_create package.json package.tar.gz
```

Once created, this package can be moved to another machine for execution.
Then, to run a program in the environment:

```
poncho_package_run -e package.tar.gz -- python example.py
```

## Specification File

Environment Specifications are declarative JSON-encoded documents
defining software and/or data requirements for executing a particular
task. This format currently supports packages via Conda and Pip,
and data via Git and HTTP fetch of an archive file.
Poncho Environment Specifications primarily set up the global interpreter
state, including the module search path, `$PATH`, and certain environment
variables. The steps to prepare an environment
may be expensive (e.g. building a Conda prefix), so systems should
perform caching and intelligent management where possible.
In general, after preparing an environment once on a node
it should be possible to run additional tasks in the same environment
almost instantly (as long as the cache can hold everything).

This format is similar to the `environment.yml` file used by Conda,
but includes additional features for fetching arbitrary data,
handling certificates, and similar setup activities that are
commonly encountered when dealing with scientific applications.
It is also designed to work from a different perspective:
whereas Conda provides command-line utilities for interactively
manipulating a shell environment tied to the user's `.bashrc`,
Environment Specifications are designed as plumbing components
for executing individual pieces of Python code, and can set up
state *within* a Python interpreter.
This format is meant to be a self-contained and portable artifact,
so it should not depend on the user's configuration, absolue paths,
or system-specific details where possible.

### Conda Packages

```json

{
    "conda": {
        "channels": [
            "conda-forge"
        ],
        "dependencies": [
            "python=3.11.0=he550d4f_1_cpython",
            "numpy=1.24",
            "matplotlib",
            "pip",
            {
                "pip": [
                    "uproot==5.0.5"
                ]
            }
        ]
    },
}
```

The `"conda"` key, if present, gives a list of channels and packages.
Each package must be a valid Conda package specification, such as:


    - package: e.g., `python`
    - package and version: e.g., `python=3.11`
    - package, version, and build: e.g., `python=3.11=he550d4f_1_cpython`


Conda supports a fairly rich syntax, documented
[here](https://docs.conda.io/projects/conda/en/latest/user-guide/concepts/pkg-specs.html#package-match-specifications).
Implementers should use the `conda.models.match_spec.MatchSpec`
class for parsing and validation.
This class is part of the Conda package
(The library, not the command line tool.
It may be necessary to run `conda install conda`.)
Classes for version comparison are also available,
as Conda uses its own flavor of version numbering.

Note that Environment Specifications do not include any way to
specify the Conda channels a user has configured.
This is intentional,
since Environment Specification should not depend on
the user's `.condarc`. If generating an Environment Specification from a user's existing
Conda prefix, be sure to capture the channel used.
When building a Conda prefix from an Environment Specification,
it should not be necessary to perform any channel configuration.

### Pip Packages

```
{
	"pip": [
		"parsl",
		"ipython==7.20.0"
	]
}
```

The `"pip"` key is listed within the dpendencies list for conda packages. This is to reflect Conda's own specifaction style. It gives a list of Pip package specifications.
Pip specifications SHOULD include the exact version if coming from an
existing user install. Package specifications are given as strings,
which must conform to
[PEP 508](https://www.python.org/dev/peps/pep-0508/)
usually seen in Pip's `requirements.txt` file.
Note that not all allowed input in `requirements.txt` is allowed here,
e.g. `-e .` is not a valid package specification.
The Setuptools package includes a class for parsing requirements
[here](https://setuptools.readthedocs.io/en/latest/pkg_resources.html#requirement-objects)
along with version comparison and feature detection.
Implementors should use the `Requirement` class to validate
package specifications.

It is also sometimes necessary to work with locally available packages
(often installed via `pip install -e .`).
If a local pip package is listed within the specification, the pip package must be installed
into the user's current environment to be included.


### Git Repository

```json
{
	"git": {
		"DATA_DIR": {
			"remote": "http://.../repo.git",
		}
	}
}
```

Some tasks might depend on data from a particular Git repository.
When running many short-lived tasks, it is undesirable to fetch
a large amount of data each time. Here, a runtime system may download
a particular Git repository once and serve many tasks that depend on it.
Specifications SHOULD refer to specific commits, since branches are
subject to change over time.
In this example, the given repo would be cloned into a common temporary
directory, the path to which is stored in a enviornment variable,
e.g. calling `DATA_DIR = Path(/tmp/whatever)` in the task runner before
invoking the user's function.
Subsequent tasks that request the same repo can simply have
this variable set by the runner. The management layer may clear the repo
from its cache at any time, though it should make an effort to keep
frequently used repositories available for fast access.

### HTTP Fetch

```json
{
	"http": {
		"REFERENCE_DB": {
			"type": "file",
			"url": "https://.../example.dat"
		},
		"TRAINING_DATASET": {
			"type": "tar",
			"compression": "gzip",
			"url": "http://.../dataset.tar.gz"
		}
	}
}
```

HTTP fetch provides similar functionality to Git checkout, but
using a different transport. The URL is required and may be
HTTP or HTTPS. If a compression type is specified, the file
will be decompressed as part of the download process. If the
type is given as `"file"`, there is no further processing required
and the path of the downloaded file will be set as a environment
variable.

If type is `"tar"`, the (possibly decompressed)
file will be passed through `tar` and extracted into a temporary
directory (`tar -C`). The path to this directory will be stored
in the corresponding enviornment variable. If "compression" is
specified, the file will be decompressed.

## Creating Poncho Packages From Existing Conda Environments

If the input to poncho\_package\_create is the path to a conda env directory, poncho\_package\_run
will package that directory as a poncho package. If the input is neither a file or directory poncho\_package\_run 
will attempt to pack an environment of the same name from the users local conda environments.


## Creating Poncho Packages Within Python

The poncho module allows users to create poncho packages within python itself.

### creating packages with a specification

Poncho packages can either be created by dicitionary or string representations of a poncho specification.
The function `dict_to_env` creates the corresponding environment and returns the path to the environment.
The function contains various options to facilitate environment creation:
	
	- cache(default=True): caches the environment in the directory set by `cache_path` 
	- cache_path(default='.poncho_cache'): Path to cache and retrieve generated environments.
	- force(default=False): forces poncho_package_create to recreate the environment.

If no cache path is specified, cached environments will be stored in the directory `.poncho_cache`.
When force is not set to True and the environment corresponding to the specification is present in the cache,
the path to the cached environment will be returned.

```python

	from poncho import package_create

	spec1 = {"conda": {"channels": ["conda-forge"],"packages": ["python","pip","conda","conda-pack","dill","xrootd"]},"pip": ["matplotlib"]}
	env = package_create.dict_to_env(spec, cache=True, cache_path='my_cache', force=False)
```

