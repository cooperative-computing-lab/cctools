# Poncho Packaging Utilities

The Poncho packaging utilities allow users to easily analyze their Python scripts and create Conda environments that are specifically built to contain the necessary dependencies required for their application to run. In distributed computing systems such as Work Queue, it is often difficult to maintain homogenous work environments for their Python applications, as the scripts utilize a large number of outside resources at runtime, such as Python interpreters and imported libraries. The Python packaging collection provides three easy-to-use tools that solve this problem, helping users to analyze their Python programs and build the appropriate Conda environments that ensure consistent runtimes within the Work Queue system. 

## Commands

- [`poncho_package_analyze`](../man_pages/poncho_package_analyze)
analyzes a Python script to determine all its top-level module dependencies and the interpreter version it uses. It then generates a concise, human-readable JSON output file containing the necessary information required to build a self-contained Conda virtual environment for the Python script.

- [`poncho_package_create`](../man_pages/poncho_package_create) takes a enviornment specification JSON file  and creates this Conda environment, preinstalled with all the necessary libraries and the correct Python interpreter version. It then generates a packaged tarball of the environment that can be easily relocated to a different machine within the system to run the Python task.

- [`poncho_package_run`](../man_pages/poncho_package_run) acts as a wrapper script for the Python task, unpacking and activating the Conda environment and running the task within the environment.

## Example

Suppose you have a Python program `example.py` like this:

```
import os
import sys
import pickle
import matplotlib


if __name__ == "__main__":
    print("example")
```

To analyze the `example.py` script for its dependencies:

```
poncho_package_analyze example.py package.json
```

This will create `package.json` with contents similar to this:


```
{
	"conda":{
		"channels":[
			"defaults"
			"conda-forge"
		]
		"packages":[
                	"python=3.8.5=h7579374_1"
	        	"matplotlib=3.3.4=py38h06a4308_0",
			"pip=20.2.4=py38h06a4308_0",
		]
	}
}
```

Then to create a complete package from the specification:

```
poncho_package_create package.json
```

The outputed tarball will be put into a directory named `envs` if a specific 
cache directory is not specified.

Once created, this package can be moved to another machine for execution.
Then, to run a program in the environment:

```
poncho_package_run -e package.tar.gz -- example.py
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

```
{
	"conda": {
		"channels":[
                        "defaults",
			"conda-forge" 
		],
		"packages":[
			"python=3.7",
			"numpy=1.20.0=py38h18fd61f_0",
			"ndcctools"
		]
	}
}
```

The `"conda"` key, if present, gives a list of channels and packages.
Each package must be a valid Conda package specification.


```
package=version=build
```

if coming from an existing user install. This is because the
particular build in use may affect performance and functionality
(e.g. whether GPU support is available).
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
	"pip": {
		"parsl",
		"ipython==7.20.0"
	}
}
```

The `"pip"` key, if present, gives a list of Pip package specifications.
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

```
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

```
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
