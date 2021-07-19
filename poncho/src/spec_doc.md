Poncho Environment Specification Format
=======================================

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

Conda Packages
--------------

```
{
	"conda": [
		"defaults::python=3.7",
		"conda-forge::numpy=1.20.0=py38h18fd61f_0",
		"conda-forge::ndcctools"
	]
}
```

The `"conda"` key, if present, gives a list of strings.
Each string must be a valid Conda package specification.
Conda package specifications SHOULD use the full three-item
specifications,

```
channel::package=version=build
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
the user's `.condarc`. Each package specification MUST
specify a channel using the `channel::package` syntax.
The channel will likely be `conda-forge` for most packages.
If generating an Environment Specification from a user's existing
Conda prefix, be sure to capture the channel used.
When building a Conda prefix from an Environment Specification,
it should not be necessary to perform any channel configuration.

Pip Packages
------------

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
Due to the added difficulties in managing such dependencies,
such packages are outside the scope of this document.
It may be necessary to use another tool to
detect and prepare local Pip package installations.

Git Checkout
------------

```
{
	"git": {
		"DATA_DIR": {
			"remote": "http://.../repo.git",
			"tag": "abcd1234...."
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

HTTP Fetch
----------

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

----------------

Chached Imports

Management of keys/certificates
- Not sure about the most ergonomic way to do this
- Unless it's just pubkeys, not safe to include directly in the spec

Function Closures
- It would be possible (albeit inefficient given binary encoding in JSON)
  to include a pickled function + inputs in the same document, giving a
  portable Python function closure
- Also need specifications for resources
- Not sure if it's better to add those to the same document, or make
  Environment Specifications a field in another thing
