# CCTools Documentation Source

There are three types of documentation present here:
- `manuals` contains the top-level documentation that appears at http://cctools.readthedocs.io
- `man` contains traditional man pages
- `api` contains API documentation generated from the source code

## User Manuals

The top level user manuals are meant to provide an overall introduction to the software,
explain the principles of operations, give examples for new users to follow, and serve
as a general reference.

**Local Build**. The manual is written using [mkdocs flavored markdown](https://www.mkdocs.org/user-guide/writing-your-docs/).
To build and test the documentation locally, run `mkdocs serve` in the `doc` directory,
which will compile the sources into HTML and start a local web server on `http://localhost:8000`.
You can then view the compiled manuals using your browser.

**Online Build**. When manual changes are pushed to github, they will be built and published automatically by the
[ReadTheDocs](http://readthedocs.io) service.  The configuration and status of the manual build
is at the [ReadTheDocs-CCTools project page](https://readthedocs.org/projects/cctools/).
Two different versions are built automatically within a few minutes of committing:
- [`stable`](http://cctools.readthedocs.io/en/stable) corresponds to whatever version in github is
tagged as `stable`.  This tag must be updated manually as part of our release process.
- [`latest`](http://cctools.readthedocs.io/en/latest) corresponds to the head of the `master` branch in github.
No action is needed to get a new deployment.

## Man Pages

Traditional Unix man pages are provided that describe each individual command that can be executed.
These are meant to be brief summaries that state the purpose of each tool, and exhaustively list
all options and environment variables.

The source for each man page is found in `man/m4` and written using the M4 macro language.
The `Makefile` runs `m4` on each source in order to generate the same text in several different formats:
- `md` for markdown documentation, which is copied to `manuals/man_pages` to become part of the online documentation
- `html` for traditional web pages, which is installed to `$PREFIX/doc` by `make install`
- `man` for traditional Unix nroff format, which is installed to $PREFIX/man by `make install`

## API Documentation

The API documentation is built by running `doxygen` on `api/cctools.doxygen.config` which scans
most of the C and Python source files looking for Doxygen-style comments.  This builds a set
of HTML pages which are then put into the directory `manual/api`.  The top level introductory page
is generated from `api/cctools.doxygen`.

## Combined Documentation

Note that both the traditional man pages and the API documentation (while useful separately)
are built and then copied into the user manual, so that they can easily be viewed online:
- API docs are built and copied into `manual/api` by `.readthedocs.yaml`
- Man pages are built and copied from `man/md` to `manual/man_pages` by `make`.

