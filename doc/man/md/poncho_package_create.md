






















# poncho_package_create(1)

## NAME
**poncho_package_create** - command-line utility for creating a Conda virtual environment given a Python dependencies file

## SYNOPSIS

**poncho_package_create [options] _&lt;dependency-file&gt;_ _&lt;_&lt;output-path&gt;_&gt;_**
**poncho_package_create [options] _&lt;dependency-file&gt;_ _&lt;_&lt;output-path&gt;_&gt;_**

## DESCRIPTION

**poncho_package_create** is a simple command-line utility that creates a local Conda environment from an input JSON dependency file.
The command creates an environment tarball at output-path that can be sent to and run on different machines with the same architecture.

The **dependency-file** argument is the path (relative or absolute) to the a JSON specification file. The **output-path** argument specifies the path for the environment tarball that is created
(should usually end in .tar.gz).

## OPTIONS

- **-h**<br />        Show this help message

## EXIT STATUS

On success, returns zero. On failure, returns non-zero.

## EXAMPLE

**$ poncho_package_create dependencies.json example_venv.tar.gz**

This will create an example_venv.tar.gz environment tarball within the current working directory, which can then be exported to different machines for execution.

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

CCTools 8.0.0 DEVELOPMENT
