






















# makeflow_linker(1)

## NAME
**makeflow_linker** - automatic dependency location for workflows

## SYNOPSIS
**makeflow_linker [options] _&lt;workflow_description&gt;_**

## DESCRIPTION
**makeflow_linker** is a tool for automatically determining dependencies of workflows. It accepts a workflow description, currently Makeflow syntax is required, and recursively determines the dependencies and produces a self-contained package. **makeflow_linker** supports Python, Perl, and shared libraries.



**makeflow_linker** finds dependencies by static analysis. **eval** and other dynamic code loading may obscure dependencies causing **makeflow_linker** to miss some critical dependencies. Therefore it is recommended to avoid these techniques when desiging a workflow.

## OPTIONS

- **--dry-run**<br />Run without creating directories or copying dependencies.
- **-h**,**--help**<br />Show this help screen.
- **-n**,**--use-named**<br />Do not copy files which are part of a named dependency, e.g. standard libraries.
- **-o**,**--output=_&lt;directory&gt;_**<br />Specify output directory.
- **--verbose**<br />Output status during run.
- **-v**,**--version**<br />Display version information.


## EXIT STATUS
On success, returns zero. On failure (typically permission errors), returns non-zero.

## BUGS

- The makeflow_linker does not check for naming collisions beyond the initial workflow inputs.
- The makeflow_linker relies on regex parsing of files


## EXAMPLES

Package a workflow:
```
makeflow_linker -o example_mf example.mf
```

Run packaged workflow:
```
makeflow example_mf/example.mf
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


-  [makeflow(1)](makeflow.md) perl(1), python(1), ldd(1)


CCTools 7.3.2 FINAL
