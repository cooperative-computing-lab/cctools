






















# parrot_timeout(1)

## NAME
**parrot_timeout** - changes or resets the main timeout for the current **parrot** session

## SYNOPSIS
**parrot_timeout _&lt;time&gt;_**

## DESCRIPTION

**parrot_timeout** changes the main timeout for the current **parrot** session to
_&lt;time&gt;_.  If _&lt;time&gt;_ was not given, it resets it to the default value (5 minutes if
an interactive session or 1 hour for a non-interactive session).

## OPTIONS

**parrot_timeout** has no options.

## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To change the manager timeout to 5 hours:

```
% parrot_run tcsh
% parrot_timeout 5h
% ./my_executable
% exit
```

To change it to 30 seconds for one program and then reset it to the default value
```
% parrot_run tcsh
% parrot_timeout 40m
% ./my_executable
% parrot_timeout
% ./my_second_executable
% exit
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Parrot User Manual]("../parrot.html")
- [parrot_run(1)](parrot_run.md) [parrot_cp(1)](parrot_cp.md) [parrot_getacl(1)](parrot_getacl.md)  [parrot_setacl(1)](parrot_setacl.md)  [parrot_mkalloc(1)](parrot_mkalloc.md)  [parrot_lsalloc(1)](parrot_lsalloc.md)  [parrot_locate(1)](parrot_locate.md)  [parrot_timeout(1)](parrot_timeout.md)  [parrot_whoami(1)](parrot_whoami.md)  [parrot_mount(1)](parrot_mount.md)  [parrot_md5(1)](parrot_md5.md)  [parrot_package_create(1)](parrot_package_create.md)  [parrot_package_run(1)](parrot_package_run.md)  [chroot_package_run(1)](chroot_package_run.md)


CCTools 7.3.2 FINAL
