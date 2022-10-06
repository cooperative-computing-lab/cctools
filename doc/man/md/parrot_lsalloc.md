






















# parrot_lsalloc(1)

## NAME
**parrot_lsalloc** - list current status of a space allocation

## SYNOPSIS
**parrot_lsalloc [path]**

## DESCRIPTION

**parrot_lsalloc** examines a given directory, determines if it is
contained within a space allocation, and then displays the allocation
size and the current usage. As the name suggests, the command only runs
correctly inside of the Parrot virtual file system, on file servers where space allocation is enabled.

The **path** argument gives the directory to examine.
If none is given, the current directory is assumed.

## OPTIONS


- **-h**<br />Show help text.



## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.
If the command is attempted on a filesystem that does not support
space allocation, it will give the following error:

```
parrot_lsalloc: This filesystem does not support allocations.
```

## EXAMPLES

To list a space allocation on a Chirp server:

```
% parrot_run bash
% cd /chirp/myserver.somewhere.edu
% parrot_lsalloc bigdir
/chirp/myserver.somewhere.edu/bigdir
10 GB TOTAL
1 GB INUSE
9 GB AVAIL
% exit
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Parrot User Manual]("../parrot.html")
- [parrot_run(1)](parrot_run.md) [parrot_cp(1)](parrot_cp.md) [parrot_getacl(1)](parrot_getacl.md)  [parrot_setacl(1)](parrot_setacl.md)  [parrot_mkalloc(1)](parrot_mkalloc.md)  [parrot_lsalloc(1)](parrot_lsalloc.md)  [parrot_locate(1)](parrot_locate.md)  [parrot_timeout(1)](parrot_timeout.md)  [parrot_whoami(1)](parrot_whoami.md)  [parrot_mount(1)](parrot_mount.md)  [parrot_md5(1)](parrot_md5.md)  [parrot_package_create(1)](parrot_package_create.md)  [parrot_package_run(1)](parrot_package_run.md)  [chroot_package_run(1)](chroot_package_run.md)


CCTools
