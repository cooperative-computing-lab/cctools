






















# parrot_mkalloc(1)

## NAME
**parrot_mkalloc** - create a directory with a space allocation

## SYNOPSIS
**parrot_mkalloc _&lt;path&gt;_ _&lt;size&gt;_**

## DESCRIPTION

**parrot_mkalloc** creates a new directory with a space allocation.
As the name suggests, the command only runs correctly inside of the
Parrot virtual file system, on file servers where space allocation is enabled.

The **path** argument gives the new directory to create, and
the **size** argument indicates how large the space allocation should be.
The latter may use metric units such as K, M, B, etc to indicate kilobytes,
megabytes, gigabytes, and so forth.

## OPTIONS


- **-h**<br />Show help text.



## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.
If the command is attempted on a filesystem that does not support
space allocation, it will give the following error:

```
parrot_mkalloc: This filesystem does not support allocations.
```

## EXAMPLES

To create a space allocation of ten gigabytes on a Chirp server:

```
% parrot_run bash
% cd /chirp/myserver.somewhere.edu
% parrot_mkalloc bigdir 10G
% exit
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO

- [Cooperative Computing Tools Documentation]("../index.html")
- [Parrot User Manual]("../parrot.html")
- [parrot_run(1)](parrot_run.md) [parrot_cp(1)](parrot_cp.md) [parrot_getacl(1)](parrot_getacl.md)  [parrot_setacl(1)](parrot_setacl.md)  [parrot_mkalloc(1)](parrot_mkalloc.md)  [parrot_lsalloc(1)](parrot_lsalloc.md)  [parrot_locate(1)](parrot_locate.md)  [parrot_timeout(1)](parrot_timeout.md)  [parrot_whoami(1)](parrot_whoami.md)  [parrot_mount(1)](parrot_mount.md)  [parrot_md5(1)](parrot_md5.md)  [parrot_package_create(1)](parrot_package_create.md)  [parrot_package_run(1)](parrot_package_run.md)  [chroot_package_run(1)](chroot_package_run.md)


CCTools
