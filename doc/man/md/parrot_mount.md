






















# parrot_mount(1)

## NAME
**parrot_mount** - mount new directories inside of a Parrot instance.

## SYNOPSIS
**parrot_mount _&lt;path&gt;_ _&lt;destination&gt;_ _&lt;permissions&gt;_**

## DESCRIPTION
**parrot_mount** utilizes **Parrot** system calls to change the namespace
of the parrot filesystem while it is running.  New mount points can be
added with read, write, or execute permissions, and existing mount points
can be removed.  The namespace can be locked down with the **--disable**
option, which prevents any further changes in the current session.


- **--unmount=_&lt;path&gt;_**<br /> Unmount a previously mounted path.
- **--disable**<br /> Disable any further mounting/unmounting in this parrot session.


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

Mount a remote Chirp filesystem as a read-only data directory:

```
% parrot_run bash
% parrot_mount /chirp/fs.somewhere.edu/data /data R
```

Umount the same directory:

```
parrot_mount --unmount /data
```

## COPYRIGHT
The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO

- [Cooperative Computing Tools Documentation]("../index.html")
- [Parrot User Manual]("../parrot.html")
- [parrot_run(1)](parrot_run.md) [parrot_cp(1)](parrot_cp.md) [parrot_getacl(1)](parrot_getacl.md)  [parrot_setacl(1)](parrot_setacl.md)  [parrot_mkalloc(1)](parrot_mkalloc.md)  [parrot_lsalloc(1)](parrot_lsalloc.md)  [parrot_locate(1)](parrot_locate.md)  [parrot_timeout(1)](parrot_timeout.md)  [parrot_whoami(1)](parrot_whoami.md)  [parrot_mount(1)](parrot_mount.md)  [parrot_md5(1)](parrot_md5.md)  [parrot_package_create(1)](parrot_package_create.md)  [parrot_package_run(1)](parrot_package_run.md)  [chroot_package_run(1)](chroot_package_run.md)


CCTools 7.3.2 FINAL
