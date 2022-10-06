






















# parrot_getacl(1)

## NAME
**parrot_getacl** - get ACL information for Parrot filesystem

## SYNOPSIS
**parrot_getacl _&lt;path&gt;_**

## DESCRIPTION
**parrot_getacl** utilizes **Parrot** system calls to retrieve the access
control list (ACL) information for the filesystem located at the specified
_&lt;path&gt;_.

Note, this program only works if it is executed under **Parrot** and if the
underlying filesystem supports ACLs.

## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES
Get ACL for **Chirp** directory:

```
% parrot_run parrot_getacl /chirp/student00.cse.nd.edu/
```

## COPYRIGHT
The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO

- [Cooperative Computing Tools Documentation]("../index.html")
- [Parrot User Manual]("../parrot.html")
- [parrot_run(1)](parrot_run.md) [parrot_cp(1)](parrot_cp.md) [parrot_getacl(1)](parrot_getacl.md)  [parrot_setacl(1)](parrot_setacl.md)  [parrot_mkalloc(1)](parrot_mkalloc.md)  [parrot_lsalloc(1)](parrot_lsalloc.md)  [parrot_locate(1)](parrot_locate.md)  [parrot_timeout(1)](parrot_timeout.md)  [parrot_whoami(1)](parrot_whoami.md)  [parrot_mount(1)](parrot_mount.md)  [parrot_md5(1)](parrot_md5.md)  [parrot_package_create(1)](parrot_package_create.md)  [parrot_package_run(1)](parrot_package_run.md)  [chroot_package_run(1)](chroot_package_run.md)


CCTools
