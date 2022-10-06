






















# parrot_whoami(1)

## NAME
**parrot_whoami** - returns the user's credentials (id and authentication method) from the perspective of the system being accessed.

## SYNOPSIS
**parrot_whoami _&lt;path&gt;_**

## DESCRIPTION

**parrot_whoami** interrogates the system being accessed at _&lt;path&gt;_ and returns the user's id
from the perspective of that system as well as the authentication method being used.  The specific
results depend on the system being accessed.

If _&lt;path&gt;_ is not provided the current working directory is used.

## OPTIONS

**parrot_whoami** has no options.

## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To get the user's credentials when accessing a remote chirp server:
```
% parrot_run parrot_whoami /chirp/server.nd.edu/joe_data/data
unix:joe
```

If you're working within a remote directory, _&lt;path&gt;_ is not necessary:
```
% parrot_run tcsh
% cd /chirp/server.nd.edu/joe_data/data
% parrot_whoami
unix:joe
% exit
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Parrot User Manual]("../parrot.html")
- [parrot_run(1)](parrot_run.md) [parrot_cp(1)](parrot_cp.md) [parrot_getacl(1)](parrot_getacl.md)  [parrot_setacl(1)](parrot_setacl.md)  [parrot_mkalloc(1)](parrot_mkalloc.md)  [parrot_lsalloc(1)](parrot_lsalloc.md)  [parrot_locate(1)](parrot_locate.md)  [parrot_timeout(1)](parrot_timeout.md)  [parrot_whoami(1)](parrot_whoami.md)  [parrot_mount(1)](parrot_mount.md)  [parrot_md5(1)](parrot_md5.md)  [parrot_package_create(1)](parrot_package_create.md)  [parrot_package_run(1)](parrot_package_run.md)  [chroot_package_run(1)](chroot_package_run.md)


CCTools
