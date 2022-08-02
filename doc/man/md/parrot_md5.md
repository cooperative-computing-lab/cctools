






















# parrot_md5(1)

## NAME
**parrot_md5** - returns the **MD5** checksum of a file, generated on the remote system if possible.

## SYNOPSIS
**parrot_md5 _&lt;path&gt;_**

## DESCRIPTION

**parrot_md5** returns the **MD5** checksum of the file stored at _&lt;path&gt;_.  If possible
it calls a native function of the remote system to get the checksum, without requiring the transfer
of the file's contents to the user's machine.
If the filesystem does not support the checksum function internally,
it is computed by the user-level program in the normal fashion.

## OPTIONS
**parrot_md5** has no options.

## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To retrieve the **MD5** checksum of a file stored on a **chirp** server:

```
% parrot_run parrot_md5 /chirp/server.nd.edu/joe/data
	d41d8cd98f00b204e9800998ecf8427e data
```


## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Parrot User Manual]("../parrot.html")
- [parrot_run(1)](parrot_run.md) [parrot_cp(1)](parrot_cp.md) [parrot_getacl(1)](parrot_getacl.md)  [parrot_setacl(1)](parrot_setacl.md)  [parrot_mkalloc(1)](parrot_mkalloc.md)  [parrot_lsalloc(1)](parrot_lsalloc.md)  [parrot_locate(1)](parrot_locate.md)  [parrot_timeout(1)](parrot_timeout.md)  [parrot_whoami(1)](parrot_whoami.md)  [parrot_mount(1)](parrot_mount.md)  [parrot_md5(1)](parrot_md5.md)  [parrot_package_create(1)](parrot_package_create.md)  [parrot_package_run(1)](parrot_package_run.md)  [chroot_package_run(1)](chroot_package_run.md)


CCTools
