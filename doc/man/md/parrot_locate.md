






















# parrot_locate(1)

## NAME
**parrot_locate** - provides the true location of the data stored in a named file.

## SYNOPSIS
**parrot_locate path**

## DESCRIPTION

**parrot_locate** utilises **parrot** system calls to identify where the data stored as the file
at **_&lt;path&gt;_** is actually located.  For example, running **parrot_locate** on a file stored in a
**chirp** multi-volume will return the server name and file path on the server where the data is.
Running it on a file stored in **hdfs** will return the list of chunk servers storing the file.

Note that **parrot_locate** varies depending on the underlying system.  Most systems return output
in the form "**_&lt;server&gt;_:_&lt;real path&gt;_**", but that output is not guaranteed.


## OPTIONS

**parrot_locate** has no options.


## ENVIRONMENT VARIABLES
Environment variables required by **parrot_locate** are system dependent.
Most systems do not use or require any.  Refer to the specific system's documentation
for more information.


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To check the location of a file stored in chirp:

```
% parrot_run parrot_locate /chirp/server.nd.edu/joe/data
	server.nd.edu:/chirp/server.nd.edu/joe/data
```

or a file stored in a chirp multi-volume

```
% parrot_run parrot_locate /multi/server.nd.edu@multivol/data
	datastore01.nd.edu:multivol/data/ttmtteotsznxewoj
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Parrot User Manual]("../parrot.html")
- [parrot_run(1)](parrot_run.md) [parrot_cp(1)](parrot_cp.md) [parrot_getacl(1)](parrot_getacl.md)  [parrot_setacl(1)](parrot_setacl.md)  [parrot_mkalloc(1)](parrot_mkalloc.md)  [parrot_lsalloc(1)](parrot_lsalloc.md)  [parrot_locate(1)](parrot_locate.md)  [parrot_timeout(1)](parrot_timeout.md)  [parrot_whoami(1)](parrot_whoami.md)  [parrot_mount(1)](parrot_mount.md)  [parrot_md5(1)](parrot_md5.md)  [parrot_package_create(1)](parrot_package_create.md)  [parrot_package_run(1)](parrot_package_run.md)  [chroot_package_run(1)](chroot_package_run.md)


CCTools
