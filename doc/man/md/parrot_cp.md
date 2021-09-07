






















# parrot_cp(1)

## NAME
**parrot_cp** - a replacement for **cp** that provides higher performance when dealing
with remote files via **parrot_run**.

## SYNOPSIS
**parrot_cp [options] ... sources ... _&lt;dest&gt;_**

## DESCRIPTION

**parrot_cp** is a drop-in replacement for the Unix **cp** command.
It provides better performance when copying files to or from remote storage
systems by taking advantage of whole-file transfer rather than copying files
block-by-block.

## OPTIONS


- **-f**,**--force**<br />Forcibly remove target before copying.
- **-i**,**--interactive**<br />Interactive mode: ask before overwriting.
- **-r**<br /> Same as -R
- **-R**,**--recursive**<br />Recursively copy directories.
- **-s**,**--symlinks**<br />Make symbolic links instead of copying files.
- **-l**,**--hardlinks**<br />)Make hard links instead of copying files.
- **-u**,**--update-only**<br />Update mode: Copy only if source is newer than target.
- **-v**,**--version**<br />Verbose mode: Show names of files copied.
- **-h**,**--help**<br />Help: Show these options.


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To use parrot_cp you can either call the code directly:

```
% parrot_run tcsh
% parrot_cp /tmp/mydata /chirp/server.nd.edu/joe/data
% exit
```

or alias calls to **cp** with calls to **parrot_cp**:

```
% parrot_run bash
% alias cp parrot_cp
% cp -r /chirp/server.nd.edu/joe /tmp/joe
% exit
```


## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Parrot User Manual]("../parrot.html")
- [parrot_run(1)](parrot_run.md) [parrot_cp(1)](parrot_cp.md) [parrot_getacl(1)](parrot_getacl.md)  [parrot_setacl(1)](parrot_setacl.md)  [parrot_mkalloc(1)](parrot_mkalloc.md)  [parrot_lsalloc(1)](parrot_lsalloc.md)  [parrot_locate(1)](parrot_locate.md)  [parrot_timeout(1)](parrot_timeout.md)  [parrot_whoami(1)](parrot_whoami.md)  [parrot_mount(1)](parrot_mount.md)  [parrot_md5(1)](parrot_md5.md)  [parrot_package_create(1)](parrot_package_create.md)  [parrot_package_run(1)](parrot_package_run.md)  [chroot_package_run(1)](chroot_package_run.md)


CCTools 7.3.2 FINAL
