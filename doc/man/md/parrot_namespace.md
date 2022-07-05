






















# parrot_namespace(1)

## NAME
**parrot_namespace** - run a command in a modified namespace.

## SYNOPSIS
**parrot_cp [options] _&lt;command&gt;_**

## DESCRIPTION

**parrot_namespace** detects whether it is already running under Parrot
and either forks a new mount namespace in the existing Parrot session or
simply executes **parrot_run**. For applications that only need to make
mount-related changes, **parrot_namespace** is a drop-in replacement
for **parrot_run** that automatically handles nested invocations.

## OPTIONS


- **-M**,**--mount=_&lt;/foo=/bar&gt;_**<br />Mount (redirect) _&lt;/foo&gt;_ to _&lt;/bar&gt;_ (**PARROT_MOUNT_STRING**)
- **-m**,**--ftab-file=_&lt;path&gt;_**<br />Use _&lt;path&gt;_ as a mountlist (**PARROT_MOUNT_FILE**)
- **-l**,**--ld-path=_&lt;path&gt;_**<br />Path to ld.so to use.
- **--parrot-path**<br />Path to **parrot_run** (**PARROT_PATH**)
- **-v**,**--version**<br />Show version number
- **-h**,**--help**<br />Help: Show these options


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To run Parrot under Parrot with a modified mount environment,
use **parrot_namespace**

```
% parrot_namespace -M /tmp=/tmp/job01 sh
% parrot_mount --unmount /tmp    # not allowed
```

Now in the same shell, we can call **parrot_namespace** regardless
of whether we're already running under Parrot or not.

```
% parrot_namespace -m mountfile foo
```


## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Parrot User Manual]("../parrot.html")
- [parrot_run(1)](parrot_run.md) [parrot_cp(1)](parrot_cp.md) [parrot_getacl(1)](parrot_getacl.md)  [parrot_setacl(1)](parrot_setacl.md)  [parrot_mkalloc(1)](parrot_mkalloc.md)  [parrot_lsalloc(1)](parrot_lsalloc.md)  [parrot_locate(1)](parrot_locate.md)  [parrot_timeout(1)](parrot_timeout.md)  [parrot_whoami(1)](parrot_whoami.md)  [parrot_mount(1)](parrot_mount.md)  [parrot_md5(1)](parrot_md5.md)  [parrot_package_create(1)](parrot_package_create.md)  [parrot_package_run(1)](parrot_package_run.md)  [chroot_package_run(1)](chroot_package_run.md)


CCTools
