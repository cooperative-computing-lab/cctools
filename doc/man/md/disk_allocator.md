






















# disk_allocator(1)

## NAME
**disk_allocator** - tool for creating and deleting loop device allocations of a given size.

## SYNOPSIS
**disk_allocator [options] _&lt;create|delete&gt;_ _&lt;target directory&gt;_ _&lt;size&gt;_ _&lt;filesystem&gt;_**

## DESCRIPTION

**disk_allcator** is a tool for creating and deleting loop device allocations
of a given size in order to sandbox an application. For creating an allocation,
it accepts a desired location for the device, the size for the sandbox, and the
filesystem to mount. For deleting an allocation, **disk_allocator** needs only
the directory of the mounted loop device which is to be removed.



You will need superuser priveleges to run **disk_allocator** on your local machine.



## OPTIONS

- **-h**,**--help**<br />Show this help screen.
- **-v**,**--version**<br />Show version string.


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

Create a disk allocation:
```
disk_allocator create /tmp/test 100MB ext2
```

Delete a disk allocation:
```
disk_allocator delete /tmp/test
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

CCTools
