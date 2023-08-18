# Coding Style in CCTools

The core of CCTools is written in C, while we also have a number of
libraries and tools for end user access written in Python.  

No style guide will exhaustively cover all situations, and there
may be valid reasons to deviate from these suggestions.
Nonetheless, strive to match the existing style of the code,
for the benefit of readers that follow you

## C Style Guidelines

### C Module Design

C source code should be broken into modules, each of which defines a
set of related operations on a common structure that can be reused.
Keep the names of files, structures, and functions ruthlessly consistent.

For example, if you are defining a module for slicing cheese:
- The interface should be described in `cheese_slicer.h`
- The implementation should be given in `cheese_slicer.c`
- The object should be a `struct cheese_slicer`
- The functions operating on `cheese_slicer` should begin with the name `cheese_slicer` and accept a first argument of `struct cheese_slicer`
- The structure should be created by functions like this:
```
struct cheese_slicer * cheese_slicer_create( ... );
void                   cheese_slicer_delete( struct cheese_slicer *s );
```
- Functions and variables that are private to the module should be marked as `static`, not mentioned in the `.h` file, and may have names without the
module prefix.

### C Utility Modules

Please familiarize yourself with the [many utility modules](https://cctools.readthedocs.io/en/stable/api/html/taskvine__json_8h.html)
that serve as our extended "standard library".  Use them instead of rolling your own from scratch:

- Data Structures:
[set](https://cctools.readthedocs.io/en/stable/api/html/set_8h.html)
[list](https://cctools.readthedocs.io/en/stable/api/html/list_8h.html)
[hash_table](https://cctools.readthedocs.io/en/stable/api/html/hash_table_8h.html)
[itable](https://cctools.readthedocs.io/en/stable/api/html/itable_8h.html)
- Network Access:
[link (TCP)](https://cctools.readthedocs.io/en/stable/api/html/link_8h.html)
[datagram (UDP)](https://cctools.readthedocs.io/en/stable/api/html/datagram_8h.html)
[domain_name_cache (UDP)](https://cctools.readthedocs.io/en/stable/api/html/domain_name_cache_8h.html)
- File Manipulation:
[path](https://cctools.readthedocs.io/en/stable/api/html/path_8h.html)
[full_io](https://cctools.readthedocs.io/en/stable/api/html/full_io_8h.html)
[sort_dir](https://cctools.readthedocs.io/en/stable/api/html/sort_dir_8h.html)
[mkdir_recursive](https://cctools.readthedocs.io/en/stable/api/html/mkdir_recursive_8h.html)
[unlink_recursive](https://cctools.readthedocs.io/en/stable/api/html/unlink_recursive_8h.html)
[trash](https://cctools.readthedocs.io/en/stable/api/html/trash_8h.html)
- JX (JSON Extended) Manipulation:
[jx](https://cctools.readthedocs.io/en/stable/api/html/jx_8h.html)
[jx_parse](https://cctools.readthedocs.io/en/stable/api/html/jx_parse_8h.html)
[jx_print](https://cctools.readthedocs.io/en/stable/api/html/jx_print_8h.html)
[jx_eval](https://cctools.readthedocs.io/en/stable/api/html/jx_eval_8h.html)
- String Handling: 
[stringtools](https://cctools.readthedocs.io/en/stable/api/html/stringtools_8h.html)

In particular, we make extensive use of `string_format` to construct and dispose of variable length strings like this:
```
char *mydatapath = string_format("%s/%d/data",mypath,day);
...
free(mydatapath);
```

### C Indenting

Code should be indented like this using hard tabs:

```
int cheese_wheel_slice( struct cheese_wheel *c, int nslices )
{
	if( ... ) {
		while( ... ) {
			...
		}
	} else {
		...
	}
}
```

### C Indenting Configuration

If you use vim, the basic indenting setup is given by a comment at the bottom of each file:
```
/* vim: set noexpandtab tabstop=8: */
```

If you use emacs, put the following in `$HOME/.emacs.d/init.el` to achieve the same effect:

```
; Force indentation with a hard tab instead of spaces.
(setq-default indent-tabs-mode t)

; For display purposes, a tab is eight spaces wide
(setq-default tab-width 8)

; Each indentation level is one tab.
(defvaralias 'c-basic-offset 'tab-width)
```

## Python Style Guidelines

TBA

### Python Indenting Configuration

If you use vim, the basic indenting setup is given by a comment at the bottom of python source files:

```
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
```
