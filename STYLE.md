# Coding Style in CCTools

The core of CCTools is written in C, while we also have a number of
libraries and tools for end user access written in Python.  

No style guide will exhaustively cover all situations, and there
may be valid reasons to deviate from these suggestions.
Nonetheless, strive to match the existing style of the code,
for the benefit of readers that follow you

# C Style Guidelines

## C Module Design

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

# Python Style Guidelines

TBA

### Python Indenting Configuration

If you use vim, the basic indenting setup is given by a comment at the bottom of python source files:

```
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
```
