/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef SORT_DIR_H
#define SORT_DIR_H

/** @file sort_dir.h Obtain a sorted directory listing.
    The prototype of @ref sort_dir is a little scary, but it is easy to use.
For example, to sort a given directory alphabetically:

<pre>
char **list;
int i;

sort_dir(dirname,&list,strcmp);

for(i=0;list[i];i++) printf("%s\n",list[i]);

sort_dir_free(list);
</pre>
*/

/** Obtain a sorted directory listing.
@param dirname The directory to list.
@param list A pointer to a doubly-indirect pointer, which will be filled with a list of strings.
The final item will be null.  This list must be freed with @ref sort_dir_free.
@param sort A pointer to a function to compare two strings, which must have the same semantics as <tt>strcmp</tt>
@return True on success, false on failure, setting errno appropriately.
*/
int sort_dir(const char *dirname, char ***list, int (*sort) (const char *a, const char *b));

/** Free a sorted directory listing.
@param list The list to be freed.
*/
#define sort_dir_free free

#endif
