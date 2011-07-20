/*
Copyright (C) 2011- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef STRING_ARRAY_H
#define STRING_ARRAY_H

/** @file string_array.h Single Memory Block String Array.
    Allows the creation of string array inside a single memory block that can
    be therefore freed using free().  Pointers in the string array may move
    during calls to this library.
*/

/** Create a new empty string array.
 
    @return New string array.
  */
char **string_array_new (void);

/** Append str to the string array. It returns the new array which
    may have been relocated.

    @return The possibly relocated string array.
  */
char **string_array_append (char **oarray, char *str);

#endif /* STRING_ARRAY_H */
