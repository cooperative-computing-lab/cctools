/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef USERNAME_H
#define USERNAME_H

/** @file username.h
Obtain information about the current user.
*/

/** The maximum size of a user name. */ 
#define USERNAME_MAX 256

/** Determine if the current user is the super user.
@return True if the current user is root, false otherwise.
*/

int username_is_super();

/** Get the name of the current user.
@param name A string of @ref USERNAME_MAX bytes to hold the username.
@return True if the username could be found, false otherwise.
*/
int username_get( char *name );

/** Switch to the named user.
@param name The name of the user to change privilege to.
@return True if the privilege could be changed, false otherwise.
*/
int username_set( const char *name );

/** Get the current user's home directory.
@param dir A string to hold the home directory.
@return True if the home directory could be found, false otherwise.
*/
int username_home( char *dir );

#endif
