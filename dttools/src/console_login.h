/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CONSOLE_LOGIN_H
#define CONSOLE_LOGIN_H

/** @file console_login.h
Read a name and password from the console.
*/

/** Read a name and password from the console.
This routine will set the console to no-echo mode,
carefully read a name and password, and then set the mode back.
@param service The name of the service to which the user is authenticating, such as a hostname.
@param name A pointer to a buffer to hold the user's name.
@param namelen The size of the name buffer in bytes.
@param pass A pointer to a buffer to hold the user's password.
@param passlen The size of the name buffer in bytes.
@return True if the name and password were successfully read, false otherwise.
*/
int console_login( const char *service, char *name, int namelen, char *pass, int passlen );

/** Read a string from the console.
This routine will set the console to no-echo mode,
carefully read a generic string, and then set the mode back.
@param prompt The prompt to be displayed on the command line.
@param buf A pointer to a buffer to hold the string.
@param buflen The size of the buffer in bytes.
@return True if the string was successfully read, false otherwise.
*/
int console_input( const char *prompt, char *buf, int buflen );

#endif
