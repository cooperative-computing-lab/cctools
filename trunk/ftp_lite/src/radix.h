/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef FTP_LITE_RADIX_H
#define FTP_LITE_RADIX_H

int ftp_lite_radix_decode( const unsigned char *inbuf, unsigned char *outbuf, int *length );
int ftp_lite_radix_encode( const unsigned char *inbuf, unsigned char *outbuf, int *length );

#endif
