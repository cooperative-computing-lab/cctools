/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This product includes software developed by and/or derived
from the Globus Project (http://www.globus.org/)
to which the U.S. Government retains certain rights.
*/

/*
This file in particular was adapted from the Globus toolkit.
These functions encode binary data in base 64 ascii.  They
were adapted from similarly-named functions in globus_ftp_control_client.c
*/


#include "ftp_lite.h"
#include <string.h>

static char *radixN =
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static char pad = '=';

int ftp_lite_radix_encode( const unsigned char * inbuf, unsigned char * outbuf, int * length )
{
	int i;
	int j;
	unsigned char c=0;

	for (i=0,j=0; i < *length; i++)
	{
		switch (i%3)
		{
		case 0:
			outbuf[j++] = radixN[inbuf[i]>>2];
			c = (inbuf[i]&3)<<4;
			break;
		case 1:
			outbuf[j++] = radixN[c|inbuf[i]>>4];
			c = (inbuf[i]&15)<<2;
			break;
		case 2:
			outbuf[j++] = radixN[c|inbuf[i]>>6];
			outbuf[j++] = radixN[inbuf[i]&63];
			c = 0;
		}
	}

	if (i%3)
	{
		outbuf[j++] = radixN[c];
	}

	switch (i%3)
	{
	case 1:
		outbuf[j++] = pad;
		/* falls through */
	case 2:
		outbuf[j++] = pad;
		break;
	default:
		break;
	}

	outbuf[*length = j] = '\0';

	return 1;
}

int ftp_lite_radix_decode( const unsigned char *inbuf, unsigned char *outbuf, int * length )
{
	int i;
	int j;
	int D;
	char *p;

	for (i=0,j=0; inbuf[i] && inbuf[i] != pad; i++)
	{

		if ((p = strchr(radixN, inbuf[i])) == NULL)
		{
			return 0;
		}

		D = p - radixN;
		switch (i&3)
		{
		case 0:
			outbuf[j] = D<<2;
			break;
		case 1:
			outbuf[j++] |= D>>4;
			outbuf[j] = (D&15)<<4;
			break;
		case 2:
			outbuf[j++] |= D>>2;
			outbuf[j] = (D&3)<<6;
			break;
		case 3:
			outbuf[j++] |= D;
		}
	}
	switch (i&3)
	{
	case 1:
		return 0;
	case 2:
		if (D&15)
		{
			return 0;
		}
		if (strcmp((char *)&inbuf[i], "=="))
		{
			return 0;
		}
		break;
	case 3:
		if (D&3)
		{
			return 0;
		}
		if (strcmp((char *)&inbuf[i], "="))
		{
			return 0;
		}
	}
	*length = j;

	return 1;
}

/* vim: set noexpandtab tabstop=4: */
