/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "error.h"
#include <errno.h>

int ftp_lite_error( int r )
{
	if(r<=0) {
		return errno;
	} else if( r<100 || r>599 ) {
		return EINVAL;
	} else if( r<400 ) {
		return 0;
	} else switch(r) {
		case 421:
			return EPERM;
		case 425:
			return ECONNREFUSED;
		case 426:
			return EPIPE;
		case 450:
			return EBUSY;
		case 451:
			return EIO;
		case 452:
			return ENOSPC;
		case 500:
		case 501:
			return EINVAL;

		case 502:
			return ENOSYS;
		case 503:
			return EINVAL;
		case 504:
			return ENOSYS;
		case 530:
		case 532:
			return EACCES;
		case 550:
			return ENOENT;
		case 551:
			return EINVAL;
		case 552:
			return EDQUOT;
		case 553:
			return EACCES;
		default:
			return EINVAL;
	}
}

/* vim: set noexpandtab tabstop=8: */
