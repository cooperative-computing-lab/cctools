/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "auth.h"
#include "debug.h"

int auth_encrypt_register()
{
	debug(D_AUTH,"encrypt: registered");
	return auth_register("encrypt",0,0);
}

