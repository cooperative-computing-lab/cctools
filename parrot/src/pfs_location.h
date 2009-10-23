/*
Copyright (C) 2009- Michael Albrecht and The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef PFS_LOCATION_H
#define PFS_LOCATION_H

extern "C" {
#include "list.h"
}

class pfs_location {
public:
	pfs_location();
	virtual ~pfs_location();

	virtual int append( const char *name );
	virtual int retrieve( char* buf, int len );

private:
	struct list *data;
};

void add_to_loc(const char *name, void *loc);

#endif
