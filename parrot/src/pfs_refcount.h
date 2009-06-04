/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef PFS_REFCOUNT_H
#define PFS_REFCOUNT_H

class pfs_refcount {
private:
	int numrefs;

public:
	pfs_refcount() {
		numrefs = 1;
	}

	void addref() {
		numrefs++;
	}

	void delref() {
		numrefs--;
	}

	int refs() {
		return numrefs;
	}
};


#endif
