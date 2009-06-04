/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef CATALOG_H
#define CATALOG_H

#include <stdlib.h>

#define CATALOG_HOST_DEFAULT "chirp.cse.nd.edu"
#define CATALOG_PORT_DEFAULT 9097

#define CATALOG_HOST (getenv("CATALOG_HOST") ? getenv("CATALOG_HOST") : CATALOG_HOST_DEFAULT ) 
#define CATALOG_PORT (getenv("CATALOG_PORT") ? atoi(getenv("CATALOG_PORT")) : CATALOG_PORT_DEFAULT ) 

#endif


