/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef PFS_CRITICAL_H
#define PFS_CRITICAL_H

#include <signal.h>

#define CRITICAL_BEGIN sigsetmask(sigmask(SIGIO)|sigmask(SIGINT)|sigmask(SIGHUP)|sigmask(SIGCHLD)|sigmask(SIGPIPE));
#define CRITICAL_END sigsetmask(sigmask(SIGPIPE));

#endif

