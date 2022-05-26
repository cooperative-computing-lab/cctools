/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_CRITICAL_H
#define PFS_CRITICAL_H

#include <signal.h>

#define CRITICAL_BEGIN {\
    sigset_t s;\
    sigaddset(&s, SIGIO);\
    sigaddset(&s, SIGINT);\
    sigaddset(&s, SIGHUP);\
    sigaddset(&s, SIGCHLD);\
    sigaddset(&s, SIGPIPE);\
    sigprocmask(SIG_SETMASK, &s, NULL);\
}

#define CRITICAL_END {\
    sigset_t s;\
    sigaddset(&s, SIGPIPE);\
    sigprocmask(SIG_SETMASK, &s, NULL);\
}

#endif
