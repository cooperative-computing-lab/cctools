/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef CANCEL_H
#define CANCEL_H

/*
These are critical section handlers for termination
signals such as SIGTERM, SIGHUP, and so on.  Between
cancel_hold and cancel_release, these signals are
trapped and will not take effect.  After cancel_release,
the signal will be re-sent to the receiver unless
cancel_reset has been called.
*/

void cancel_hold();
void cancel_release();
int  cancel_pending();
void cancel_reset();

#endif
