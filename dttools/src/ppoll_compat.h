/*
Copyright (C) 2020- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PPOLL_COMPAT_H
#define PPOLL_COMPAT_H

#include <poll.h>

/** Wait for some event on file descriptors.
 *
 * While Linux provides ppoll() natively, other platforms like
 * like OSX don't, so this compatibility shim is necessary.
 * The interface differs somewhat: stoptime is given as an
 * int (for a 5 second timeout, pass time(NULL) + 5). In addition,
 * the signal set is assumed to be the empty set, i.e. all signals
 * are unblocked for the duration of the call. To break out on
 * receiving a signal, be sure it's not set to SIG_IGN. The normal
 * way to set up signals to work with ppoll is
 * 1) Block the signal.
 * 2) Install a signal handler (may be a no-op function).
 * 3) Call ppoll_compat(), which will unblock the signal only
 *    during the call.
 *
 * If ppoll() is detected at compile time, it will be used.
 * Otherwise, its behavior is emulated using regular poll().
 * Note that this fallback suffers from the race condition
 * described in select(2). It is therefore important not to rely
 * on signals for correctness (it is OK to use them as a fast path,
 * and in most cases the race condition probably won't appear).
 */
int ppoll_compat(struct pollfd fds[], nfds_t nfds, int stoptime);

/** Set up signal handling to ensure that SIGCHLD will interrupt ppoll_compat()
 */
void ppoll_compat_set_up_sigchld(void);

#endif
