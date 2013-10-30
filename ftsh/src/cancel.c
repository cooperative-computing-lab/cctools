/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <signal.h>
#include <unistd.h>
#include <sys/time.h>
#include <time.h>

#include "ftsh_error.h"
#include "stringtools.h"

struct sigaction old_int_handler;
struct sigaction old_hup_handler;
struct sigaction old_quit_handler;
struct sigaction old_term_handler;

static int cancel_signal = 0;

static void cancel_handler( int sig )
{
	ftsh_error(FTSH_ERROR_PROCESS,0,"received signal %d",sig);
	cancel_signal = sig;
}

void cancel_hold()
{
	struct sigaction sa;
	sigset_t ss;

	sigemptyset(&ss);
	sa.sa_handler = cancel_handler;
	sa.sa_mask = ss;
	sa.sa_flags = 0;

	sigaction(SIGINT,&sa,&old_int_handler);
	sigaction(SIGHUP,&sa,&old_hup_handler);
	sigaction(SIGQUIT,&sa,&old_quit_handler);
	sigaction(SIGTERM,&sa,&old_term_handler);
}

void cancel_release()
{
	sigaction(SIGINT,&old_int_handler,0);
	sigaction(SIGHUP,&old_int_handler,0);
	sigaction(SIGQUIT,&old_int_handler,0);
	sigaction(SIGTERM,&old_int_handler,0);
	if(cancel_signal!=0) kill(getpid(),cancel_signal);
}

int cancel_pending()
{
	return cancel_signal!=0;
}

void cancel_reset()
{
	cancel_signal = 0;
}

/* vim: set noexpandtab tabstop=4: */
