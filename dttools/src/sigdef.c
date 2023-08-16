#include "sigdef.h"

#include <signal.h>
#include <string.h>

struct sigdef {
	char def[16];
	int signal;
};

static const struct sigdef sigdefs[] = {
	/* ANSI C signals */
#ifdef SIGABRT
	{"SIGABRT", SIGABRT},
#endif
#ifdef SIGFPE
	{"SIGFPE", SIGFPE},
#endif
#ifdef SIGILL
	{"SIGILL", SIGILL},
#endif
#ifdef SIGINT
	{"SIGINT", SIGINT},
#endif
#ifdef SIGSEGV
	{"SIGSEGV", SIGSEGV},
#endif
#ifdef SIGTERM
	{"SIGTERM", SIGTERM},
#endif
	/* posix signals */
#ifdef SIGHUP
	{"SIGHUP", SIGHUP},
#endif
#ifdef SIGQUIT
	{"SIGQUIT", SIGQUIT},
#endif
#ifdef SIGTRAP
	{"SIGTRAP", SIGTRAP},
#endif
#ifdef SIGKILL
	{"SIGKILL", SIGKILL},
#endif
#ifdef SIGUSR1
	{"SIGUSR1", SIGUSR1},
#endif
#ifdef SIGUSR2
	{"SIGUSR2", SIGUSR2},
#endif
#ifdef SIGPIPE
	{"SIGPIPE", SIGPIPE},
#endif
#ifdef SIGALRM
	{"SIGALRM", SIGALRM},
#endif
#ifdef SIGCHLD
	{"SIGCHLD", SIGCHLD},
#endif
#ifdef SIGCONT
	{"SIGCONT", SIGCONT},
#endif
#ifdef SIGSTOP
	{"SIGSTOP", SIGSTOP},
#endif
#ifdef SIGTSTP
	{"SIGTSTP", SIGTSTP},
#endif
#ifdef SIGTTIN
	{"SIGTTIN", SIGTTIN},
#endif
#ifdef SIGTTOU
	{"SIGTTOU", SIGTTOU},
#endif
	/* some BSD signals */
#ifdef SIGIOT
	{"SIGIOT", SIGIOT},
#endif
#ifdef SIGBUS
	{"SIGBUS", SIGBUS},
#endif
#ifdef SIGCLD
	{"SIGCLD", SIGCLD},
#endif
#ifdef SIGURG
	{"SIGURG", SIGURG},
#endif
#ifdef SIGXCPU
	{"SIGXCPU", SIGXCPU},
#endif
#ifdef SIGXFSZ
	{"SIGXFSZ", SIGXFSZ},
#endif
#ifdef SIGVTALRM
	{"SIGVTALRM", SIGVTALRM},
#endif
#ifdef SIGPROF
	{"SIGPROF", SIGPROF},
#endif
#ifdef SIGWINCH
	{"SIGWINCH", SIGWINCH},
#endif
#ifdef SIGPOLL
	{"SIGPOLL", SIGPOLL},
#endif
#ifdef SIGIO
	{"SIGIO", SIGIO},
#endif
	/* add odd signals */
#ifdef SIGSTKFLT
	{"SIGSTKFLT", SIGSTKFLT}, /* stack fault */
#endif
#ifdef SIGSYS
	{"SIGSYS", SIGSYS},
#endif
	{"", 0} /* sentinel */
};

const char *sigdefstr (int s)
{
	int i;
	for (i = 0; sigdefs[i].signal; i++) {
		if (s == sigdefs[i].signal) {
			return sigdefs[i].def;
		}
	}
	return "(Unknown)";
}

int sigdefint (const char *def)
{
	int i;
	for (i = 0; sigdefs[i].signal; i++) {
		if (strcmp(def, sigdefs[i].def) == 0) {
			return sigdefs[i].signal;
		}
	}
	return -1;
}

/* vim: set noexpandtab tabstop=8: */
