/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "console_login.h"
#include "stringtools.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <termios.h>

static int setecho(int fd, int onoff)
{
	struct termios term;
	if(tcgetattr(fd, &term) < 0)
		return 0;
	if(onoff) {
		term.c_lflag |= ECHO;
	} else {
		term.c_lflag &= ~(ECHO);
	}
	if(tcsetattr(fd, TCSANOW, &term) < 0)
		return 0;
	return 1;
}

static int do_getline(const char *prompt, char *buffer, int length, int echo)
{
	int fd;
	char *result;
	FILE *stream;

	fd = open("/dev/tty", O_RDWR);
	if(fd < 0)
		return 0;

	if(!echo) {
		if(!setecho(fd, 0)) {
			close(fd);
			return 0;
		}
	}

	stream = fdopen(fd, "r+");
	if(!stream) {
		if(!echo) {
			setecho(fd, 1);
		}
		close(fd);
		return 0;
	}

	fprintf(stream, "%s", prompt);
	fflush(stream);

	result = fgets(buffer, length, stream);

	string_chomp(buffer);

	if(!echo) {
		fprintf(stream, "\n");
		fflush(stream);
		setecho(fd, 1);
	}

	fclose(stream);

	return result != 0;
}

int console_login(const char *service, char *name, int namelen, char *pass, int passlen)
{
	char *prompt;

	prompt = malloc(strlen(service) + 10);
	if(!prompt)
		return 0;

	sprintf(prompt, "%s login: ", service);

	int result = do_getline(prompt, name, namelen, 1) && do_getline("password: ", pass, passlen, 0);

	free(prompt);

	return result;
}

int console_input(const char *prompt, char *buf, int buflen)
{
	return do_getline(prompt, buf, buflen, 0);
}

/* vim: set noexpandtab tabstop=8: */
