#!/bin/sh

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh

exe="socket.test"

prepare()
{
	gcc -I../src/ -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none -lm <<EOF
#include <unistd.h>
#include <fcntl.h>

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CATCHUNIX(expr) \\
	do {\\
		if ((expr) == -1) {\\
			fprintf(stderr, "[%s:%d] ", __FILE__, __LINE__);\\
			perror(#expr);\\
			unlink(argv[1]);\\
			exit(EXIT_FAILURE);\\
		}\\
	} while (0)

int main (int argc, char *argv[])
{
	struct sockaddr_un addr;
	addr.sun_family = AF_UNIX;
	if (argc != 2) {
		fprintf(stderr, "specify a socket path\n");
		return 1;
	}
	strcpy(addr.sun_path, argv[1]);
	pid_t child = fork();
	if (child == 0) {
		CATCHUNIX(alarm(2));
		int server = socket(AF_UNIX, SOCK_STREAM, 0);
		CATCHUNIX(server);
		unlink(addr.sun_path);
		CATCHUNIX(bind(server, (struct sockaddr *)&addr, sizeof(addr)));
		CATCHUNIX(listen(server, 1));
		int client = accept(server, NULL, NULL);
		CATCHUNIX(client);
		char buf[4096];
		ssize_t n = read(client, buf, sizeof(buf));
		CATCHUNIX(n);
		n = write(client, buf, n);
		CATCHUNIX(n);
		CATCHUNIX(close(client));
		CATCHUNIX(close(server));
		CATCHUNIX(unlink(argv[1]));
		exit(EXIT_SUCCESS);
	} else if (child > 0) {
		CATCHUNIX(alarm(2));
		int client = socket(AF_UNIX, SOCK_STREAM, 0);
		CATCHUNIX(client);
		while (connect(client, (struct sockaddr *)&addr, sizeof(addr)) == -1)
			if (!(errno == ENOENT || errno == ECONNREFUSED))
				CATCHUNIX(-1);
			else
				usleep(500);
		const char *str = "abcdefg";
		char buf[4096] = "";
		strcpy(buf, str);
		CATCHUNIX(write(client, buf, strlen(buf)));
		memset(buf, 0, sizeof(buf));
		CATCHUNIX(read(client, buf, sizeof(buf)));
		CATCHUNIX(strcmp(buf, str) == 0 ? 0 : (errno = EINVAL, -1));
		CATCHUNIX(wait(NULL));
	}
	return 0;
}
EOF
	return $?
}

run()
{
	set -e
	parrot -- "$(pwd)/$exe" /tmp/foo
	parrot -- "$(pwd)/$exe" /http/foo && return 1
	return 0
}

clean()
{
	rm -f "$exe" /tmp/foo
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
