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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CATCHUNIX(expr) \
	do {\
		if ((expr) == -1) {\
			perror(#expr);\
			exit(EXIT_FAILURE);\
		}\
	} while (0)

int sendfd(int sock, int fd)
{
  struct iovec data;
  data.iov_base = "foo";
  data.iov_len = strlen(data.iov_base);

  struct msghdr msg;
  msg.msg_name = NULL;
  msg.msg_namelen = 0;
  msg.msg_iov = &data;
  msg.msg_iovlen = 1;
  msg.msg_flags = 0;

  char control[CMSG_SPACE(sizeof(int)*2)] = "";
  msg.msg_control = control;
  msg.msg_controllen = sizeof(control);

  struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
  cmsg->cmsg_len = CMSG_LEN(sizeof(int)*2);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;

  *(int*)CMSG_DATA(cmsg) = fd;
  *((int*)CMSG_DATA(cmsg)+1) = 0;

  CATCHUNIX(sendmsg(sock, &msg, 0));

  return 0;
}


int recvfd(int sock)
{
	struct iovec iov;
	char buf[4096];
	iov.iov_base = buf;
	iov.iov_len = sizeof(buf);

	struct msghdr msg;
	char control[CMSG_SPACE(sizeof(int))];
	msg.msg_name = NULL;
	msg.msg_namelen = 0;
	msg.msg_iov = &iov;
	msg.msg_iovlen = 1;
	msg.msg_control = control;
	msg.msg_controllen = sizeof(control);

	CATCHUNIX(recvmsg (sock, &msg, 0));

	struct cmsghdr *cmsg;
	for (cmsg = CMSG_FIRSTHDR(&msg); cmsg; CMSG_NXTHDR(&msg, cmsg)) {
		if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS) {
			int fd = *((int *)CMSG_DATA(cmsg));
			return fd;
		}
	}

	return 0;
}

int main (int argc, char *argv[])
{
	int fds[2];
	CATCHUNIX(socketpair(AF_UNIX, SOCK_STREAM, 0, fds));
	pid_t child = fork();
	if (child == 0) {
		CATCHUNIX(close(fds[0]));
		int fd = recvfd(fds[1]);
		CATCHUNIX(write(fd, "hello", 5));
		CATCHUNIX(close(fd));
	} else if (child > 0) {
		CATCHUNIX(close(fds[1]));
		int fd = open("foo", O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);
		CATCHUNIX(fd);
		CATCHUNIX(unlink("foo"));
		CATCHUNIX(alarm(2));
		CATCHUNIX(sendfd(fds[0], fd));
		CATCHUNIX(wait(NULL));
	}
	return 0;
}
EOF
	return $?
}

run()
{
	parrot -- ./"$exe"
	return $?
}

clean()
{
	rm -f "$exe"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
