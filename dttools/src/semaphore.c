#include <errno.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <unistd.h>

#include "debug.h"

#include "semaphore.h"

void semaphore_down(int s)
{
	if (s < 0)
		return;
	struct sembuf buf;
	buf.sem_num = 0;
	buf.sem_op = -1;
	buf.sem_flg = SEM_UNDO;
	semop(s, &buf, 1);
}

void semaphore_up(int s)
{
	if (s < 0)
		return;
	struct sembuf buf;
	buf.sem_num = 0;
	buf.sem_op = 1;
	buf.sem_flg = SEM_UNDO;
	semop(s, &buf, 1);
}

int semaphore_create(int value)
{
	int s = semget(IPC_PRIVATE, 1, 0600 | IPC_CREAT);
	if (s < 0) {
		debug(D_BATCH, "warning: couldn't create transfer semaphore (%s) but will proceed anyway", strerror(errno));
		return -1;
	}

	union semun {
		int value;
		struct semid_ds *buf;
		unsigned short *array;
	} arg;

	arg.value = value;

	semctl(s, 0, SETVAL, arg);
	return s;
}
