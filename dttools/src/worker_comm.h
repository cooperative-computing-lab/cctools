#include "link.h"
#include "list.h"

#include <mpi.h>


#define WORK_QUEUE_LINE_MAX 1024

struct worker_comm {
	int type;
	int mpi_rank;
	int active_timeout;
	int short_timeout;
	int results;
	char *hostname;
	struct link *lnk;
	MPI_Request mpi_req;
	MPI_Status mpi_stat;
};

struct worker_op {
	int type;
	int jobid;
	int id;
	int options;
	int flags;
	int payloadsize;
	
	char name[WORK_QUEUE_LINE_MAX];
	char *payload;
};

#define WORKER_COMM_TCP 1
#define WORKER_COMM_MPI 2

#define WORKER_COMM_ARRAY_CHAR		1
#define WORKER_COMM_ARRAY_INT		2
#define WORKER_COMM_ARRAY_FLOAT	3
#define WORKER_COMM_ARRAY_DOUBLE	4


struct list * worker_comm_accept_connections(int interface, struct link *master_link, int active_timeout, int short_timeout);

struct worker_comm * worker_comm_connect(struct worker_comm *comm, int interface, const char *hostname, int port_id, int active_timeout, int short_timeout);

void worker_comm_disconnect(struct worker_comm *comm);

void worker_comm_delete(struct worker_comm *comm);

int worker_comm_send_id(struct worker_comm *comm, int id, const char *hostname);

int worker_comm_send_array(struct worker_comm *comm, int datatype, void* buf, int length);

int worker_comm_recv_array(struct worker_comm *comm, int datatype, void* buf, int length);

int worker_comm_send_buffer(struct worker_comm *comm, char *buffer, int length, char header);

int worker_comm_send_file(struct worker_comm *comm, const char *filename, int length, char header);

int worker_comm_recv_buffer(struct worker_comm *comm, char **buffer, int *bufferlength, char header);

int worker_comm_send_op(struct worker_comm *comm, struct worker_op *op);

int worker_comm_receive_op(struct worker_comm *comm, struct worker_op *op); 

int worker_comm_test_results(struct worker_comm *comm);


