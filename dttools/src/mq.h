/*
Copyright (C) 2020- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MQ_H
#define MQ_H

#include <stddef.h>
#include <time.h>

#include "buffer.h"

/** @file mq.h Non-blocking message-oriented queue for network communication.
 *
 * This module provides ordered, message-oriented semantics and
 * queuing over the network. See @ref link.h for lower-level
 * socket communication.
 *
 * Rather than calling send() or recv() and waiting for the other side,
 * messages are asynchronously placed in send and receive queues.
 * To send a message, simply append it to the send queue. Likewise
 * received messages are put in the receive queue and can be popped
 * at the application's convenience.
 *
 * Pushing a message onto the send queue is a constant-time operation,
 * since nothing is actually sent over the network. To trigger real
 * network communication, it is necessary regularly call
 * @ref mq_flush_send and @ref mq_flush_recv. These are non-blocking
 * operations guaranteed to return quickly, but they only transfer as
 * much as the socket buffers allow. It may take many flush calls to
 * complete a large transfer. Socket connect and accept are non-blocking
 * as well. Thus a server will probably call @ref mq_accept() as part
 * of its main loop. The only blocking operations are @ref mq_wait
 * and @ref mq_poll_wait, which block until waiting message(s)/connection(s)
 * become available (or a signal handler or timeout interrupts).
 *
 * The polling interface loosely approximates the Linux epoll interface.
 * First, a set of message queues are added to a polling group with
 * @ref mq_poll_add. Then a call to @ref mq_poll_wait blocks until
 * at least one of the queues in the group has message(s)/connection(s)
 * ready. @ref mq_poll_wait calls @ref mq_flush_send and
 * @ref mq_flush_receive internally, so the event loop does not need to.
 * Send buffers will be flushed as much as possible while waiting for
 * messages/connections. Helper functions (@ref mq_poll_readable,
 * @ref mq_poll_acceptable, @ref mq_poll_error) are available to
 * efficiently find queues with messages/connections available.
 *
 * The examples that follow are lazy about checking for errors,
 * be sure to check carefully!
 *
 * Example Client:
 *
 *	struct mq *mq = mq_connect("127.0.0.1", 1234);
 *	mq_send(mq, msg);
 *	while (true) {
 *		switch (mq_wait(mq, time() + 30)) {
 *			case 0:
 *			// interrupted by timeout or signal
 *			case -1:
 *			// error or closed socket, mq should be deleted
 *			default:
 *			// got some messages!
 *			mq_recv(mq, &buf);
 *			// process the result
 *			// make sure something breaks out of the loop!
 *		}
 *	}
 *	mq_close(mq);
 *
 * Example Server:
 *
 *	struct mq *server = mq_serve(NULL, 1234);
 *	while (true) {
 *		switch (mq_wait(server, time() + 30)) {
 *			case 0:
 *			// interrupted by timeout or signal
 *			case -1:
 *			// error or closed socket, mq should be deleted
 *			default:
 *			// got connections!
 *			handle_client(mq_accept(server));
 *		}
 *	}
 *	mq_close(server);
 *
 * Example Polling Server:
 *
 *	struct mq_poll *M = mq_poll_create();
 *	struct mq *server = mq_serve(NULL, 1234);
 *	mq_poll_add(server, NULL);
 *
 *	while (true) {
 *		switch (mq_poll_wait(M, time() + 30)) {
 *			case 0:
 *			// interrupted by timeout or signal
 *			case -1:
 *			// error, time to abort
 *			default: {
 *				struct mq *mq;
 *				if ((mq = mq_poll_acceptable(M))) {
 *					// got a new connection
 *					struct mq *n = mq_accept(mq);
 *					mq_poll_add(n, NULL);
 *					setup_client(n);
 *				}
 *				if ((mq = mq_poll_readable(M))) {
 *					// got a new message
 *					handle_client(mq);
 *				}
 *			}
 *		}
 *	}
 *	// make sure all client connections were closed
 *	mq_poll_delete(M);
 *	mq_close(server);
 */


typedef enum {
	MQ_MSG_NONE = 0,
	MQ_MSG_NEWBUFFER,
	MQ_MSG_BUFFER,
	//MQ_MSG_FILE,
	//MQ_MSG_PROCESS,
} mq_msg_t;


//---------Basic Functions----------------//

/** Connect to a remote host.
 *
 * Returns immediately, with sends buffered until the connection actually
 * completes. If the connection fails, mq_flush_* will return -1.
 * @param addr IP address of the server in string form.
 * @param port Port of the server.
 * @returns A pointer to the new message queue.
 * @returns NULL on failure, with errno set appropriately.
 */
struct mq *mq_connect(const char *addr, int port);

/** Prepare to accept connections.
 *
 * The server socket will listen on the first available port between
 * low and high. Use @ref mq_accept to get client connections.
 * @param addr IP address of the network interface,
 *  or NULL to accept connections on any interface.
 * @param low The low port in a range to listen on (inclusive).
 * @param high The high port in a range to listen on (inclusive).
 * @returns A server queue that can be passed to @ref mq_accept.
 * @returns NULL on failure, with errno set appropriately.
 */
struct mq *mq_serve(const char *addr, int port);

/** Close a connection.
 *
 * @param mq The connection to close.
 */
void mq_close(struct mq *mq);

/** Accept a connection.
 *
 * This is a non-blocking operation, so it will return immediately if no
 * connections are available.
 * @param server A queue returned from @ref mq_serve*.
 * @returns A connection to a client.
 * @returns NULL if no connections are available.
 */
struct mq *mq_accept(struct mq *server);

/* Wait for a message or connection.
 *
 * Blocks the current thread until a message/connection is received
 * (or until a signal or timeout interrupts). Sends are still carried
 * out while waiting. Note that all signals are unblocked for the
 * duration of this call.
 * @param mq The queue to wait on.
 * @param stoptime The time at which to stop waiting.
 * @returns 1 if a message/connection is available.
 * @returns 0 if interrupted by timeout/signal handling.
 * @returns -1 on closed socket or other error, with errno set appropriately.
 */
int mq_wait(struct mq *mq, time_t stoptime);


//------------Polling API-----------------//

/** Create a new (empty) polling group.
 *
 * This is a specialized hash table that knows about the states of the
 * channels it contains, and can efficiently handle non-blocking IO for a
 * large number of connections.
 * @returns A new polling group.
 */
struct mq_poll *mq_poll_create(void);

/** Delete a polling group.
 *
 * Note that the polling group does not take ownership of queues it
 * contains, so you should be keeping a list somewhere else. This call only
 * deletes the polling group itself, not the message queues it contains.
 * @param p The polling group to delete.
 */
void mq_poll_delete(struct mq_poll *p);

/** Add a message queue to a polling group.
 *
 * The tag value is returned from @ref mq_poll_readable and friends.
 * This could be a pointer to some client/worker struct that
 * knows its own channel, or NULL to just use mq. Note that a
 * queue can only be added to a single polling group.
 * @param p The polling group to add to.
 * @param mq The queue to add.
 * @param tag A pointer to identify the queue. If tag is NULL, mq will
 *  be used instead.
 * @returns 0 on success.
 * @returns -1 on error, with errno set appropriately.
 */
int mq_poll_add(struct mq_poll *p, struct mq *mq, void *tag);

/** Remove a message queue from a polling group.
 *
 * Be sure to remove a message queue from any polling groups before deleting
 * the it! Removing a message queue from a polling group that does
 * not contain is is a no-op, but will succeed.
 * @param p The polling group to remove from.
 * @param mq The message queue to remove.
 * @returns 0 on success.
 * @returns -1 on error, with errno set appropriately.
 */
int mq_poll_rm(struct mq_poll *p, struct mq *mq);

/** Wait for messages or connections.
 *
 * Blocks the current thread until a message/connection is received on one
 * of the message queues in the polling group (or until a signal or
 * timeout interrupts). Sends are still carried out while waiting.
 * Note that all signals are unblocked for the duration of this call.
 * @param p The polling group to wait on.
 * @param stoptime The time at which to stop waiting.
 * @returns The number of events available (or zero if interrupted by
 * timeout/signal handling).
 * @returns -1 on error, with errno set appropriately.
 */
int mq_poll_wait(struct mq_poll *p, time_t stoptime);

/** Find a queue with messages waiting.
 *
 * Returns the tag of an arbitrary queue in the polling group, and may
 * return the same queue until its messages are popped. This is more
 * efficient than looping over a list of message queues and calling
 * @ref mq_recv on all of them, since the polling group keeps track of
 * queue states internally.
 * @param p The polling group to inspect.
 * @returns The tag associated with a queue with messages waiting.
 * @returns NULL if no queues in the polling group have messages waiting.
 */
void *mq_poll_readable(struct mq_poll *p);

/** Find a server queue with connections waiting.
 *
 * Returns the tag of an arbitrary server queue in the polling group, and may
 * return the same queue until its connections are accepted. This is more
 * efficient than looping over a list of queues and calling @ref mq_accept
 * on all of them, since the polling group keeps track of queue states
 * internally.
 * @param p The polling group to inspect.
 * @returns The tag associated with a queue with connections waiting.
 * @returns NULL if no queues in the polling group have connections waiting.
 */
void *mq_poll_acceptable(struct mq_poll *p);

/** Find a queue in the error state or closed socket.
 *
 * Returns the tag of an arbitrary queue in the polling group, and may
 * return the same queue until it it removed. This is more efficient than
 * looping over a list of message queues and checking for errors on all
 * of them, since the polling group keeps track of queue states internally.
 * @param p The polling group to inspect.
 * @returns The tag of a queue in the error state or with closed socket.
 * @returns NULL if no queues in the polling group are in error.
 */
void *mq_poll_error(struct mq_poll *p);


//------------Send/Recv-------------------//

/** Push a message onto the send queue.
 *
 * This is a non-blocking operation, and will return immediately.
 * Messages are delivered in the order they were sent. Note that the queue
 * takes ownership of the passed-in buffer, so callers MUST NOT use/delete
 * the buffer after pushing it onto the send queue. It will
 * be automatically deleted when the send is finished. In particular,
 * passing in a stack-allocated buffer_t WILL result in memory corruption.
 * Only use heap-allocated buffers here.
 * @param mq The message queue.
 * @param buf The message to send.
 * @returns 0 on success. Note that this only indicates that the message was
 *  successfully queued. It gives no indication about delivery.
 * @returns -1 on failure.
 */
int mq_send_buffer(struct mq *mq, buffer_t *buf);

/** Pop a message from the receive queue.
 *
 * This is a non-blocking operation, and will return immediately if no
 * messages are available. This function supports two modes of operation:
 * allocating storage or using provided storage.
 * If no storage is provided, each message will be written to a newly-allocated
 * buffer, with a pointer stored in @ref out. The caller takes ownership of any such
 * buffers, and is responsible for deleting them. If a storage location was provided
 * (via mq_store_*), the return code will indicate that a complete message was
 * written there. @ref out will be ignored.
 * @param mq The message queue.
 * @param out The location of a buffer_t pointer containing the message.
 * @returns MQ_MSG_NONE if no message is available.
 * @returns MQ_MSG_NEWBUFFER if a message was stored in a newly-allocated
 *  buffer (pointed to by out).
 * @returns MQ_MSG_BUFFER if a message has been written to a previously-provided buffer.
 * @returns MQ_MSG_BUFFER if a message has been written to a previously-provided file.
 * @returns MQ_MSG_BUFFER if a message has been written to a previously-provided process.
 */
mq_msg_t mq_recv(struct mq *mq, buffer_t **out);

/** Store the next message in the given buffer.
 *
 * This function allows the caller to provide the storage space for the next
 * message to be received. This may be useful in avoiding heap allocations.
 * @ref buf must already be initialized. Any existing contents will be overwritten.
 * It is undefined behavior to inspect/modify @ref buf before getting MQ_MSG_BUFFER
 * from @ref mq_recv (the notable exception is deleting the buffer and *not*
 * calling *_wait() again). It is undefined behavior to call this if a message has
 * already been partially received. It is therefore only safe to call this before
 * calling *_wait() for the first time or immediately after receiving a message
 * from @ref mq_recv().
 * @param mq The message queue.
 * @param buf The buffer to use to store the next message.
 * @returns 0 on success.
 * @returns -1 on failure, with errno set appropriately.
 */
int mq_store_buffer(struct mq *mq, buffer_t *buf);

//int mq_store_file(struct mq *mq, ...);
//int mq_store_process(struct mq *mq, ...);

#endif
