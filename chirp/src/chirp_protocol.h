/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_PROTOCOL_H
#define CHIRP_PROTOCOL_H

/** @file chirp_protocol.h
This file defines the binary values mandated by the Chirp Protocol specification,
used by both the client and server implementations of the protocol.
*/

/** The maximum length of a line in a Chirp remote procedure call. */
#define CHIRP_LINE_MAX 1024

/** The maximum length of a full path in any Chirp operation. */
#define CHIRP_PATH_MAX 1024

/** The current version of the Chirp protocol. */
#define CHIRP_VERSION 3

/** The default TCP port used by a Chirp server. */
#define CHIRP_PORT 9094

/** Error: Cannot perform this operation without successfully authenticated. */
#define CHIRP_ERROR_NOT_AUTHENTICATED -1

/** Error: User is authenticated but is not allowed to perform this operation. */
#define CHIRP_ERROR_NOT_AUTHORIZED -2

/** Error: The named file or directory does not exist. */
#define CHIRP_ERROR_DOESNT_EXIST -3

/** Error: The named file or directory already exists. */
#define CHIRP_ERROR_ALREADY_EXISTS -4

/** Error: The server cannot process a file or directory that large. */
#define CHIRP_ERROR_TOO_BIG -5

/** Error: There is no physical space remaining, or the user has exceeded some space allocation policy. */
#define CHIRP_ERROR_NO_SPACE -6

/** Error: There is not enough local memory to complete the request. */
#define CHIRP_ERROR_NO_MEMORY -7

/** Error: The request was malformed at the protocol level. */
#define CHIRP_ERROR_INVALID_REQUEST -8

/** Error: The client has too many files open. */
#define CHIRP_ERROR_TOO_MANY_OPEN -9

/** Error: The requested file or directory is indicated as 'busy' by the operating system, but may be available later. */
#define CHIRP_ERROR_BUSY -10

/** Error: The operation could not be completed, but may be tried again. */
#define CHIRP_ERROR_TRY_AGAIN -11

/** Error: The indicated file descriptor is not valid. */
#define CHIRP_ERROR_BAD_FD -12

/** Error: The request does not apply to this object, which is a directory. */
#define CHIRP_ERROR_IS_DIR -13

/** Error: The request does not apply to this object, which is not a directory. */
#define CHIRP_ERROR_NOT_DIR -14

/** Error: The requested directory could not be deleted, because it still contains something. */
#define CHIRP_ERROR_NOT_EMPTY -15

/** Error: A link cannot be constructed across devices. */
#define CHIRP_ERROR_CROSS_DEVICE_LINK -16

/** Error: The service is not currently available. */
#define CHIRP_ERROR_OFFLINE -17

/** Error: The service is not currently available. */
#define CHIRP_ERROR_TIMED_OUT -18

/** Error: The service is not currently available. */
#define CHIRP_ERROR_DISCONNECTED -19

/** Error: The service is not currently available. */
#define CHIRP_ERROR_GRP_UNREACHABLE -20

/** Error: A attempted job operation refers to a job which does not exist. */
#define CHIRP_ERROR_NO_SUCH_JOB -21

/** Error: A attempted operation does not apply to a pipe. */
#define CHIRP_ERROR_IS_A_PIPE -22

/** Error: This operation is not supported by this server. */
#define CHIRP_ERROR_NOT_SUPPORTED -23

/** Error: File name too long. */
#define CHIRP_ERROR_NAME_TOO_LONG -24

/** Error: An unknown error occurred. */
#define CHIRP_ERROR_UNKNOWN -127

#endif

/* vim: set noexpandtab tabstop=8: */
