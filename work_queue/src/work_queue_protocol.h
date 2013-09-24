/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/** @file work_queue_protocol.h
This file describes a handful of constants that are necessary for
a common implementation of the work queue protocol between the master,
worker, and catalog, but should not be visible to the WQ user API.
This file should not be installed and should only be included by .c files.
*/

#ifndef WORK_QUEUE_PROTOCOL_H
#define WORK_QUEUE_PROTOCOL_H

#define WORK_QUEUE_PROTOCOL_VERSION 3

#define WORK_QUEUE_LINE_MAX 4096       /**< Maximum length of a work queue message line. */
#define WORK_QUEUE_POOL_NAME_MAX 128   /**< Maximum length of a work queue pool name. */
#define WORKER_WORKSPACE_NAME_MAX 2048   /**< Maximum length of a work queue worker's workspace name. */

#define WORK_QUEUE_FS_CMD 1            /**< Indicates thirdput/thirdget should execute a command. */
#define WORK_QUEUE_FS_PATH 2           /**< Indicates thirdput/thirdget refers to a path. */
#define WORK_QUEUE_FS_SYMLINK 3        /**< Indicates thirdput/thirdget should create a symlink. */

#define WORK_QUEUE_PROTOCOL_FIELD_MAX 256

#endif
