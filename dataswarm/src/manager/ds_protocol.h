/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/** @file ds_protocol.h
This file describes a handful of constants that are necessary for
a common implementation of the work queue protocol between the manager,
worker, and catalog, but should not be visible to the WQ user API.
This file should not be installed and should only be included by .c files.
*/

#ifndef DS_PROTOCOL_H
#define DS_PROTOCOL_H

/* 4: added invalidate-file message, for cache management.    */
/* 5: added wall_time, end_time messages, for task maximum running time. */
/* 6: worker only report total, max, and min resources. */
/* 7: added category message */
/* 8: worker send feature message. */
/* 9: recursive send/recv and filename encoding. */
/* 10: added coprocess message. */
/* 11: added url/command as a file source, added cache-update/invalidate */

#define DS_PROTOCOL_VERSION 11

#define DS_LINE_MAX 4096       /**< Maximum length of a work queue message line. */
#define DS_POOL_NAME_MAX 128   /**< Maximum length of a work queue pool name. */
#define WORKER_WORKSPACE_NAME_MAX 2048   /**< Maximum length of a work queue worker's workspace name. */

#define DS_PROTOCOL_FIELD_MAX 256

#endif
