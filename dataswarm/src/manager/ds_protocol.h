/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/** @file ds_protocol.h
This file describes a handful of constants that are necessary for
a common implementation of the protocol between the manager,
worker, and catalog, but should not be visible to the public user API.
*/

#ifndef DS_PROTOCOL_H
#define DS_PROTOCOL_H

#define DS_PROTOCOL_VERSION 1

#define DS_LINE_MAX 4096       /**< Maximum length of a ds message line. */

#endif
