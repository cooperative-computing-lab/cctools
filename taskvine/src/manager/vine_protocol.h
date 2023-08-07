/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/** @file vine_protocol.h
This file describes a handful of constants that are necessary for
a common implementation of the protocol between the manager,
worker, and catalog, but should not be visible to the public user API.
*/

#ifndef VINE_PROTOCOL_H
#define VINE_PROTOCOL_H

#define VINE_PROTOCOL_VERSION 2

#define VINE_LINE_MAX 4096       /**< Maximum length of a vine message line. */

#endif
