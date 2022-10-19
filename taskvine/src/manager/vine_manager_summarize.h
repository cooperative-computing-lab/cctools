/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_MANAGER_SUMMARIZE_H
#define VINE_MANAGER_SUMMARIZE_H

#include "vine_manager.h"
#include "rmsummary.h"

struct rmsummary ** vine_manager_summarize_workers( struct vine_manager *q );

#endif
