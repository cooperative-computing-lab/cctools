/*
   Copyright (C) 2016- The University of Notre Dame
   This software is distributed under the GNU General Public License.
   See the file COPYING for details.
   */

#include <stdio.h>
#include <string.h>

#include "qbucket.h"
#include "rmsummary.h"
#include "int_sizes.h"


const char *category = "test";

int main(int argc, char **argv) {
    struct qbucket *qb = qbucket_create();
    init_qbucket(1, qb);
    return 0; 
}

