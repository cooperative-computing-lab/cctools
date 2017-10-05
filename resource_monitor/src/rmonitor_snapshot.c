/*
  Copyright (C) 2017- The University of Notre Dame
  This software is distributed under the GNU General Public License.
  See the file COPYING for details.
*/

#include <string.h>

#include "debug.h"
#include "rmonitor_helper_comm.h"

int main(int argc, char **argv) {

    if(argc < 2) {
        fatal("Use: %s MESSAGE", argv[0]);
    }

    struct rmonitor_msg msg;

    msg.type   = SNAPSHOT;
    msg.error  = 0;
    msg.origin = -1;
    strncpy(msg.data.s, argv[1], sizeof(msg.data.s) - 1);

    int status = send_monitor_msg(&msg);
    if(status < 0) {
        fatal("Could not send message to resource_monitor");
    }
}


