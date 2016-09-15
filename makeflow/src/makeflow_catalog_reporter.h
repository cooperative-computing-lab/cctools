/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/* 
 * File:   makeflow_catalog_reporter.h
 * Author: Kyle D. Sweeney
 *
 * Created on July 20, 2016, 10:51 AM
 */

#ifndef MAKEFLOW_CATALOG_REPORTER_H
#define MAKEFLOW_CATALOG_REPORTER_H

/**
 * Creates a summary of the status of makeflow and sends it to the catalog server.
 * @param d the DAG maintained by makeflow
 * @param name the name of the project
 * @return 1 if everything went well, 0 if not
 */
int makeflow_catalog_summary(struct dag* d, char* name, batch_queue_type_t type, timestamp_t start);


#endif /* MAKEFLOW_CATALOG_REPORTER_H */

