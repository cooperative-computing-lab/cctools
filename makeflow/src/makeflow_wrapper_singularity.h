/*
 * Copyright (C) 2016- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 * */

/* 
 * File:   makeflow_wrapper_singularity.h
 * Author: Kyle D Sweeney
 *
 * Created on July 27, 2016
 */

#ifndef MAKEFLOW_WRAPPER_SINGULARITY_H
#define MAKEFLOW_WRAPPER_SINGULARITY_H

#define CONTAINER_SINGULARITY_SH "singularity.wrapper.sh"

void makeflow_wrapper_singularity_init( struct makeflow_wrapper *w, char *container_image);

#endif /* MAKEFLOW_WRAPPER_SINGULARITY_H */

