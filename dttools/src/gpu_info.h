/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef GPU_INFO_H
#define GPU_INFO_H

/** @file gpu_info.h
Query gpu properties.
*/

/** Get the total number of gpus
@return Number of gpus
*/
int gpu_count_get();

/** Get the model name of gpu
@return Name of gpu
*/
char *gpu_name_get();

#endif

/* vim: set noexpandtab tabstop=8: */
