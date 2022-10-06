/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef ENVTOOLS_H
#define ENVTOOLS_H

int find_executable(const char *exe_name, const char *env_path_var, char *exe_path, int max_length);

/** Takes an infile and replaces all environment variables of the form
  $[A-Za-z_]+[0-9A-Za-z_]* with the resolved environment using getenv.
  This updated information is written to outfile.
  @param infile Input file name that is opens and scanned for variables
  @param outfile Output file name where resolved contents will be written
  @return Result value 1 is failure and 0 is success.
  */

int env_replace( const char *infile, const char *outfile );

#endif
