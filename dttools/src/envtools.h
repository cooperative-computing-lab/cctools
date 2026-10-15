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

/** If the environment variable named by src is set, copy its value into
  the environment variable named by dst (see setenv(3) for the meaning of
  overwrite). Does nothing if src is unset.
  @param dst Name of the destination environment variable.
  @param src Name of the source environment variable.
  @param overwrite Whether to overwrite dst if it is already set.
  */
void setenv_compat(const char *dst, const char *src, int overwrite);


/* Return the first temporary directory found in the following order:
   override_tmp_dir argument value, CCTOOLS_TEMP env var, _CONDOR_SCRATCH_DIR
   env var, TMPDIR env var, TEMP env var, /tmp
   */
const char *system_tmp_dir(const char *override_tmp_dir);
#endif
