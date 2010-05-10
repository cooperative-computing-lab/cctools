/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/** Parse a long file into a set of chunks. Files are expected to be delimited by having either the logical filename or the file content lines prefixed. If both prefix parameters are null or empty, the file will not be parsed.
@param file_name The name of the physical file to parse.
@param ln_prefix A string containing the prefix before a new logical file's file name. May be null or empty.
@param fc_prefix A string containing the prefix before a line of content. May be null or empty.
@return A pointer to a set of file chunks, on which @ref chunk_read may be called.
*/

struct chunk_set *chunk_parse_file( char *file_name, char *ln_prefix, char *fc_prefix );

/** Get a large chunk of data corresponding to the entire content of a logical file (within a larger physical file that has already been parsed into chunks with @ref chunk_parse_file.
@param chunk_set A chunk_set created via @ref chunk_parse_file.
@param file_name The name of the logical file you want to read.
@param size A pointer to an integer which will be filled with the length of the chunk.
@return A pointer to the content of the logical file.
*/

char *chunk_read( struct chunk_set *chunk_set, const char *file_name, int *size );

/** Read a number of physical files and concatenate them into a single physical file with many logical files within. After this function returns, @ref chunk_parse_file may be called.
@param new_name The name of the new physical file name to be created.
@param filenames An array of file names to be read and concatenated into the new file.
@param num_files The number of file names in the filenames array.
*/

int chunk_concat( const char *new_name, char **filenames, int num_files, char *ln_prefix, char *fc_prefix );

