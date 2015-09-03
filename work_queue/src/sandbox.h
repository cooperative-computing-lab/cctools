#ifndef SANDBOX_H
#define SANDBOX_H

struct sandbox {
	char *sandbox_path;
	char *output_files;
};

/**
Create a sandbox by making a temporary directory, then linking
all of the input files into place.  Returns a sandbox object on success.
@param parent_dir The directory in which the sandbox will be created.
@param input_files A comma-separated list of files to link into the sandbox.
@param output_files A comma-separated list of files to move out of the sandbox upon completion.
**/

struct sandbox * sandbox_create( const char *parent_dir, const char *input_files, const char *output_files );

/**
Gracefully clean up a sandbox by moving back the declared outputs,
and then deleting the sandbox directory.
@param s The sandbox to clean up.
**/

void sandbox_cleanup( struct sandbox *s );

/**
Forcibly delete a sandbox, without retrieving the outputs.
@param s The sandbox to delete.
**/

void sandbox_delete( struct sandbox *s );

#endif
