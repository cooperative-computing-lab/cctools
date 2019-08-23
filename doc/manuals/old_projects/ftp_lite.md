# FTP-Lite Manual

**Last edited: 27 August 2004**

## Table of Contents⇗

  * Front Matter
  * Introduction
  * Installation
  * Examples
  * Reference 

## Front Matter⇗

The FTP-Lite Library is copyright (C) 2004 Douglas Thain and the University of
Notre Dame.

This product includes software developed by and/or derived from the Globus
Project (http://www.globus.org/) to which the U.S. Government retains certain
rights.

This program is released under the GNU General Public License. See the file
COPYING for details.

This software comes with no warranty and no guarantee of support. Questions,
bug reports, and patches may be sent to [condor-
admin@cs.wisc.edu](mailto:condor-admin@cs.wisc.edu). We will address such
inquiries as time and resources permit.

## Introduction⇗

FTP-Lite is a library for accessing FTP servers from UNIX C programs.

It is designed to be simple, correct, and easily debuggable. In particular,
FTP-Lite presents an FTP service in terms of UNIX abstractions. Errors are
return in standard ` errno` values. Data streams are presented as `FILE`
objects. All procedures perform blocking I/O.

The library may be built as-is in order to communicate with ordinary
name/password or anonymous FTP servers. However, if the [Globus
Toolkit](http://www.globus.org) is available, it will also perform GSI
authentication with your proxy certificate.

FTP-Lite provides perfectly reasonable performance for simple clients that
access one file at a time. For clients that need to manage multiple servers at
once, we heartily recommend the FTP implementation found in the Globus
Toolkit. It uses a variety of techniques, such as multiple data streams and
non-blocking interfaces, for achieving high performance and multi-server
access.

This library was designed specifiy to be used with the [Pluggable File
System](http://www.cs.wisc.edu/condor/pfs), which presents network storage
devices as UNIX file systems. You are welcome to use it for other purposes,
according to the terms set out in the GNU Library General Public License.

## Installation⇗

Download the FTP-Lite source package from the [web
page](http://www.cs.wisc.edu/condor/ftp_lite). Unpack the archive like so: `%
gunzip ftp_lite-0.0.tar.gz % tar xvf ftp_lite-0.0.tar ` Decide on a place to
install FTP-Lite. If you have the Globus and SSL libraries, figure out where
they are installed, and feed the information to `configure`: `% cd
ftp_lite-0.0 % ./configure --prefix /install/dir --with-globus-path
/path/to/globus --with-ssl-path /path/to/ssl ` (At UW-Madison, the appropriate
invocation would be:) `% cd ftp_lite-0.0 % ./configure --prefix ~/ftp_lite
--with-globus-path /p/condor/workspaces/globus --with-ssl-path
/p/condor/workspaces/ssl ` Finally, build and install the library: `% make %
make install ` To build a program using FTP-Lite, change your compile and link
options like so: `CCFLAGS += -I/install/dir/include LDFLAGS +=
-L/install/dir/lib -lftp_lite `

## Examples⇗

For examples of using the library, we recommend that you begin by examining
the code for the simple programs `ftp_lite_test` and `ftp_lite_copy`. A
complete description of every function may be found in the reference section
below. Here is a brief example to get you started.

A program using the library must first include `ftp_lite.h`: `#include
"ftp_lite.h"` To open a server, `ftp_lite_open` with a server name, port
number, and a stream on which to send debugging messages. For no debugging,
leave the third argument null. On success, this function returns a pointer to
a server. `struct ftp_server *s; s = ftp_lite_open( "ftp.cs.wisc.edu",
FTP_LITE_DEFAULT_PORT, stderr ); if(!s) { perror("couldn't open server");
return 0; } ` You must authenticate yourself to the server before accessing
any data. Three sorts of authentication are currently provided: anonymous,
userpass, and Globus. For example, to authenticate with a username and
password: `success = ftp_lite_auth_userpass(server,"fred","secret");
if(!success) { perror("couldn't log in"); return 0; } ` For convenience, FTP-
Lite provides a single procedure which tries the various authentication
methods, possible requesting information from the console. Most users will
find it easiest to replace the above two steps with : `s =
ftp_lite_open_and_auth( "ftp.cs.wisc.edu", stderr ); if(!s) { perror("couldn't
open server"); return 0; } ` To retrieve a file, `ftp_lite_get` with the
server pointer, a path name, and the offset at which to begin. On success, it
returns a `FILE` object. `FILE *f; f = ftp_lite_get( s, "README", 0 ); if(!f)
{ perror("couldn't get file"); return 0; } ` You may read from this stream
pointer using any of the standard UNIX I/O operations, such as `fscanf`,
`fread`, and so on. For convenience, FTP-Lite provides a function
`ftp_lite_stream_to_stream` that will copy one whole file pointer into
another. So, to display this file, you might do this: `length =
ftp_lite_stream_to_stream( f, stdout ); if(length<0) { perror("couldn't
transfer data"); return 0; } ` When done reading data, you must close the
stream and inform the server that you are done: `fclose(f); ftp_lite_done(s);
` To close the connection to the server completely: `ftp_lite_close(s);`

## Reference⇗

This section lists all of the public functions in the FTP-Lite library.

Unless noted otherwise, all functions return true (non-zero) on success or
false (zero) on failure. In addition, every function sets `errno`
appropriately on a failure. Tools for handling error values can be found in
the UNIX man pages for `errno`, `strerror`, and `perror`. Nearly every error
value is a possible result of any function.

Some error values are inacurrate, due to weaknesses in the FTP protocol
itself. For example, the FTP error 550 is represented as the errno `EEXIST`.
However, a brief poll of servers shows that many return the same error value
for errors that should be distinct, such as "no such file", and "file already
exists." So, be careful.

If the library is returning unexpected results, we recommend that you debug
the code by passing `stderr` as the debugging argument to `ftp_lite_open`.
This will show a low of events in the protocol, and is invaluable in revealing
unexpected events.

So, here are the procedures in the library:

* **ftp_lite_auth_anonymous** `int ftp_lite_auth_anonymous( struct ftp_lite_server *s );` Attempt to log in anonymously to an already-opened server. 
* **ftp_lite_auth_globus** `int ftp_lite_auth_globus( struct ftp_lite_server *s );` Attempt to log in with Globus credentials to an already-opened server. The er must have already established a proxy certificate with ` grid-proxy-init` or a similar tool. 
* **ftp_lite_auth_userpass** `int ftp_lite_auth_userpass( struct ftp_lite_server *s, const char *user, const char *password );` Attempt to log in with this name and password. This mechanism sends names and passwords in the clear and should be deprecated in favor of Globus authentication. 
* **ftp_lite_change_dir** `int ftp_lite_change_dir( struct ftp_lite_server *s, const char *dir );` Change the current working directory to that given. 
* **ftp_lite_close** `void ftp_lite_close( struct ftp_lite_server *server );` Close this open server. Once a connection is closed, the server pointer is no longer valid. 
* **ftp_lite_delete int** `int ftp_lite_delete( struct ftp_lite_server *s, const char *path );` Delete a file. 
* **ftp_lite_delete_dir** `int ftp_lite_delete_dir( struct ftp_lite_server *s, const char *dir );` Delete a directory. Most servers will not permit the deletion of a directory that is not empty. 
* **ftp_lite_done** `int ftp_lite_done( struct ftp_lite_server *s );` Signal that a data transfer is complete. This must be ed before any other functions are invoked. 
* **ftp_lite_get** `FILE * ftp_lite_get( struct ftp_lite_server *s, const char *path, off_t offset );` Retrieve a file beginning from this offset. On success, returns a stream pointer. On failure, returns null. After reading to the end of the stream, you must ` fclose` and `ftp_lite_done`. 
* **ftp_lite_list** `FILE * ftp_lite_list( struct ftp_lite_server *s, const char *path );` Retrieve the list of names contained in this directory. On success, return a stream pointer which will provide the list of newline-terminated names. On failure, returns null. After reading to the end of the stream, you must ` fclose` and `ftp_lite_done`. 
* **ftp_lite_login** `int ftp_lite_login( const char *prompt, char *name, int namelen, char *pass, int passlen );` Display a prompt on the console and ask the user to enter a name and password. ` name` and `pass` are filled in up to the lengths given. 
* **ftp_lite_make_dir** `int ftp_lite_make_dir( struct ftp_lite_server *s, const char *dir );` Create a directory. 
* **ftp_lite_nop** `int ftp_lite_nop( struct ftp_lite_server *s );` Send a no-operation command to the server. This command is useful for determining if a connection is still alive. 
* **ftp_lite_open** `struct ftp_lite_server * ftp_lite_open( const char *host, int port, FILE *log );` Connect to a server on the given host and port. The third argument gives a stream which is used for debugging information. On success, return an opaque pointer to a server. On failure, return null. The appropriate port depends on the authentication method to be used. For Globus authentication, connect to ` FTP_LITE_GSS_DEFAULT_PORT`. For anonymous and userpass authentication, connect to `FTP_LITE_DEFAULT_PORT`. 
* **ftp_lite_open_and_auth** `struct ftp_lite_server * ftp_lite_open_and_auth( const char *host, FILE *log );` Connect to a server, but try all available ports and authentication methods. The second argument gives a stream to be used for debugging. On success, return an opaque pointer to a server. On failure, return null. 
* **ftp_lite_put** `FILE * ftp_lite_put( struct ftp_lite_server *s, const char *path, off_t offset, size_t size );` Transmit a file to a server. On success, returns a stream to be written to. On failure, returns null. After writing all data to the stream, you must ` fclose` and `ftp_lite_done`. `offset` controls the point at which writing will begin in the target file. If `size` is `FTP_LITE_WHOLE_FILE`, then the target file will be truncated when the stream is closed. A variety of FTP commands may be used to implement a put, and not all severs will support all combinations of `offset` and `size`. 
* **ftp_lite_rename** `int ftp_lite_rename( struct ftp_lite_server *s, const char *oldname, const char *newname );` Move the file ` oldname` in `newname`. 
* **ftp_lite_size** `size_t ftp_lite_size( struct ftp_lite_server *s, const char *path );` Return the number of bytes stored in this file. On failure, returns -1. 
* **ftp_lite_stream_to_buffer** `int ftp_lite_stream_to_buffer( FILE *input, char **buffer );` Copy the contents of this stream into a memory buffer. On success, returns the number of bytes copied. On failure, returns -1. ` input` must be a stream opened for reading, and `buffer` must be a pointer to an uninitialized `char *`. Space for the buffer will be allocated with `malloc`. The er becomes responsible for freeing the buffer when done. 
* **ftp_lite_stream_to_stream** `int ftp_lite_stream_to_stream( FILE *input, FILE *output );` Copy the contents of one stream into another. On success, returns the number of bytes copied. On failure, returns -1. 
* **ftp_lite_third_party_transfer** `int ftp_lite_third_party_transfer( struct ftp_lite_server *source, const char *source_file, struct ftp_lite_server *target, const char *target_file );` Performs a third-party transfer between two servers. Each server must already be opened and authenticated. There are a large number of reasons for which a third party transfer might fail. We recommend you use this feature with the debugging stream enabled. 

