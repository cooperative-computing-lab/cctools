/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DEBUG_H
#define DEBUG_H

/** @file debug.h General purpose debugging routines.
The cctools debugging system is built into all software components.
Any code may invoke @ref debug with a printf-style message to log relevant
information.  Each debug call uses a flag to indicate which subsystem is
doing the logging, so that various subsystems may be easily turned on and off.
For example, the Chirp subsystem has many statements like this:

<pre>
debug(D_CHIRP,"reading file %s from host %s:d",filename,hostname,port);
</pre>

The <tt>main</tt> routine of a program is responsible for
calling @ref debug_config, @ref debug_config_file and @ref debug_flags_set to choose
what to display and where to send it.  By default, nothing is displayed,
unless it has the flags D_NOTICE or D_FATAL.  For example, a main program might do this:

<pre>
  debug_config("superprogram");
  debug_config_file("/tmp/myoutputfile");
  debug_flags_set("tcp");
  debug_flags_set("chirp");
</pre>
*/

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>

#include "int_sizes.h"

#define D_SYSCALL  0x000000001	/**< Debug system calls in Parrot. */
#define D_CHANNEL  0x000000002	/**< Debug the I/O channel in Parrot. */
#define D_PROCESS  0x000000004	/**< Debug jobs and process. */
#define D_NOTICE   0x000000008	/**< Indicates a message that is always shown. */
#define D_RESOLVE  0x000000010	/**< Debug the file name resolver in Parrot. */
#define D_LIBCALL  0x000000020	/**< Debug I/O library calls in Parrot. */
#define D_LOCAL    0x000000040	/**< Debug the local I/O module in Parrot. */
#define D_DNS      0x000000080	/**< Debug domain name lookups. */
#define D_TCP      0x000000100	/**< Debug TCP connections and disconnections. */
#define D_AUTH     0x000000200	/**< Debug authentication and authorization actions. */
#define D_IRODS    0x000000400	/**< Debug the iRODS module in Parrot. */
#define D_CVMFS    0x000000800	/**< Debug CVMFS module in Parrot. */
#define D_HTTP     0x000001000	/**< Debug HTTP queries. */
#define D_FTP      0x000002000	/**< Debug FTP operations. */
#define D_NEST     0x000004000	/**< Debug the NEST module in Parrot. */
#define D_GROW     0x000008000	/**< Debug the GROW filesystem in Parrot. */
#define D_CHIRP    0x000010000	/**< Debug Chirp protocol operations. */
#define D_DCAP     0x000020000	/**< Debug the DCAP module in Parrot. */
#define D_RFIO     0x000040000	/**< Debug the RFIO module in Parrot. */
#define D_GLITE    0x000080000	/**< Debug the gLite module in Parrot. */
#define D_MULTI    0x000100000	/**< Debug Chirp Multi filesystems. */
#define D_PSTREE   0x000200000	/**< Debug process trees in Parrot. */
#define D_ALLOC    0x000400000	/**< Debug space allocations in the Chirp server. */
#define D_LFC	   0x000800000	/**< Debug LFC file lookups in Parrot. */
#define D_GFAL	   0x001000000	/**< Debug the GFAL module in Parrot. */
#define D_SUMMARY  0x002000000	/**< Show I/O summary stats in Parrot. */
#define D_DEBUG    0x004000000	/**< Show general debugging messages. */
#define D_LOGIN    0x008000000	/**< Debug logins on the Chirp server. */
#define D_CACHE    0x010000000	/**< Debug cache operations in Parrot. */
#define D_POLL     0x020000000	/**< Debug FD polling in Parrot. */
#define D_HDFS	   0x040000000	/**< Debug the HDFS module in Parrot. */
#define D_WQ	   0x080000000	/**< Debug the Work Queue operations. */
#define D_BXGRID   0x100000000LL  /**< Debug the BXGRID Module in Parrot. */
#define D_USER	   0x200000000LL  /**< Debug custom user application. */
#define D_XROOTD   0x400000000LL  /**< Debug Xrootd module in Parrot */
#define D_MPI      0x800000000LL  /**< Debug MPI module for Makeflow */
#define D_BATCH   0x1000000000LL  /**< Debug batch_job modules */
#define D_LOG     0x2000000000LL  /**< Debug batch_job modules */

/** Debug all remote I/O operations. */
#define D_REMOTE   (D_HTTP|D_FTP|D_NEST|D_CHIRP|D_DCAP|D_RFIO|D_LFC|D_GFAL|D_MULTI|D_GROW|D_IRODS|D_HDFS|D_BXGRID|D_XROOTD|D_CVMFS)

/** Show all debugging info. */
#define D_ALL      ~(0LL)

/*
It turns out that many libraries and tools make use of
symbols like "debug" and "fatal".  This causes strange
failures when we link against such codes.  Rather than change
all of our code, we simply insert these defines to
transparently modify the linker namespace we are using.
*/

#define debug                  cctools_debug
#define fatal                  cctools_fatal
#define warn                   cctools_warn
#define debug_config           cctools_debug_config
#define debug_config_file      cctools_debug_config_file
#define debug_config_file_size cctools_debug_config_file_size
#define debug_config_fatal     cctools_debug_config_fatal
#define debug_config_getpid    cctools_debug_config_getpid
#define debug_flags_set        cctools_debug_flags_set
#define debug_flags_print      cctools_debug_flags_print
#define debug_flags_clear      cctools_debug_flags_clear
#define debug_flags_restore    cctools_debug_flags_restore
#define debug_set_flag_name    cctools_debug_set_flag_name

/** Emit a debugging message.
Logs a debugging message, if the given flags are active.
@param flags Any of the standard debugging flags OR-ed together.
@param fmt A printf-style formatting string, followed by the necessary arguments.
*/

void debug(INT64_T flags, const char *fmt, ...);

/** Emit a warning message.
Logs a warning message, regardless of if given flags are active.
@param flags Any of the standard debugging flags OR-ed together.
@param fmt A printf-style formatting string, followed by the necessary arguments.
*/

void warn(INT64_T flags, const char *fmt, ...);

/** Emit a fatal debugging message and exit.
Displays a printf-style message, and then forcibly exits the program.
@param fmt A printf-style formatting string, followed by the necessary arguments.
*/

void fatal(const char *fmt, ...);

/** Initialize the debugging system.
Must be called before any other calls take place.
@param name The name of the program to use in debug output.
*/

void debug_config(const char *name);

/** Direct debug output to a file.
All enabled debugging statements will be sent to this file.
@param file The pathname of the file for output.
@see debug_config_file_size
*/

void debug_config_file(const char *file);

/** Set the maximum debug file size.
Debugging files can very quickly become large and fill up your available disk space.
This functions sets the maximum size of a debug file.
When it exceeds this size, it will be renamed to (file).old, and a new file will be started.
@param size Maximum size in bytes of the debugging file.
*/

void debug_config_file_size(size_t size);

void debug_config_fatal(void (*callback) (void));
void debug_config_getpid(pid_t(*getpidfunc) (void));

/** Set debugging flags to enable output.
Accepts a debug flag in ASCII form, and enables that subsystem.  For example: <tt>debug_flags_set("chirp");</tt>
Typically used in command-line processing in <tt>main</tt>.
@param flagname The name of the debugging flag to enable.
@return One if the flag is valid, zero otherwise.
@see debug_flags_print, debug_flags_clear
*/

int debug_flags_set(const char *flagname);

/** Display the available debug flags.
Prints on the standard output all possible debug flag names that
can be passed to @ref debug_flags_set.  Useful for constructing a program help text.
@param stream Standard I/O stream on which to print the output.
*/

void debug_flags_print(FILE * stream);

/** Clear all debugging flags.
Clear all currently set flags, so that no output will occur.
@see debug_flags_set
*/

INT64_T debug_flags_clear(void);

/** Set name of flag combination
Sets the string value associated with flag.  This is normally used to set the <tt>D_USER</tt> user flag as so: <tt>debug_set_flag_name(D_USER, "my-application");</tt>.
@param flag Any of the standard debugging flags.
@param name New name to associate with flag.
*/

void debug_set_flag_name(INT64_T flag, const char *name);

/** Restore debug flags.
@param flags flags to set
*/
void debug_flags_restore(INT64_T flags);

#endif
