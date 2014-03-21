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

#include "int_sizes.h"

#include <unistd.h>

#include <sys/types.h>

#include <stdarg.h>
#include <stdio.h>

/* priority */
#define D_INFO     (0LL)     /**< Indicates a message that is of general interest to the user. (the default) */
#define D_FATAL    (1LL<<1)  /**< Indicates a message that is fatal. */
#define D_ERROR    (1LL<<2)  /**< Indicates a message that is a warning/error */
#define D_NOTICE   (1LL<<3)  /**< Indicates a message that is always shown. */
#define D_DEBUG    (1LL<<4)  /**< Indicates a general debugging message. */

/* subsystem */
#define D_SYSCALL  (1LL<<4)  /**< Debug system calls in Parrot. */
#define D_CHANNEL  (1LL<<5)  /**< Debug the I/O channel in Parrot. */
#define D_PROCESS  (1LL<<6)  /**< Debug jobs and process. */
#define D_RESOLVE  (1LL<<7)  /**< Debug the file name resolver in Parrot. */
#define D_LIBCALL  (1LL<<8)  /**< Debug I/O library calls in Parrot. */
#define D_LOCAL    (1LL<<9)  /**< Debug the local I/O module in Parrot. */
#define D_DNS      (1LL<<10)  /**< Debug domain name lookups. */
#define D_TCP      (1LL<<11)  /**< Debug TCP connections and disconnections. */
#define D_AUTH     (1LL<<12)  /**< Debug authentication and authorization actions. */
#define D_IRODS    (1LL<<13)  /**< Debug the iRODS module in Parrot. */
#define D_CVMFS    (1LL<<14)  /**< Debug CVMFS module in Parrot. */
#define D_HTTP     (1LL<<15)  /**< Debug HTTP queries. */
#define D_FTP      (1LL<<16)  /**< Debug FTP operations. */
#define D_NEST     (1LL<<17)  /**< Debug the NEST module in Parrot. */
#define D_GROW     (1LL<<18)  /**< Debug the GROW filesystem in Parrot. */
#define D_CHIRP    (1LL<<19)  /**< Debug Chirp protocol operations. */
#define D_DCAP     (1LL<<20)  /**< Debug the DCAP module in Parrot. */
#define D_RFIO     (1LL<<21)  /**< Debug the RFIO module in Parrot. */
#define D_GLITE    (1LL<<22)  /**< Debug the gLite module in Parrot. */
#define D_MULTI    (1LL<<23)  /**< Debug Chirp Multi filesystems. */
#define D_PSTREE   (1LL<<24)  /**< Debug process trees in Parrot. */
#define D_ALLOC    (1LL<<25)  /**< Debug space allocations in the Chirp server. */
#define D_LFC      (1LL<<26)  /**< Debug LFC file lookups in Parrot. */
#define D_GFAL     (1LL<<27)  /**< Debug the GFAL module in Parrot. */
#define D_SUMMARY  (1LL<<28)  /**< Show I/O summary stats in Parrot. */
#define D_LOGIN    (1LL<<29)  /**< Debug logins on the Chirp server. */
#define D_CACHE    (1LL<<30)  /**< Debug cache operations in Parrot. */
#define D_POLL     (1LL<<31)  /**< Debug FD polling in Parrot. */
#define D_HDFS     (1LL<<32)  /**< Debug the HDFS module in Parrot. */
#define D_WQ       (1LL<<33)  /**< Debug the Work Queue operations. */
#define D_BXGRID   (1LL<<34)  /**< Debug the BXGRID Module in Parrot. */
#define D_USER     (1LL<<35)  /**< Debug custom user application. */
#define D_XROOTD   (1LL<<36)  /**< Debug Xrootd module in Parrot */
#define D_MPI      (1LL<<37)  /**< Debug MPI module for Makeflow */
#define D_BATCH    (1LL<<38)  /**< Debug batch_job modules */
#define D_RMON     (1LL<<39)  /**< Debug resource monitor */

/** Debug all remote I/O operations. */
#define D_REMOTE   (D_HTTP|D_FTP|D_NEST|D_CHIRP|D_DCAP|D_RFIO|D_LFC|D_GFAL|D_MULTI|D_GROW|D_IRODS|D_HDFS|D_BXGRID|D_XROOTD|D_CVMFS)

/** Show all debugging info. */
#define D_ALL      (~(0LL))

/*
It turns out that many libraries and tools make use of symbols like "debug" and
"fatal".  This causes strange failures when we link against such codes.  Rather
than change all of our code, we simply insert these defines to transparently
modify the linker namespace we are using.
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
#define debug_rename           cctools_debug_rename

/** Emit a debugging message.
Logs a debugging message, if the given flags are active.
@param flags Any of the standard debugging flags OR-ed together.
@param fmt A printf-style formatting string, followed by the necessary arguments.
*/

void debug(INT64_T flags, const char *fmt, ...)
#ifndef SWIG
__attribute__ (( format(printf,2,3) ))
#endif
;

/** Emit a debugging message.
Logs a debugging message, if the given flags are active, using a va_list instead of a list of arguments.
@param flags Any of the standard debugging flags OR-ed together.
@param fmt A printf-style formatting string.
@param args A va_list containing the arguments.
*/

void vdebug(INT64_T flags, const char *fmt, va_list args);

/** Emit a warning message.
Logs a warning message, regardless of if given flags are active.
@param flags Any of the standard debugging flags OR-ed together.
@param fmt A printf-style formatting string, followed by the necessary arguments.
*/

void warn(INT64_T flags, const char *fmt, ...);

/** Emit a fatal debugging message and terminate with SIGTERM.
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

void debug_config_file_size(off_t size);

void debug_config_fatal(void (*callback) (void));

void debug_config_getpid (pid_t (*getpidf)(void));

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

/** Rename debug file with given suffix.
@param suffix Suffix of saved log.
*/
void debug_rename(const char *suffix);

#endif
