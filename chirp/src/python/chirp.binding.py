## @package ChirpPython
#
# Python Chirp bindings.
#
# The objects and methods provided by this package correspond to the native
# C API in @ref chirp_reli.h and chirp_swig_wrap.h
#
# The SWIG-based Python bindings provide a higher-level interface that
# revolves around:
#
# - @ref Chirp.Client
# - @ref Chirp.Stat

import os
import time

##
# Python Client object
#
# This class is used to create a chirp client
class Client:

    ##
    # Create a new chirp client
    #
    # @param self              Reference to the current task object.
    # @param hostport          The host:port of the server.
    # @param timeout           The time to wait for a server response on every request.
    # @param authentication    A list of prefered authentications. E.g., ['tickets', 'unix']
    # @param debug             Generate client debug output.
    def __init__(self, hostport, timeout=60, authentication=None, tickets=None, debug=False):
        self.hostport    = hostport
        self.timeout = timeout

        if debug:
            cctools_debug_config('chirp_python_client')
            cctools_debug_flags_set('chirp')


        if tickets and (authentication is None):
            authentication = ['ticket']

        self.__set_tickets(tickets)

        if authentication is None:
            auth_register_all()
        else:
            for auth in authentication:
                auth_register_byname(auth)

        self.identity = self.whoami()

        if self.identity is '':
            raise AuthenticationFailure(authentication)

    def __stoptime(self, absolute_stop_time=None, timeout=None):
        if timeout is None:
            timeout = self.timeout

        if absolute_stop_time is None:
            absolute_stop_time = time.time() + timeout

        return absolute_stop_time

    def __set_tickets(self, tickets):
        tickets_str = None
        if tickets is None:
            try:
                tickets_str = os.environ['CHIRP_CLIENT_TICKETS']
            except KeyError:
                tickets_str = None
        else:
            tickets_str = ','.join(tickets)

        if tickets_str is not None:
            auth_ticket_load(tickets_str)


    ##
    # Returns a string with identity of the client according to the server.
    #
    # @param self                Reference to the current task object.
    # @param absolute_stop_time  If given, maximum number of seconds since
    #                            epoch to wait for a server response.
    #                            (Overrides any timeout.)
    # @param timeout             If given, maximum number of seconds to
    #                            wait for a server response.
    def whoami(self, absolute_stop_time=None, timeout=None):
        return chirp_wrap_whoami(self.hostport, self.__stoptime(absolute_stop_time, timeout))


    ##
    # Returns a string with the ACL of the given directory.
    # Throws an IOError on error (no such directory).
    #
    # @param self                Reference to the current task object.
    # @param path                Target directory.
    # @param absolute_stop_time  If given, maximum number of seconds since
    #                            epoch to wait for a server response.
    #                            (Overrides any timeout.)
    # @param timeout             If given, maximum number of seconds to
    #                            wait for a server response.
    def listacl(self, path='/', absolute_stop_time=None, timeout=None):
        acls = chirp_wrap_listacl(self.hostport, path, self.__stoptime(absolute_stop_time, timeout))

        if acls is None:
            raise IOError(path)

        return acls.split('\n')


    ##
    # Returns a list with the names of the files in the path.
    # Throws an IOError on error (no such directory).
    #
    # @param self                Reference to the current task object.
    # @param path                Target file/directory.
    # @param absolute_stop_time  If given, maximum number of seconds since
    #                            epoch to wait for a server response.
    #                            (Overrides any timeout.)
    # @param timeout             If given, maximum number of seconds to
    #                            wait for a server response.
    def ls(self, path, absolute_stop_time=None, timeout=None):
        dr    = chirp_reli_opendir(self.hostport, path, self.__stoptime(absolute_stop_time, timeout))
        files = []

        if dir is None:
            raise IOError(path)

        while True:
            d =  chirp_reli_readdir(dr)
            if d is None: break
            files.append(Stat(d.name, d.info))

        return files


    ##
    # Returns a Chirp.Stat object with information on path.
    # Throws an IOError on error (e.g., no such path or insufficient permissions).
    #
    # @param self                Reference to the current task object.
    # @param path                Target file/directory.
    # @param absolute_stop_time  If given, maximum number of seconds since
    #                            epoch to wait for a server response.
    #                            (Overrides any timeout.)
    # @param timeout             If given, maximum number of seconds to
    #                            wait for a server response.
    def stat(self, path, absolute_stop_time=None, timeout=None):
        info = chirp_wrap_stat(self.hostport, path, self.__stoptime(absolute_stop_time, timeout))

        if info is None:
            raise IOError(path)

        return Stat(path, info)


    ##
    # Copies local file/directory source to the chirp server as file/directory destination.
    # If destination is not given, source name is used.
    # Raises Chirp.TransferFailure on error.
    #
    # @param self                Reference to the current task object.
    # @param source              A local file or directory.
    # @param destination         File or directory name to use in the server (defaults to source).
    # @param absolute_stop_time  If given, maximum number of seconds since
    #                            epoch to wait for a server response.
    #                            (Overrides any timeout.)
    # @param timeout             If given, maximum number of seconds to
    #                            wait for a server response.
    def put(self, source, destination=None, absolute_stop_time=None, timeout=None):
        if destination is None:
            destination = source
        result = chirp_recursive_put(self.hostport,
                                     source, destination,
                                     self.__stoptime(absolute_stop_time, timeout))
        if(result > -1):
            return result

        raise TransferFailure('put', result, source, destination)


    ##
    # Copies server file/directory source to the local file/directory destination.
    # If destination is not given, source name is used.
    # Raises Chirp.TransferFailure on error.
    #
    # @param self                Reference to the current task object.
    # @param source              A server file or directory.
    # @param destination         File or directory name to be used locally (defaults to source).
    # @param absolute_stop_time  If given, maximum number of seconds since
    #                            epoch to wait for a server response.
    #                            (Overrides any timeout.)
    # @param timeout             If given, maximum number of seconds to
    #                            wait for a server response.
    def get(self, source, destination=None, absolute_stop_time=None, timeout=None):
        if destination is None:
            destination = source
        result = chirp_recursive_get(self.hostport,
                                     source, destination,
                                     self.__stoptime(absolute_stop_time, timeout))

        if(result > -1):
            return result

        raise TransferFailure('get', result, source, destination)


    ##
    # Removes the given file or directory from the server.
    # Raises OSError on error.
    #
    # @param self                Reference to the current task object.
    # @param path                Target file/directory.
    # @param absolute_stop_time  If given, maximum number of seconds since
    #                            epoch to wait for a server response.
    #                            (Overrides any timeout.)
    # @param timeout             If given, maximum number of seconds to
    #                            wait for a server response.
    def rm(self, path, absolute_stop_time=None, timeout=None):
        status = chirp_reli_rmall(self.hostport, path, self.__stoptime(absolute_stop_time, timeout))

        if status < 0:
            raise OSError(path)


##
# Python Stat object
#
# This class is used to record stat information for files/directories of a chirp server.
class Stat:
    def __init__(self, path, cstat):
        self._path = path
        self._info = cstat

    ##
    # Target path.
    #
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print s.path
    # @endcode
    @property
    def path(self):
        return self._path

    ##
    # ID of device containing file.
    #
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print s.device
    # @endcode
    @property
    def device(self):
        return self._info.cst_dev

    ##
    # inode number
    #
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print s.inode
    # @endcode
    @property
    def inode(self):
        return self._info.cst_ino

    ##
    # file mode permissions
    #
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print s.mode
    # @endcode
    @property
    def mode(self):
        return self._info.cst_mode

    ##
    # number of hard links
    #
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print s.nlink
    # @endcode
    @property
    def nlink(self):
        return self._info.cst_nlink

    ##
    # user ID of owner
    #
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print s.uid
    # @endcode
    @property
    def uid(self):
        return self._info.cst_uid

    ##
    # group ID of owner
    #
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print s.gid
    # @endcode
    @property
    def gid(self):
        return self._info.cst_gid

    ##
    # device ID if special file
    #
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print s.rdev
    # @endcode
    @property
    def rdev(self):
        return self._info.cst_rdev

    ##
    # total size, in bytes
    #
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print s.size
    # @endcode
    @property
    def size(self):
        return self._info.cst_size

    ##
    # block size for file system I/O
    #
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print s.block_size
    # @endcode
    @property
    def block_size(self):
        return self._info.cst_blksize

    ##
    # number of 512B blocks allocated
    #
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print s.blocks
    # @endcode
    @property
    def blocks(self):
        return self._info.cst_blocks

    ##
    # number of seconds since epoch since last access
    #
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print s.atime
    # @endcode
    @property
    def atime(self):
        return self._info.cst_atime

    ##
    # number of seconds since epoch since last modification
    #
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print s.mtime
    # @endcode
    @property
    def mtime(self):
        return self._info.cst_mtime

    ##
    # number of seconds since epoch since last status change
    #
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print s.ctime
    # @endcode
    @property
    def ctime(self):
        return self._info.cst_ctime

    def __repr__(self):
        return "%s uid:%d gid:%d size:%d" % (self.path, self.uid, self.gid, self.size)


class AuthenticationFailure(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class TransferFailure(Exception):
    def __init__(self, action, status, source, dest):
        self.action = action
        self.status = status
        self.source = source
        self.dest   = dest
    def __str__(self):
        return "Error with %s(%s) %s %s" % (self.action, self.status, self.source, self.dest)
