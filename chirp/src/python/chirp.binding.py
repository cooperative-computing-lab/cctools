import os
import time

cctools_debug_config('chirp')
cctools_debug_flags_set('chirp')

class Client:
    def __init__(self, host, timeout=60, authentication=None, tickets=None):
        self.host    = host
        self.timeout = timeout

        if tickets and (authentication is None):
            authentication = ['tickets']

        self.set_tickets(tickets)

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

    def set_tickets(self, tickets):
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

    def whoami(self, absolute_stop_time=None, timeout=None):
        return chirp_wrap_whoami(self.host, self.__stoptime(absolute_stop_time, timeout))

    def listacl(self, path='/', absolute_stop_time=None, timeout=None):
        acls = chirp_wrap_listacl(self.host, path, self.__stoptime(absolute_stop_time, timeout))

        if acls is None:
            raise IOError(path)
        else:
            return acls.split('\n')

    def ls(self, path, absolute_stop_time=None, timeout=None):
        dr    = chirp_reli_opendir(self.host, path, self.__stoptime(absolute_stop_time, timeout))
        files = []
        if dir is not None:
            while True:
                d =  chirp_reli_readdir(dr)
                if d is None: break
                files.append(Stat(d.name, d.info))
        else:
            raise IOError(path)
        return files

    def stat(self, path, absolute_stop_time=None, timeout=None):
        info = chirp_wrap_stat(self.host, path, self.__stoptime(absolute_stop_time, timeout))
        return Stat(path, info)

    def put(self, source, destination=None, absolute_stop_time=None, timeout=None):
        if destination is None:
            destination = source
        chirp_recursive_put(self.host,
                            source, destination,
                            self.__stoptime(absolute_stop_time, timeout))

    def get(self, source, destination=None, absolute_stop_time=None, timeout=None):
        if destination is None:
            destination = source
        chirp_recursive_get(self.host,
                            source, destination,
                            self.__stoptime(absolute_stop_time, timeout))

    def rm(self, path, absolute_stop_time=None, timeout=None):
        status = chirp_reli_rmall(self.host, path, self.__stoptime(absolute_stop_time, timeout))

        if status < 0:
            raise IOError(path)

class Stat:
    def __init__(self, path, cstat):
        self._path = path
        self._info = cstat

    @property
    def path(self):
        return self._path

    @property
    def device(self):
        return self._info.cst_dev

    @property
    def inode(self):
        return self._info.cst_ino

    @property
    def mode(self):
        return self._info.cst_mode

    @property
    def nlink(self):
        return self._info.cst_nlink

    @property
    def uid(self):
        return self._info.cst_uid

    @property
    def gid(self):
        return self._info.cst_gid

    @property
    def rdev(self):
        return self._info.cst_rdev

    @property
    def size(self):
        return self._info.cst_size

    @property
    def block_size(self):
        return self._info.cst_blksize

    @property
    def blocks(self):
        return self._info.cst_blocks

    @property
    def atime(self):
        return self._info.cst_atime

    @property
    def mtime(self):
        return self._info.cst_mtime

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
