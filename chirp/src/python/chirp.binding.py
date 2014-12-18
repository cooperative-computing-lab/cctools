import time
import os

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
        max_id_len = 1024
        identity=' ' * max_id_len
        chirp_reli_whoami(self.host, identity, max_id_len, self.__stoptime(absolute_stop_time, timeout))
        return identity.strip()

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


class AuthenticationFailure(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

            
#print CChirp.chirp_recursive_put(host, 'text.txt', 'text.chirp', time.time() + 15)
#print Chirp.chirp_recursive_put(host, 'text.txt', 'text.chirp', time.time() + 15)
#print Chirp.chirp_recursive_get(host, 'text_get.txt', 'text_get.chirp', time.time() + 15)

