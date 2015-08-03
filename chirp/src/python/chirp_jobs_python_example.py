#!/usr/bin/env cctools_python
# CCTOOLS_PYTHON_VERSION 2.7 2.6 2.5 2.4 3

import sys

try:
    import Chirp
except ImportError:
    print 'Could not find Chirp module. Please set PYTHONPATH accordingly.'
    sys.exit(1)

def make_job(job_number):
    job = { 'executable' : './my_script',
            'arguments'  : ['my_script', str(job_number)],
            'files'      : [ { 'task_path':  'my_script',
                               'serv_path':  '/my_script_org',
                               'type':  'INPUT',
                             },
                             { 'task_path':  'my.output',
                               'serv_path':  "/my." + str(job_number) + ".output",
                               'type':  'OUTPUT' } ] }
    return job


if __name__ == '__main__':

    if(len(sys.argv) != 3):
        print "Usage: %s HOST:PORT executable" % sys.argv[0]
        sys.exit(1)

    hostport   = sys.argv[1]
    executable = sys.argv[2]

    try:
        client = Chirp.Client(hostport,
                              authentication = ['unix'],
                              timeout = 15,
                              debug   = True)
    except Chirp.AuthenticationFailure, e:
         print "Could not authenticate using: %s" % ', '.join(e.value)
         sys.exit(1)

    print 'Chirp server sees me, ' + client.identity

    jobs = [ make_job(x) for x in range(1,10) ]

    client.put(executable, '/my_script_org')

    jobs_running = 0
    for job in jobs:
        job_id = client.job_create(job)
        client.job_commit(job_id)
        jobs_running += 1

    while(jobs_running > 0):
        job_states = client.job_wait(5)

        for state in job_states:
            print 'job ' + str(state['id']) + ' is ' + state['status'] + "\n"

            try:
                if   state['status'] == 'FINISHED':
                    client.job_reap( state['id'] )
                elif state['status'] == 'ERRORED':
                    client.job_kill( state['id'] )
            except Chirp.ChirpJobError:
                print 'error trying to clean job ' + str(state['id'])

            jobs_running -= 1

    sys.exit(0)
