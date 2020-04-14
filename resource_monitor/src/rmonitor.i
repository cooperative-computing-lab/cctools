/* ResourceMonitor.i */

/* Copyright (C) 2016- The University of Notre Dame This software is
 * distributed under the GNU General Public License.  See the file COPYING for
 * details. */

%module resource_monitor

%begin %{
	#define SWIG_PYTHON_2_UNICODE
%}

%{
	#include "debug.h"
	#include "int_sizes.h"
	#include "timestamp.h"
	#include "category_internal.h"
	#include "category.h"
	#include "rmonitor_poll.h"
	#include "rmsummary.h"
%}

%typemap(in) off_t = long long int;
%typemap(in) pid_t = int;

%extend rmsummary {
    rmsummary() {
        return rmsummary_create(-1);
    }

    ~rmsummary() {
        rmsummary_delete($self);
    }

%pythoncode %{
    def to_dict(self):
        d = {}
        for k in ['category', 'command', 'taskid',
                'start', 'end',
                'exit_type', 'signal', 'exit_status', 'last_error',
                'wall_time', 'total_processes', 'max_concurrent_processes', 'cpu_time',
                'virtual_memory', 'memory', 'swap_memory',
                'bytes_read', 'bytes_written', 'bytes_sent', 'bytes_received', 'bandwidth',
                'total_files', 'disk',
                'cores', 'cores_avg', 'gpus', 'machine_load', 'machine_cpus']:
            v = getattr(self, k)
            if v is None or (isinstance(v, int) and v < 0):
                continue
            else:
                d[k] = v
        for k in ['limits_exceeded', 'peak_times']:
            v = getattr(self, k)
            if v:
                d[k] = v.to_dict()
        return d

    @classmethod
    def from_dict(cls, pairs):
        rm = rmsummary()
        for k in pairs.keys():
            v = pairs[k]
            if k in ['limits_exceeded', 'peak_times']:
                v = rmsummary.from_dict(v)
            elif isinstance(v, float):
                v = int(v)
            try:
                setattr(rm, k, v)
            except KeyError:
                pass
        return rm

    def __getstate__(self):
        return self.to_dict()

    def __setstate__(self, pairs):
        oth = rmsummary.from_dict(pairs)
        self.__init__()
        rmsummary_merge_max(self, oth)
        setattr(self, 'limits_exceeded', rmsummary_copy(oth.limits_exceeded))
        setattr(self, 'peak_times', rmsummary_copy(oth.limits_exceeded))
%}
}

/* vdebug() takes va_list as arg but SWIG can't wrap such functions. */
%ignore vdebug;
%ignore debug;

%include "stdint.i"
%include "debug.h"
%include "int_sizes.h"
%include "timestamp.h"
%include "category_internal.h"
%include "category.h"
%include "rmonitor_poll.h"
%include "rmsummary.h"

