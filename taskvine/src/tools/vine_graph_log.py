#!/usr/bin/env python

# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import sys
import re
import os
import getopt
from subprocess import Popen, PIPE, check_output
from functools import reduce

gnuplot_cmd   = 'gnuplot'
format        = 'svg'
extension     = format
resolution    = 30          # this many seconds to one log entry. Default is 30 seconds.
x_range       = None        # If unspecified, then plot all the valid range.
geometry      = "600,480"   # width,height in pixels

log_entries   = None
times         = None

unit_labels   = {'s' : 'seconds', 'm' : 'minutes', 'h' : 'hours', 'd' : 'days', 'GB' : 'GB', 'i' : '' }
unit_factors  = {'s' : 1, 'm' : 60, 'h' : 3600, 'd' : 86400, 'GB' : 1073741824}
x_units       = 'm'          # Default is minutes.

def read_fields(file, lines_patience = 10):
    for line in file:
        if line[0] != '#':
            lines_patience = lines_patience - 1
        else:
            return line.strip('#\n\r\t ').split()
        if lines_patience < 1:
            break
    sys.stderr.write("Could not find fields descriptions (a line such as # timestamp workers_....)\n")
    sys.exit(1)

def time_to_resolution(t):
    return (t - (t % (resolution * 1000000))) / 1000000

def time_field_p(field):
    return re.search('^time_.*$', field)

def read_log_entries(file, fields):
    log_entries = {}
    idxs  = range(0, len(fields))
    pairs = list(zip(idxs, fields))
    epoch = None
    count_lines = 0
    prev_line = None

    for line in file:
        count_lines = count_lines + 1
        try:
            numbers = [float(x) for x in line.split()]
            record  = {}

            for (i, field) in pairs:
                if field == 'timestamp':
                    record['time_step'] = time_to_resolution(numbers[i])
                    if not epoch:
                        epoch = numbers[i]/1000000
                    record['timestamp'] = numbers[i]/1000000 - epoch
                elif time_field_p(field):
                    record[field] = numbers[i]/1000000
                else:
                    record[field] = numbers[i]

            if prev_line and record['time_step'] == prev_line['time_step']:
                continue

            log_entries[record['timestamp']] = record

            if(prev_line is None):
                prev_line = record

            delta = {}
            for key in record.keys():
                delta[key] = record[key] - prev_line[key];

            total_time = reduce( lambda x,y: x+y, [ delta[m] for m in ['time_status_msgs', 'time_internal', 'time_polling', 'time_send', 'time_receive', 'time_application']])

            # One of time_* may be larger than the given resolution, and here we force everything to the same height
            delta['timestamp'] = total_time

            res_factor = 100;
            if delta['timestamp'] > 0:
                record['stack_time_status_msgs'] = res_factor * (delta['time_status_msgs'] / delta['timestamp'])
                record['stack_time_internal']    = res_factor * (delta['time_internal']    / delta['timestamp']) + record['stack_time_status_msgs']
                record['stack_time_polling']     = res_factor * (delta['time_polling']     / delta['timestamp']) + record['stack_time_internal']
                record['stack_time_send']        = res_factor * (delta['time_send']        / delta['timestamp']) + record['stack_time_polling']
                record['stack_time_receive']     = res_factor * (delta['time_receive']     / delta['timestamp']) + record['stack_time_send']
                record['stack_time_application'] = res_factor * (delta['time_application'] / delta['timestamp']) + record['stack_time_receive']
                record['stack_time_other']       = res_factor
            else:
                record['stack_time_status_msgs'] = 0
                record['stack_time_internal']    = 0
                record['stack_time_polling']     = 0
                record['stack_time_send']        = 0
                record['stack_time_receive']     = 0
                record['stack_time_application'] = 0
                record['stack_time_other']       = 0

            record['bytes_sent'] /= unit_factors['GB']
            record['bytes_received'] /= unit_factors['GB']
            record['bytes_transfered'] = record['bytes_sent'] + record['bytes_received']

            prev_line = record

        except ValueError:
            sys.stderr.write('Line %d has an invalid value. Ignoring.\n' % (count_lines, ))
            continue
        except IndexError:
            sys.stderr.write('Line %d has less than %d fields. Aborting.\n' % (count_lines, len(fields)))
            sys.exit(1)

    return log_entries

def sort_time(log_entries):
    times = []
    for k in log_entries.keys():
        times.append(k)
    times.sort()
    return times

def pout(file, str):
    file.write("{}\n".format(str).encode('utf-8'))

class WQPlot:
    def __init__(self, title, ylabel, fields, labels=None, x_units = x_units, y_units = x_units, range = x_range, stack = False):
        self.title   = title
        self.fields  = fields
        self.labels  = labels or self.fields
        self.x_units = x_units
        self.y_units = y_units
        self.ylabel  = ylabel
        self.range   = range
        self.stack   = stack

    def preamble(self, file):
        self.__preamble_common(file)

    def __preamble_common(self, file):
        pout(file, """
set term {fmt} size {geo} butt linewidth 1
set title  '{title}'
set xlabel 'manager lifetime in {x_units}'
set ylabel '{y_units}'
set yrange [0:]
set noborder
set tics nomirror out
""".format(fmt=format, geo=geometry, title=self.title, x_units=unit_labels[self.x_units], y_units=self.ylabel))

        if self.stack:
            pout(file, "set key below\n")
        else:
            pout(file, "set key left top\n")

        intervals = [len(log_entries.keys())/x for x in [19,17,13,11,7,5,3]]

        pout(file, """
set style line 1 pt 5   lc rgb '#1b9e77' pointinterval {0}
set style line 2 pt 13  lc rgb '#d95f02' pointinterval {1}
set style line 3 pt 7   lc rgb '#7570b3' pointinterval {2}
set style line 4 pt 9   lc rgb '#e7298a' pointinterval {3}
set style line 5 pt 10  lc rgb '#66a61e' pointinterval {4}
set style line 6 pt 2   lc rgb '#e6ab02' pointinterval {5}
set style line 7 pt 1   lc rgb '#a6761d' pointinterval {6}
""".format(*intervals))

        if self.range:
            pout(file, 'set xrange [%s]' % (self.range,))

    def __data_one_time_field(self, file, field):
        time_scale = unit_factors[self.x_units]
        # if a time field, then scale
        mod = time_field_p(field) and unit_factors[self.y_units] or 1

        for t in times:
            r = log_entries[t]
            try:
                pout(file, '%lf %lf' % (t/time_scale, r[field]/mod))
            except KeyError:
                sys.stderr.write("Field '%s' does not exist in the log\n" % (field,))
                break
        pout(file, 'EOF')

    def plot_line(self, label, place):
        return "'-' using 1:2 title '%s' with linespoints ls %d lw 3" % (label,place+1)

    def plot_stack(self, label, place):
        return "'-' using 1:2 title '%s' with fillsteps fs solid 1.0 noborder ls %d" % (label,place+1)

    def write_plot(self, file):
        self.preamble(file)

        plots = []
        for i in range(len(self.labels)):
            if self.stack:
                plots.append(self.plot_stack(self.labels[i], i))
            else:
                plots.append(self.plot_line(self.labels[i], i))

        pout(file, 'plot %s;' % (',\\\n'.join(plots),))

        for field in self.fields:
            self.__data_one_time_field(file, field)

    def __plot_internal(self, output, command):
        sys.stdout.write("Generating '%s'.\n" % (output,))
        fout = open(output, 'w')
        gnuplot = Popen(command, stdin = PIPE, stdout = fout)
        self.write_plot(gnuplot.stdin)
        gnuplot.stdin.close()
        gnuplot.wait()

    def plot(self, output):
        try:
            self.__plot_internal(output, command = gnuplot_cmd)
        except IOError:
            sys.stderr.write("Could not generate file %s.\n" % (output,))
            exit(1)
        except OSError:
            sys.stderr.write("Could not execute '%s'. Please try again specifying -c <gnuplot-path>, or -Ttext\n" % (gnuplot_cmd, ))
            exit(1)


class WQPlotLog(WQPlot):
    def preamble(self, file):
        WQPlot.preamble(self, file)
        pout(file, 'set logscale y')
        pout(file, '')

def check_gnuplot_version(gnuplot_cmd):
        try:
            version = check_output([gnuplot_cmd, '--version'])
            version = version.decode("utf-8")
            result = re.match('gnuplot\s+(\d+\.\d+)\D', version)

            if not result:
                sys.stderr.write("Could not determine gnuplot version.")
                exit(1)

            version = result.group(1)

            if float(version) < 4.6:
                sys.stderr.write("gnuplot is version %s, at least version 4.6 is required." % (version,))
                exit(1)

        except OSError:
            sys.stderr.write("Could not execute '%s'. Please try again specifying -c <gnuplot-path>, or -Ttext\n" % (gnuplot_cmd, ))
            exit(1)

def show_usage():
    usage_string = """
{command} [options] <work-queue-log>
        -h                  This message.
        -c <gnuplot-path>   Specify the location of the gnuplot executable.
                            Default is gnuplot.
        -o <prefix-output>  Generate prefix-output.{{workers,workers-accum,
                                                    tasks,tasks-accum,
                                                    time-manager,time-workers,
                                                    transfer,cores}}.{format}
                            Default is <work-queue-log>.
        -r <range>          Range of time to plot, in time units (see -u) from
                            the start of execution. Of the form: min:max,
                            min:, or :max.
        -s <seconds>        Sample log every <seconds> (default is {format}).
        -u <time-unit>      Time scale to output. One of s,m,h or d, for seconds,
                            minutes (default), hours or days.
        -T <output-format>  Set output format. Default is {format}.
                            If \'text\', then the gnuplot scripts are written
                            instead of the images.
        -g <width,height>   Size of each plot. Default is {geometry}.
""".format(command=os.path.basename(sys.argv[0],), format=format, resolution=resolution, geometry=geometry)
    print(usage_string)


if __name__ == '__main__':

    try:
        optlist, args = getopt.getopt(sys.argv[1:], 'c:g:ho:r:s:T:u:')
    except getopt.GetoptError as e:
        sys.stderr.write(str(e) + '\n')
        show_usage()
        sys.exit(1)

    if len(args) < 1:
        show_usage()
        sys.exit(1)

    logname = args[0]
    prefix  = logname

    for opt, arg in optlist:
        if   opt == '-c':
            gnuplot_cmd = arg
        elif opt == '-o':
            prefix = arg
        elif opt == '-h':
            show_usage()
            sys.exit(0)
        elif opt == '-r':
            x_range = arg
        elif opt == '-s':
            resolution = float(arg)
        elif opt == '-g':
            geometry = arg
        elif opt == '-T':
            if arg == 'text':
                gnuplot_cmd = 'cat'
                extension   = format + '.gnuplot'
            else:
                format    = arg
                extension = format
        elif opt == '-u':
            if arg in unit_factors:
                x_units = arg
            else:
                sys.stderr.write("Time scale factor '%s' is not valid. Options: s,m,h or d.\n"  % (arg,))
                exit(1)

    try:

        if extension is not (format + '.gnuplot'):
            check_gnuplot_version(gnuplot_cmd)

        with open(logname) as file:
            log_entries = read_log_entries(file, read_fields(file))
            times       = sort_time(log_entries)

        plots = {}
        plots['workers'] = WQPlot(x_units = x_units, ylabel = 'workers', range = x_range,
                title = "workers instantaneous counts",
                fields = ['workers_connected', 'workers_idle', 'workers_busy'],
                labels = ['connected', 'idle', 'busy'])

        plots['workers-accum'] = WQPlot(x_units = x_units, ylabel = 'workers', range = x_range,
                title = "workers cumulative counts",
                fields = ['workers_joined', 'workers_removed', 'workers_released', 'workers_slow', 'workers_idled_out', 'workers_lost'],
                labels = ['joined', 'removed', 'released', 'slow', 'idled out,', 'lost'])

        plots['tasks'] = WQPlot(x_units = x_units, ylabel = 'tasks', range = x_range,
                title = "tasks instantaneous counts",
                fields = ['tasks_waiting', 'tasks_on_workers', 'tasks_running', 'tasks_with_results'],
                labels = ['waiting', 'on workers', 'running', 'with results'])

        plots['tasks-capacities'] = WQPlot(x_units = x_units, ylabel = 'tasks', range = x_range,
                title = "tasks instantaneous capacities",
                fields = ['tasks_running', 'capacity_instantaneous', 'capacity_weighted'],
                labels = ['tasks running', 'tasks capacity raw', 'tasks capacity weighted'])

        plots['tasks-accum'] = WQPlot(x_units = x_units, ylabel = 'tasks', range = x_range,
                title = "tasks cumulative counts",
                fields = ['tasks_submitted', 'tasks_dispatched', 'tasks_done', 'tasks_failed', 'tasks_cancelled', 'tasks_exhausted_attempts'],
                labels = ['submitted', 'dispatched', 'done', 'failed', 'cancelled', 'exhausted attempts'])

        plots['time-manager'] = WQPlot(x_units = x_units, ylabel = unit_labels[x_units], range = x_range,
            title = "cumulative times at the manager",
            fields = ['time_send', 'time_receive', 'time_polling', 'time_status_msgs', 'time_internal', 'time_application'],
            labels = ['send', 'receive', 'manager polling', 'manager status msgs', 'manager internal', 'manager application'])

        plots['time-workers'] = WQPlot(x_units = x_units, y_units = 'h', ylabel = unit_labels['h'], range = x_range,
            title = "cumulative times at workers",
            fields = ['time_execute', 'time_execute_good', 'time_execute_exhaustion'],
            labels = ['execute', 'execute good', 'execute exhaustion'])

        plots['transfer'] = WQPlot(x_units = x_units, y_units = 'GB', ylabel = 'GB', range = x_range,
            title = "manager data transfer",
            fields = ['bytes_sent', 'bytes_received'],
            labels = ['sent', 'received'])

        plots['times-stacked'] = WQPlot(x_units = x_units, y_units = 's', ylabel = 'utilization per time sample (%)', range = x_range, stack = True,
                title = 'manager time proportions',
                fields = ['stack_time_other', 'stack_time_application', 'stack_time_receive', 'stack_time_send', 'stack_time_polling', 'stack_time_internal', 'stack_time_status_msgs'],
                labels = ['other', 'application', 'receive', 'send', 'polling', 'internal', 'status msgs'])

        for name in plots.keys():
            plots[name].plot(prefix     + '.' + name + '.' + extension)

    except IOError:
        sys.stderr.write("Could not open file %s\n" % (logname,))
        sys.exit(1)

# vim: tabstop=8 shiftwidth=4 softtabstop=4 expandtab shiftround autoindent
