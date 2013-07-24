#!/usr/bin/env python

import os
import sys
import math
import tempfile
import warnings
import subprocess

def make_path(path):
  try:
    os.makedirs(path)
  except:
    pass

def find_gnuplot_version():
  return float(os.popen("gnuplot --version | awk '{print $2}'").read())

def source_exists(path):
  if not os.path.isdir(path):
    print "source directory does not exist"
    sys.exit(1)

def usage():
  print "\nUsage: " + __file__ + " data_path destination_path workflow_name"
  print "\nWhere:"
  print "\tdata_path\t\tThe path to the data recorded by the resource_monitor."
  print "\tdestination_path\tThe path in which to store the visualization."
  print "\tworkflow_name\t\tThe name of the workflow being visualized."
  print ""

def get_args():
  if len(sys.argv) == 2 and (sys.argv[1] == "-h" or sys.argv[1] == "--help"):
    usage()
    sys.exit(0)
  if len(sys.argv) != 4:
    usage()
    sys.exit(1)

  source = sys.argv[1]
  destination = sys.argv[2]
  name = sys.argv[3]
  return source, destination, name


def find_summary_paths(source):
  summary_paths = []
  for r, d, f in os.walk(source):
    for files in f:
      if files.endswith(".summary"):
        summary_paths.append(os.path.join(r, files))
  return summary_paths

def load_summaries_by_group(paths):
  groups = {}
  for sp in paths:
    data_stream = open(sp, 'r')
    summary = {}
    for line in data_stream:
      data = line.strip().split(':', 2)
      data = [x.strip() for x in data]
      key = data[0]
      value = data[1]
      if key == 'bytes_written' or key == 'bytes_read':
        value = str(scale_value(data[1], 'MB')) + ' MB'
      summary[key] = value

    summary['filename'] = os.path.basename(sp)

    group_name = summary.get('command').split(' ')[0]
    while group_name[0] == '.' or group_name[0] == '/':
      group_name = group_name[1:]

    if groups.get(group_name) == None:
      groups[group_name] = [summary]
    else:
      groups[group_name].append(summary)

    data_stream.close()
  return groups

def gnuplot(commands):
  child = subprocess.Popen("gnuplot", stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  child.stdin.write("%s\n" % commands)
  child.stdin.write("quit\n")
  child.stdin.flush()
  child.wait()

def fill_histogram_table_template(max_sorted, table_path, binwidth, data_path):
  n     = len(max_sorted)
  min_x = max_sorted[0]
  max_x = max_sorted[-1]

  nbins = math.floor((max_x - min_x)/binwidth) - 2
  result  = "set table \"" + table_path + "\"\n"
  result += "binwidth=" + str(binwidth) + "\n"

  if(n < 3 or nbins < 3):
    result += "binc(x,w)=x\n"
  else:
    result += "binc(x,w)=(w*floor(x/w) + 0.5)\n"

  result += "plot \"" + data_path + "\" using (binc($1,binwidth)):(1.0) smooth freq\n"
  result += "unset table\n\n"

  return result

def fill_histogram_template(max_sorted, width, height, image_path, binwidth, resource_name, unit, table_path):
  n     = len(max_sorted)
  min_x = max_sorted[0]
  max_x = max_sorted[-1]
  nbins = math.floor((max_x - min_x)/binwidth) - 2

  if nbins < 2:
    nbins = 1

  result  = "set terminal png transparent size " + str(width) + "," + str(height) + "\n"
  result += "plot \"" + table_path + "\" using 1:(stringcolumn(3) eq \"i\"? $2:1/0) w boxes\n"
  result += "min_x = GPVAL_DATA_X_MIN\n"
  result += "max_x = GPVAL_DATA_X_MAX\n"
  result += "min_y = GPVAL_DATA_Y_MIN\n"
  result += "max_y = GPVAL_DATA_Y_MAX\n"

  result += "reset"
  result += "set terminal png transparent size " + str(width) + "," + str(height) + "\n"
  result += "unset key\n"
  result += "unset border\n"
  result += "set border 3\n"
  result += "set output \"" + image_path + "\"\n"
  result += "binwidth=" + str(binwidth) + "\n"
  result += "set style fill solid 0.5\n"

  if(n < 3 or nbins < 3):
    result += "set boxwidth 2.0 relative\n"
  else:
    result += "set boxwidth 0.9*binwidth absolute\n"

  result += "set tics nomirror scale 0.25\n"
  result += "set yrange [0:(max_y + max_y*0.1)]\n"
  result += "set ytics  (max_y)\n"

  #handle corner cases for xrange
  if n == 1 and min_x < 0.0001:
    result += "set xrange [-0.1:1]\n"
    result += "set xtics (0, 1)\n"
  elif nbins < 3:
    result += "set xrange [0:(max_x + 0.25*max_x)]\n"
    result += "set xtics (0, \"" + str(int(round(max_x))) + "\" " + str(max_x) + ")\n"
  else:
    cent   = (max_x - min_x)/10.0
    quatr  = (max_x - min_x + cent)/3
    result += "set xrange [" + str(min_x - cent) + ":" + str(max_x + cent) + "]\n"
    result += "set xtics (%d, %d, %d, %d)\n" % (int(min_x), int(quatr + min_x), int(max_x - quatr), int(max_x))

  result += "set xlabel \"" + resource_name.replace('_', ' ')
  if unit != " ":
    result += " (" + unit + ")"
  result += "\"\n"

  result += "if (max_y < (100*min_y))"
  result += "set ylabel \"Frequency\"; "
  result += "plot \"" + table_path + "\" using 1:(stringcolumn(3) eq \"i\"? $2:1/0) w boxes;"
  result += "else "
  result += "set ylabel \"Frequency (log)\"; "
  result += "set ytics  (\"1\" 0, sprintf(\"%d\", max_y) log10(max_y)); "
  result += "set yrange [-0.001:(1.1*log10(max_y))]; "
  result += "plot \"" + table_path + "\" using 1:((stringcolumn(3) eq \"i\" && $2 > 0) ? log10($2):1/0) w boxes\n"
  return result

def rule_id_for_task(task):
  rule_id = task.get('filename').split('.')
  rule_id = rule_id[0].split('-')[-1]
  return rule_id

def resource_group_page(name, group_name, resource, width, height, tasks, out_path):
  page  = "<!doctype html>\n"
  page += "<meta name=\"viewport\" content=\"initial-scale=1.0, width=device-width\" />\n"
  page += '<link rel="stylesheet" type="text/css" media="screen, projection" href="../../css/style.css" />' + "\n"
  page += "<title>" + name + "</title>\n"
  page += "<div class=\"content\">\n"
  page += "<p> Showing <tt>" + resource.replace('_', ' ') + "</tt> for executable <tt>" + group_name + "</tt> in workflow <tt><a href=\"../../index.html\">" + name + "</a></tt>\n"
  page += "<img src=\"../" + resource + "_" + str(width) + "x" + str(height) + "_hist.png\" class=\"center\" />\n"
  page += "<table>\n"

  columns = min(4, len(tasks))
  header  = "<th>id</th><th>" + resource.replace('_', ' ') +  "</th>"

  page += "<tr>"
  page += header * columns
  page += "</tr>\n"

  comp = lambda x,y: cmp(float(x.get(resource).split(' ')[0]), float(y.get(resource).split(' ')[0]))
  sorted_tasks = sorted(tasks, comp, reverse=True)

  count = 0
  for d in sorted_tasks:
    if(count % columns == 0):
      page += "<tr>"

    rule_id = rule_id_for_task(d)
    page += "<td><a href=\"../" + rule_id + ".html\">" + rule_id + "</a></td><td>" + str(d.get(resource)) + "</td>"

    if(count % columns == (columns - 1)):
      page += "</tr>\n"

    count += 1

  page += "</table>\n"
  page += "</div>\n"

  index_path = out_path + "/" + resource
  make_path(index_path)
  index_path += "/" + "index.html"
  f = open(index_path, "w")
  f.write("%s\n" % page)
  f.close()

def compute_binwidth(maximum_value):
  if maximum_value > 40:
    binwidth = maximum_value/40.0
  else:
    binwidth = 1
  return binwidth

def compute_binwidth_iqr(maximums):
  n  = len(maximums) - 1
  q1 = maximums[int(math.ceil(n / 4.0))] 
  q3 = maximums[int(math.floor(3*n / 4.0))]

  if(q1 >= q3):
    return 1
  else:
    return 2*(q3 - q1)*math.pow(float(n), float(-1)/3)

def find_maximums(tasks, resource):
  maximums = []
  for d in tasks:
    maximums.append(d.get(resource))
  return maximums

def write_maximums(maximums, resource, group_name, base_directory):
  directory = base_directory + "/" + group_name
  make_path(directory)
  data_path = directory + "/" + resource
  f = open(data_path, "w")
  for m in maximums:
    f.write("%d\n" % m)
  f.close()
  return data_path

def scale_maximums(maximums, unit):
  result = []
  for m in maximums:
    m = scale_value(m, unit)
    result.append(m)
  return result

def task_has_timeseries(task, source_directory):
  base_name = task.get('filename').split('.')[0]
  timeseries_name = base_name + '.series'
  try:
    f = open(source_directory + "/" + timeseries_name)
    f.close()
  except:
    return None
  return timeseries_name

def fill_in_time_series_format(resource, unit, data_path, column, out_path, width=1250, height=500):
  if unit != " ":
    unit = ' (' + unit + ')'
  commands  = 'set terminal png transparent size ' + str(width) + ',' + str(height) + "\n"
  commands += "set bmargin 4\n"
  commands += "unset key\n"
  commands += 'set xlabel "Time (seconds)" offset 0,-2 character' + "\n"
  commands += 'set ylabel "' + resource.replace('_', ' ') + unit + '" offset 0,-2 character' + "\n"
  commands += 'set output "' + out_path + '"' + "\n"
  commands += "set yrange [0:*]\n"
  commands += "set xrange [0:*]\n"
  commands += "set xtics right rotate by -45\n"
  commands += "set bmargin 7\n"
  commands += 'plot"' + data_path + '" using 1:' + str(column) + ' w lines lw 5 lc rgb"#465510"' + "\n"
  return commands

def generate_time_series_plot(resource, unit, data_path, column, out_path, width, height):
  commands = fill_in_time_series_format(resource, unit, data_path, column, out_path, width, height)
  gnuplot(commands)

def scale_time_series(source_directory, data_file, units, aggregate_data, workspace):
  start = -1
  out_file_path = os.path.join(workspace, data_file + '.scaled')
  out_stream = open(out_file_path, 'w')
  data_stream = open(source_directory + '/' + data_file, 'r')
  for line in data_stream:
    if line[0] == '#':
      continue
    data = line.split()
    if start < 0:
      start = data[0]
    data[6] = str(scale_value(data[6] + ' B', 'GB'))
    data[7] = str(scale_value(data[7] + ' B', 'GB'))

    # store in aggregate_data
    key = round(int(data[0])/1000000)
    previous_values = aggregate_data.get(key, [0,0,0,0,0,0,0,0,0])
    for x in range(0,8):
      previous_values[x] = previous_values[x] + float(data[x+1])
    aggregate_data[key]  = previous_values

    data[0] = str((float(data[0]) - float(start))/10e5)
    out_stream.write("%s\n" % str.join(' ', data))
  data_stream.close()
  out_stream.close()
  return out_file_path, aggregate_data

def create_individual_pages(groups, destination_directory, name, resources, units, source_directory, workspace):
  aggregate_data = {}
  for group_name in groups:
    for task in groups[group_name]:
      timeseries_file = task_has_timeseries(task, source_directory)
      has_timeseries = False
      if timeseries_file != None:
        has_timeseries = True
        data_path, aggregate_data = scale_time_series(source_directory, timeseries_file, units, aggregate_data, workspace)
        column = 1
        for r in resources:
          out_path = destination_directory + '/' + group_name + '/' + r + '/' + rule_id_for_task(task) + '.png'
          if column > 1:
            generate_time_series_plot(r, units.get(r), data_path, column, out_path, 600, 300)
          column += 1
      page  = "<html>\n"
      page += '<link rel="stylesheet" type="text/css" media="screen, projection" href="../css/style.css" />' + "\n"

      page += "<p> Showing task <tt>" + rule_id_for_task(task) + "</tt> in workflow <tt><a href=\"../index.html\">" + name + "</a></tt><br><br>\n"

      page += "<table>\n"
      page += "<tr><td>command</td><td></td><td>" + task.get('command') + "</td></tr>\n"
      for r in resources:
        page += "<tr><td><a href=\"" + r + "/index.html\">" + r + "</a></td><td>" + task.get(r) + "</td>"
        if has_timeseries and r != 'wall_time':
          image_path = r + '/' + rule_id_for_task(task) + '.png'
          page += '<td><img src="' + image_path +'" /></td>'
        else:
          page += '<td></td>'
        page += "</tr>\n"
      page += "</html>\n"
      f = open(destination_directory + "/" + group_name + "/" + rule_id_for_task(task) + ".html", "w")
      f.write("%s\n" % page)
      f.close()
  return aggregate_data

def write_aggregate_data(data, resources, work_directory):
  sorted_keys = sorted(data.keys(), key=lambda x: float(x))
  start_time = float(sorted_keys[0])
  files = []
  for index, r in enumerate(resources):
    if index != 0:
      f = open(work_directory + '/' + r + '.aggregate', 'w')
      files.append(f)
  for k in sorted_keys:
    for index, f in enumerate(files):
      f.write("%s %d\n" % ((k-start_time), data.get(k)[index]))
  for f in files:
    f.close()

def create_aggregate_plots(resources, units, work_directory, destination_directory):
  for r in resources:
    unit = units.get(r)
    data_path = work_directory + '/' + r + '.aggregate'
    column = 2
    out_path = destination_directory + '/' + r + '_aggregate.png'
    commands = fill_in_time_series_format(r, unit, data_path, column, out_path, 1250, 500)
    gnuplot(commands)

def create_main_page(group_names, name, resources, destination, hist_height=600, hist_width=600, timeseries_height=1250, timeseries_width=500, has_timeseries=False):
  out_path = destination + "/index.html"
  f = open(out_path, "w")
  content  = "<!doctype html>\n"
  content += "<meta charset=\"UTF-8\">\n"
  content += '<meta name="viewport" content="initial-scale=1.0, width=device-width" />' + "\n"
  content += '<link rel="stylesheet" type="text/css" media="screen, projection" href="css/style.css" />' + "\n"
  content += '<title>' + name + "Workflow</title>\n"
  content += '<div class="content">' + "\n"
  content += '<h1>' + name + "Workflow</h1>\n"
  if has_timeseries:
    content += '<script src="http://ajax.googleapis.com/ajax/libs/jquery/1.7.1/jquery.min.js"></script>' + "\n"
    content += '<script src="js/slides.min.jquery.js"></script>' + "\n"
    content += '<script>' + "\n" + '  $(function(){' + "\n" + "    $('#slides').slides({ preload: true, });\n  });\n</script>\n"
    content += '<!-- javascript and some images licensed under Apache-2.0 by Nathan Searles (http://nathansearles.com/) -->' + "\n"
    content += '<section class="summary">' + "\n"
    content += '  <div id="slides">' + "\n"
    content += '    <div class="slides_container">' + "\n"
    for index, r in enumerate(resources):
      if index != 0:
        content += '<div class="slide"><div class="item"><img src = "' + r + '_aggregate.png" /></div></div>' + "\n"
    content += "</div>\n"
    content += '<a href="#" class="prev"><img src="img/arrow-prev.png" width="24" height="43" alt="Arrow Prev"></a>' + "\n"
    content += '<a href="#" class="next"><img src="img/arrow-next.png" width="24" height="43" alt="Arrow Next"></a>' + "\n"
    content += "  </div>\n</section>\n"

  for g in group_names:
    content += '<h2>' + g + "</h2>\n"
    for r in resources:
      content += '<a href="' + g + '/' + r + '/index.html"><img src="' + g + "/" + r + "_" + str(hist_width) + "x" + str(hist_height) + '_hist.png" /></a>\n'
    content += "<hr />\n\n"
  content += "</div>\n"
  f.write("%s\n" % content)
  f.close()

def to_base(value, unit):
  prefix = unit[0]
  if   prefix == "K":
    value *= 1024
  elif prefix == "M":
    value *= 1024**2
  elif prefix == "G":
    value *= 1024**3
  elif prefix == "T":
    value *= 1024**4
  return value

def to_target(value, target):
  prefix = target[0]
  if   prefix == "K":
    value /= 1024
  elif prefix == "M":
    value /= 1024**2
  elif prefix == "G":
    value /= 1024**3
  elif prefix == "T":
    value /= 1024**4
  return value

def scale_value(initial, target_unit=" "):
  value, unit = initial.split(' ', 2)
  value = float(value)
  unit = unit.strip()

  v = to_target(to_base(value, unit), target_unit)

  return v


def main():
  warnings.filterwarnings("ignore", category=DeprecationWarning)

  GNUPLOT_VERSION = find_gnuplot_version()

  visualizer_home = os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))

  (source_directory,
  destination_directory,
  name) = get_args()

  source_exists(source_directory)

  make_path(destination_directory)

  workspace = tempfile.mkdtemp('rmv')

  try:
    summary_paths = find_summary_paths(source_directory)

    resource_units = {"wall_time": "s",
                      "cpu_time": "s",
                      "max_concurrent_processes": " ",
                      "virtual_memory":  "MB",
                      "resident_memory": "MB",
                      "swap_memory":     "MB",
                      "bytes_read":      "MB",
                      "bytes_written":   "MB",
                      "workdir_num_files": " ",
                      "workdir_footprint": "MB"
                     }

    resources = [ "wall_time",
                  "cpu_time",
                  "max_concurrent_processes",
                  "virtual_memory",
                  "resident_memory",
                  "swap_memory",
                  "bytes_read",
                  "bytes_written",
                  "workdir_num_files",
                  "workdir_footprint"
                ]

    print "Reading summaries..."
    groups = load_summaries_by_group(summary_paths)

    print "Plotting summaries histograms..."
    hist_large = 600
    hist_small = 250
    for r in resources:
      unit = resource_units.get(r)
      for group_name in groups:
        maximums = find_maximums(groups[group_name], r)
        maximums = scale_maximums(maximums, unit)
        data_path = write_maximums(maximums, r, group_name, workspace)

        out_path = destination_directory + "/" + group_name
        make_path(out_path)

        max_sorted = sorted(maximums)
        binwidth = compute_binwidth_iqr(max_sorted)

        table_path = out_path + "/" + r + "_hist.dat"
        gnuplot_format = fill_histogram_table_template(max_sorted, table_path, binwidth, data_path)
        gnuplot(gnuplot_format)

        image_path = out_path + "/" + r + "_" + str(hist_large) + "x" + str(hist_large) + "_hist.png"
        gnuplot_format = fill_histogram_template(max_sorted, hist_large, hist_large, image_path, binwidth, r, unit, table_path)
        gnuplot(gnuplot_format)

        image_path = out_path + "/" + r + "_" + str(hist_small) + "x" + str(hist_small) + "_hist.png"
        gnuplot_format = fill_histogram_template(max_sorted, hist_small, hist_small, image_path, binwidth, r, unit, table_path)
        gnuplot(gnuplot_format)

        resource_group_page(name, group_name, r, hist_large, hist_large, groups[group_name], out_path)

    aggregate_height = 500
    aggregate_width = 1250
    aggregate_data = create_individual_pages(groups, destination_directory, name, resources, resource_units, source_directory, workspace)

    time_series_exist = False
    if aggregate_data != {}:
      print "Aggregating time series..."
      time_series_exist = True
      write_aggregate_data(aggregate_data, resources, workspace)
      print "Plotting time series..."
      create_aggregate_plots(resources, resource_units, workspace, destination_directory)

    create_main_page(groups.keys(), name, resources, destination_directory, hist_small, hist_small, aggregate_height, aggregate_width, time_series_exist)
    
    lib_static_home = os.path.normpath(os.path.join(visualizer_home, 'lib/resource_monitor_visualizer_static'))
    os.system("cp -r " + lib_static_home + "/* " + destination_directory)

  finally:
    print "Cleaning up..."
    os.system("rm -rf " + workspace)


if __name__ == "__main__":
  main()

