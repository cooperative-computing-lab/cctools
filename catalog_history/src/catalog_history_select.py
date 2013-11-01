#!/usr/bin/env python

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This program reads gets catalog history data from disk.
# It accepts arguments: catalog history directory, starting point, ending point.
# It outputs the checkpoint at the starting point, followed by all deltas until the ending point.

import sys
import os
import time
import datetime


day_length = 3600*24   # 1 day


debug = 0
def logDebug(str):
  if debug:
    logError(str)
def logError(str):
  sys.stderr.write(str+"\n")
def badSyntax(str):
  sys.stderr.write(str+"\n\n")
  sys.stderr.write("Please use the following syntax:\n")
  sys.stderr.write("catalog_history_select [source_directory] [starting_point] [ending_point]\n\n")
  sys.stderr.write("See man page for more details and examples.\n\n")
  sys.exit(0)
  
def timeToFileName(ts, days_offset=0):
  ts += (3600*24)*days_offset
  tt = datetime.datetime.fromtimestamp(ts).timetuple()
  dayOfYear = tt.tm_yday
  year = tt.tm_year
  path = directory+str(year)+'/'+str(dayOfYear)
  return path


# Get source directory
if len(sys.argv)>1 and sys.argv[1]:
  arg = sys.argv[1]
  if (arg[0]=='.'):
    pass
    directory = os.getcwd()+'/'+arg+'/'
  else:
    directory = arg+'/'
  if not os.path.isdir(directory):
    badSyntax('Source directory does not exist...')
else:
  directory = os.getcwd()+'/'


# Get starting point
begin = 0
if len(sys.argv)>2 and sys.argv[2]:
  arg = sys.argv[2]
  if (len(arg)>=14 and len(arg)<=19):
    datearr = time.strptime(arg, '%Y-%m-%d-%H-%M-%S')
    dt = datetime.datetime(*(datearr[0:6]))
    begin = int( time.mktime(dt.timetuple()))
  else:
    begin = int(arg)
  filename = timeToFileName(begin,True)
else:
  datearr = datetime.date(datetime.date.today().year, 1, 1)
  begin = int( time.mktime(datearr.timetuple()))


# Get ending point
end = sys.maxint
if len(sys.argv)>3 and sys.argv[3]:
  arg = sys.argv[3]
  if (len(arg)>=14 and len(arg)<=19):
    dt = datetime.datetime(*(time.strptime(arg, '%Y-%m-%d-%H-%M-%S')[0:6]))
    end = int( time.mktime(dt.timetuple()) )
  elif (arg[0]=='y'):
    end = begin + int(arg[1:])*31557600
  elif (arg[0]=='w'):
    end = begin + int(arg[1:])*604800
  elif (arg[0]=='d'):
    end = begin + int(arg[1:])*86400
  elif (arg[0]=='h'):
    end = begin + int(arg[1:])*3600
  elif (arg[0]=='m'):
    end = begin + int(arg[1:])*60
  elif (arg[0]=='s'):
    end = begin + int(arg[1:])
  elif (arg[0]=='+'):
    end = begin + int(arg[1:])
  else:
    end = int(arg)
else:
  end = begin + day_length

logDebug('Start:'+str(begin)+' End:'+str(end))

seriesNow = {}
seriesLog = []
  
# Read the initial checkpoint file and group data by the value of the "key" field
# Go a day earlier (day_offset=-1) to handle for auto-deleted series' which start reporting again after not appearing in the checkpoint
day = -1
filename = timeToFileName(begin,day)
try:
  checkpoint_file = open(filename+'.ckpt', 'r')
  lastHost = None
  seriesTmp = {}
  for line in checkpoint_file.readlines():
    line = line.strip()
    if line.__len__() <= 0:
      seriesNow[seriesTmp["key"]] = seriesTmp
      seriesTmp = {}
      lastHost = None
    else:
      key, value = line.split(' ',1)
      seriesTmp[key] = value
except IOError:
  logDebug('File does not exist? '+filename)
if 'checkpoint_file' in locals():
  checkpoint_file.close()

# Read each of the log files and add the data to the appropriate series.
prevLines = [] #used for debugging bad lines
pastStartTime = 0
pastEndTime = 0
while True:

  lastHost = None
  line_num = 0
  
  try:
    logDebug(filename+'.log')
    f = open(filename+'.log', 'r')
    for line in f:
      
      line_num += 1
      line = line.strip()
      if debug:
        prevLines.append(line) #used for debugging bad lines
      if 1:
        res = line.split(' ')
        format = res.pop(0)
        
        if pastStartTime==0:

          # Timestamp for any proceeding events
          if format == 'T' and res.__len__()>=1:
            time = res.pop(0)
            logTime = int(time)
          # Create a new host series
          elif format == 'C' and res.__len__()>=1:
            key = res.pop(0)
            lastHost = key
            seriesTmp = {}
          # Delete entire host series
          elif format == 'D' and res.__len__()>=1:
            host = res.pop(0)
            if host in seriesNow:
              del seriesNow[host]
            else:
              logError("v---------Ignoring delete for non-existant host series------------")
              logError(line)
              logError("^-------------Perhaps the host was already deleted--------")
          # Update a single field
          elif format == 'U' and res.__len__()>=3:
            host = res.pop(0)
            field = res.pop(0)
            value = " ".join(res)
            if host in seriesNow:
              seriesNow[host][field] = value
            else:
              logError("v-------------Ignoring update for non-existant host series------------")
              logError(line)
              logError("^----------Perhaps the host was deleted and is still reporting--------")
               
          # Record removal of a single field
          elif format == 'R' and res.__len__()>=2:
            host = res.pop(0)
            field = res.pop(0)
            if host in seriesNow:
              if field in seriesNow[host]:
                del seriesNow[host][field]

          elif lastHost is not None:
            if res.__len__() > 0 or format.__len__()>0:
              field = format
              value = " ".join(res)
              seriesTmp[field] = value
            else:
              seriesNow[lastHost] = seriesTmp
              lastHost = None
          else:
            logError("v-----------------Error on last line below ("+filename+':'+str(line_num)+")----------------")
            logError('\n'.join(prevLines))
            logError("^------------------------------End of error------------------------------")
            
          if prevLines.__len__()>20:
            prevLines.pop(0)
            
          if pastStartTime==0 and logTime>=begin:
            pastStartTime = 1
            for host in seriesNow:
              for key in seriesNow[host]:
                print key+' '+seriesNow[host][key]
              print ""
            print ""
            print line
            #del seriesNow
          
        else: #pastStartTime==1
          if format == 'T' and res.__len__()>=1:
            time = res.pop(0)
            logTime = int(time)
          if logTime<end:
            print line
          else:
            pastEndTime = 1
            break

      if debug and prevLines.__len__()>20:
        prevLines.pop(0)
  except IOError:
    logDebug('File does not exist? '+filename+'.log')
    if 'logTime' not in locals():
      logTime = begin
    logTime += 3600*24
    
  if 'f' in locals():
    f.close()

  if logTime>end or pastEndTime:
    break
  day += 1
  filename = timeToFileName(begin,day)




