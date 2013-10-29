#!/usr/bin/python
import sys
import json
import time
import datetime

directory = '/var/tmp/catalog_data/'

# First checkpoint "18.ckpt" starts at 1358553600
first_file_number = 18
first_timestamp = 1358553600

#Default starting point for testing (There are bugs earlier)
#first_file_number = 23
#first_timestamp = 1358985600


debug = 0

begin = 0
startDay = None
if len(sys.argv)>1 and sys.argv[1]:
  arg = sys.argv[1]
  if (arg[0]=='F'):
    startDay = int(arg[1:])
    if (startDay>18):
      begin = 1358553600 + ( (startDay-18) * 86400 )
    else:
      startDay = 18
      begin = first_timestamp
  elif (len(arg)>=14 and len(arg)<=19):
    dt = datetime.datetime.strptime(arg, '%Y-%m-%d-%H-%M-%S')
    begin = int( time.mktime(dt.timetuple()) )
  else:
    begin = int(arg)
if begin<first_timestamp:
  begin = first_timestamp



end = sys.maxint
if len(sys.argv)>2 and sys.argv[2]:
  arg = sys.argv[2]
  if (arg[0]=='F'):
    lastDay = int(arg[1:])
    if (lastDay>18):
      end = 1358553599 + ( (lastDay-18+1) * 86400 )
  elif (len(arg)>=14 and len(arg)<=19):
    dt = datetime.datetime.strptime(arg, '%Y-%m-%d-%H-%M-%S')
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
    begin = int(arg)
else:
  end = begin + default_length


if (startDay is None):
  startDay = int( (begin-first_timestamp)/86400 ) + first_file_number

#sys.stderr.write('Starting with day '+str(startDay)+"\n")
#sys.stderr.write("Timestamp: "+str(begin)+"..."+str(end)+"\n")
#sys.stderr.write("TimeSpan: "+str(end-begin)+"\n")
#sys.exit(1)

seriesNow = {}
seriesLog = []

def logError(str):
  pass
  #sys.stderr.write(str+"\n")
  
  
# Read the initial checkpoint file and group data by the value of the "key" field
try:
  checkpoint_file = open(directory+str(startDay)+'.ckpt', 'r')
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
      
finally:
  checkpoint_file.close()


# Read each of the log files and add the data to the appropriate series.
prevLines = [] #used for debugging bad lines
pastStartTime = 0
pastEndTime = 0
day = startDay
while True:

  #try:
    #log_file = open(str(day)+'.log', 'r')
    #for line in log_file.readlines():
  logTime = 0
  lastHost = None
  
  line_num = 0
  with open(directory+str(day)+'.log', 'r') as f:
    for line in f:
      
      line_num += 1
      line = line.strip()
      if debug:
        prevLines.append(line) #used for debugging bad lines
      if 1:
        res = line.split(' ')
        format = res.pop(0)
        
        if pastStartTime==0:
          
          #print line
          #print res.__len__()

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
              lastHost = None
          else:
            logError("v-----------------Error on last line below ("+str(day)+':'+str(line_num)+")----------------")
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

    
  if pastEndTime:
    break
  day += 1
  if day>=239:
    break





