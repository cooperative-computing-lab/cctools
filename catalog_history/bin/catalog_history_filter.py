#!/usr/bin/python
import sys
import json
import re

# Iota operator

sep = re.compile('([!=<>]+)')

dynamic = 0

conditions = {}
if len(sys.argv)>1:
  for arg in sys.argv[1:]:
    if (arg=='-static'):
      dynamic = 0
    if (arg=='-dynamic'):
      dynamic = 1
    else:
      key,op,val = sep.split(arg)
      conditions[key] = [op,val]
else:
  print "Please add an argument like key=<hostname>"+str(len(sys.argv))

checkPoint = {}
eventLog = []
status = {}


def logError(str):
  pass
  #sys.stderr.write(str+"\n")

"""
def newValueFound(field,value):
    if host in ignores:
      ignore = 1
    elif field in conditions
      ignore = shouldBeIgnored(field,value)
      if ignore==1:
        status[lastHost]['ignoreList'].append(status[lastHost]['creation'])
        del status[lastHost]['creation']
        if host in checkPoint
          del checkPoint[host]
    if ignore==0:
      eventLog.append(line)
"""


def shouldIncludeSeries(field,value):
  global conditions
  include = 0
  if field in conditions:
    op, val = conditions[field]
    #val = str(val)
    #value = str(value)
    if (op=='=' or op=='=='):
      if value==val:
        include = 1
    elif (op=='!=' or op=='!=='):
      if value != val:
        include = 1
    elif (op=='>='):
      if int(value)>=int(val):
        include = 1
    elif (op=='<='):
      if int(value)<=int(val):
        include = 1
    elif (op=='>'):
      if int(value)>int(val):
        include = 1
    elif (op=='<'):
      if int(value)<int(val):
        include = 1
    #print value+' '+conditions[field][0]+' '+val+' Include? '+str(include)
  return include

def shouldBeIgnored(field,value):
  global conditions
  ignore = 0
  if field in conditions:
    op, val = conditions[field]
    #val = str(val)
    #value = str(value)
    if (op=='=' or op=='=='):
      if value==val:
        ignore = 1
    elif (op=='!=' or op=='!=='):
      if value != val:
        ignore = 1
    elif (op=='>='):
      if int(value)>=int(val):
        ignore = 1
    elif (op=='<='):
      if int(value)<=int(val):
        ignore = 1
    elif (op=='>'):
      if int(value)>int(val):
        ignore = 1
    elif (op=='<'):
      if int(value)<int(val):
        ignore = 1
    #print value+' '+conditions[field][0]+' '+val+' Ignore? '+str(ignore)
  return ignore

def flushEventLog():
  global eventLog
  timeLine = None
  #if (len(eventLog)>1):
  #  print eventLog
  for line in eventLog:
    if (len(line)>2 and line[0]=='T' and line[1]==' '):
      timeLine = line
    #elif (line[0]=='C' and line[1]==' '):
    #  timeLine = line
    else:
      if timeLine is not None:
        print timeLine
        timeLine = None
      print line
  eventLog = []




if dynamic==0:
  prevLines = [] #used for debugging bad lines
  checkpoint_done = 0
  seriesTmp = {}
  createTmp = []
  blankLine = 1
  include = 0
  for line in sys.stdin:
    line = line.strip()
    prevLines.append(line) #used for debugging bad lines
      
    if checkpoint_done==0:
      # Read in checkpoint data
      line = line.strip()
      if line.__len__() <= 0:
        if blankLine:
          checkpoint_done = 1
          
          for host in checkPoint:
            for key in checkPoint[host]:
              print key+' '+checkPoint[host][key]
            print ""
          print ""
          #checkPoint = {}

        else:
          lastHost = seriesTmp["key"]
          if include>0:
            checkPoint[lastHost] = seriesTmp
          seriesTmp = {}
          lastHost = None
          include = 0
        blankLine = 1
        
      else:
        blankLine = 0
        try:
          key, value = line.split(' ',1)
          include += shouldIncludeSeries(key,value)
          seriesTmp[key] = value
        except ValueError:
          sys.stderr.write("Error on line containing: "+line+"\n")
          sys.stderr.write(str(prevLines)+"\n")
      
    
    else:
      # Read in log data
      #print "================================="+line+"==============================="
      res = line.split(' ')
      format = res.pop(0)
      ignoreS = 1
      
      # Timestamp for any proceeding events
      if format == 'T' and res.__len__()>=1:
        flushEventLog()
        time = res.pop(0)
        logTime = int(time)
        eventLog.append(line)
      # Create a new host series
      elif format == 'C' and res.__len__()>=1:
        key = res.pop(0)
        lastHost = key
        if shouldIncludeSeries('key',lastHost):
          checkPoint[lastHost] = {'key':lastHost}
          ignoreS = 0
        createTmp = [line]
      # Delete entire host series
      elif format == 'D' and res.__len__()>=1:
        host = res.pop(0)
        if host in checkPoint:
          eventLog.append(line)
      # Update a single field
      elif format == 'U' and res.__len__()>=3:
        host = res.pop(0)
        field = res.pop(0)
        value = " ".join(res)

        #print host+' not included'
        if host in checkPoint:
          eventLog.append(line)
          
        else:
          logError("v-------------Ignoring update for non-existant host series------------")
          logError(line)
          logError("^----------Perhaps the host was deleted and is still reporting--------")
           
      # Record removal of a single field
      elif format == 'R' and res.__len__()>=2:
        host = res.pop(0)
        if host in checkPoint:
          eventLog.append(line)
          
      elif lastHost is not None:
        
        if res.__len__() > 0 or format.__len__()>0:
          createTmp.append(line)
          field = format
          value = " ".join(res)
          if shouldIncludeSeries(field,value):
              ignoreS = 0
        else:
          if ignoreS==0:
            lastHost = None
            for line in createTmp:
              eventLog.append(line)
            eventLog.append("")
            createTmp = None
        
      else:
        logError("v-----------------Error on last line below ("+str(logTime)+")----------------")
        logError('\n'.join(prevLines))
        logError("^------------------------------End of error------------------------------")
        
      if prevLines.__len__()>20:
        prevLines.pop(0)
      



  flushEventLog()





else: #Dynamic==1
  prevLines = [] #used for debugging bad lines
  checkpoint_done = 0
  seriesTmp = {}
  createTmp = []
  blankLine = 1
  ignore = 0
  for line in sys.stdin:
    line = line.strip()

    if checkpoint_done==0:
      # Read in checkpoint data
      line = line.strip()
      if line.__len__() <= 0:
        if blankLine:
          checkpoint_done = 1
        else: 
          lastHost = seriesTmp["key"]
          if ignore==0:
            checkPoint[lastHost] = seriesTmp
            status[lastHost] = {'creation':1, 'ignore':[]}
          else:
            #print "Ignoring "+lastHost
            status[lastHost] = {'ignore':[1]}
          seriesTmp = {}
          lastHost = None
          ignore = 0
        blankLine = 1
        
      else:
        blankLine = 0
        key, value = line.split(' ',1)
        ignore += shouldBeIgnored(key,value)
        seriesTmp[key] = value
      
    
    else:
      # Read in log data
      prevLines.append(line) #used for debugging bad lines
      #print line
      res = line.split(' ')
      format = res.pop(0)
      ignore = 0
      
      # Timestamp for any proceeding events
      if format == 'T' and res.__len__()>=1:
        time = res.pop(0)
        logTime = int(time)
      # Create a new host series
      elif format == 'C' and res.__len__()>=1:
        key = res.pop(0)
        lastHost = key
        if shouldBeIgnored('key',lastHost):
          if lastHost in status:
            status[lastHost]['ignore'].append(logTime)
          else:
            status[lastHost] = {'ignore':[logTime]}
        else:
          if lastHost in status:
            status[lastHost]['creation'] = logTime
          else:
            status[lastHost] = {'creation':logTime, 'ignore':[]}
        ignore = 1
        createTmp = [line]
      # Delete entire host series
      elif format == 'D' and res.__len__()>=1:
        host = res.pop(0)
        if host in status:
          if 'creation' in status[host]:
            pass
          else:
            ignore = 1
        else:
          ignore = 1
      # Update a single field
      elif format == 'U' and res.__len__()>=3:
        host = res.pop(0)
        field = res.pop(0)
        value = " ".join(res)



        if host in status:
          if 'creation' in status[host]:
            if shouldBeIgnored(field,value):
              #print "Ignoring; "+line
              status[host]['ignore'].append(status[host]['creation'])
              del status[host]['creation']
              if host in checkPoint:
                del checkPoint[host]
              ignore = 1
            #else:
              #print "Not Ignoring; "+line
              #eventLog.append(line)
          else:
            ignore = 1
            
          
          
        else:
          logError("v-------------Ignoring update for non-existant host series------------")
          logError(line)
          logError("^----------Perhaps the host was deleted and is still reporting--------")
           
      # Record removal of a single field
      elif format == 'R' and res.__len__()>=2:
        host = res.pop(0)
        if host in status:
          if 'creation' in status[host]:
            pass
          else:
            ignore = 1
        else:
          ignore = 1
      elif lastHost is not None:
        
        if res.__len__() > 0 or format.__len__()>0:
          if lastHost in status:
            if 'creation' in status[lastHost]:
              createTmp.append(line) 
              field = format
              value = " ".join(res)
              if shouldBeIgnored(field,value):
                status[lastHost]['ignore'].append(status[lastHost]['creation'])
                del status[lastHost]['creation']
                if lastHost in checkPoint:
                  del checkPoint[lastHost]
                createTmp.append(line)
              
            else:
              ignore = 1
          else:
            ignore = 1
            print "Something happened that shouldn't happen code:123847169"
        else:
          if lastHost in status:
            if 'creation' in status[lastHost]:
              lastHost = None
              for line in createTmp:
                eventLog.append(line)
              eventLog.append("")
              createTmp = []
        ignore = 1
        
      else:
        logError("v-----------------Error on last line below ("+str(logTime)+")----------------")
        logError('\n'.join(prevLines))
        logError("^------------------------------End of error------------------------------")
        
      if prevLines.__len__()>20:
        prevLines.pop(0)
      
      if ignore==0:
        eventLog.append(line)

  for host in checkPoint:
    for key in checkPoint[host]:
      print key+' '+checkPoint[host][key]
    print ""
  print ""


  timeLine = None
  for line in eventLog:
    if (len(line)>2 and line[0]=='T' and line[1]==' '):
      timeLine = line
    #elif (line[0]=='C' and line[1]==' '):
    #  timeLine = line
    else:
      if timeLine is not None:
        print timeLine
        timeLine = None
      print line




