#!/usr/bin/python
import sys
import json
import re

# Pi operator

sep = re.compile('([A-Z]+)?\.([A-Z]+)?\@?([^\@]*)')

sys.stderr.write(str()+"\n")

output_vars = []
varList = {'key':1}
if len(sys.argv)>2:
  for arg in sys.argv[2:]:
    p1,interop,intraop,key,p2 = sep.split(arg)
    if (len(interop)>0 and len(key)>0):
      output_vars.append({'interop':interop, 'intraop':intraop, 'key':key})
      varList[key] = 1
else:
  print "Please add an argument"+str(len(sys.argv))
  sys.exit(1)

#report_frequency = 3600   # 1 hour
#report_frequency = 86400   # 1 day
#report_frequency = 31536000   # 1 year
report_frequency = int(sys.argv[1])

lastReport = 0
seriess = {}


def logError(str):
  pass
  #sys.stderr.write(str+"\n")

def handleReport():
  global output_vars, seriess
  #print "Time to report........................."
  output = [str(reportNumber)]
  for var in output_vars:
    value = 0
    interop = var['interop']
    intraop = var['intraop']
    field = var['key']
    intermediateRes = []
    #print seriess
    for host in seriess:
      if field in seriess[host]:
        if (is_array(seriess[host][field])):
          l = seriess[host][field]
          if (intraop=='AVG'):
            if len(l)>1:
              intraRes = sum(l[1:]) / len(l[1:])
            else:
              intraRes = l[0]
          elif (intraop=='MAX'):
            if len(l)>1:
              intraRes = max(l[1:])
            else:
              intraRes = l[0]
          elif (intraop=='MIN'):
            if len(l)>1:
              intraRes = min(l[1:])
            else:
              intraRes = l[0]
          elif (intraop=='LAST'):
            intraRes = l[-1]
          elif (intraop=='FIRST'):
            if len(l)>1:
              intraRes = l[1]
            else:
              intraRes = l[0]
          elif (intraop=='INC'):
            if len(l)>1:
              first = l[0]
              last = None
              intraRes = 0
              for val in l:
                if (last is not None and val<last):
                  intraRes += (last-first)
                  first = 0
                  last = val
                else:
                  last = val
              if (last is not None):
                intraRes += (last-first)
            else:
              intraRes = 0
          elif (intraop=='LIST'):
            intraRes = l[1:]
          elif (intraop=='COUNT'):
            intraRes = len(l)-1
          else:
            intraRes = l[1:]
        else:
          if (intraop=='COUNT'):
            intraRes = 0
          elif (intraop=='INC'):
             intraRes = 0
          else:
            intraRes = seriess[host][field]
        intermediateRes.append(intraRes)
      #else:
        #Field does not exist
    if (interop=='SUM'):
      #print intermediateRes
      output.append( str(sum(intermediateRes)) )
    else:
      output.append( str(intermediateRes) )
  for var in output_vars:
    field = var['key']
    for host in seriess:
      for field in seriess[host]:
        if (is_array(seriess[host][field])):
          seriess[host][field] = [seriess[host][field][-1]]

  print '\t'.join(output)


is_array = lambda var: isinstance(var, (list, tuple))  


prevLines = [] #used for debugging bad lines
checkpoint_done = 0
seriesTmp = {}
blankLine = 1

beginReport = None
endReport = None
varSet = {}

dynamicFields = []
staticFields = []

reportNumber = None
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
        seriess[lastHost] = seriesTmp
        seriesTmp = {}
        lastHost = None
        ignore = 0
      blankLine = 1
      
    else:
      blankLine = 0
      key, value = line.split(' ',1)
      try:
        value = long(value)
      except ValueError:
        pass
      if key in varList:
        seriesTmp[key] = value
    
  else:
    #print '=================='+line+'====================='
    # Read in log data
    prevLines.append(line) #used for debugging bad lines
    #print line
    res = line.split(' ')
    format = res.pop(0)
    
    # Timestamp for any proceeding events
    if format == 'T' and res.__len__()>=1:
      time = res.pop(0)
      logTime = int(time)
      #print logTime
      if beginReport is None:
        beginReport = logTime
        endReport = logTime+report_frequency
        reportNumber = 0
      while (logTime>endReport):
        reportNumber += 1
        handleReport()
        beginReport = endReport + 1
        endReport += report_frequency
        
    # Create a new host series
    elif format == 'C' and res.__len__()>=1:
      lastHost = res.pop(0)
      if 'key' in varList:
        seriesTmp = {'key':lastHost}
    # Delete entire host series
    elif format == 'D' and res.__len__()>=1:
      host = res.pop(0)
      #seriess[host] = {}
      #print line
    # Update a single field
    elif format == 'U' and res.__len__()>=3:
      host = res.pop(0)
      field = res.pop(0)
      if field in dynamicFields:
        pass
      else:
        dynamicFields.append(field)
      if field in varList:
        value = " ".join(res)
        try:
          value = long(value)
        except ValueError:
          pass

        if host in seriess:
          if field in seriess[host]:
            if (is_array(seriess[host][field])):
              seriess[host][field].append(value)
            else:
              seriess[host][field] = [seriess[host][field],value]
          else:
            seriess[host][field] = [value]

        else:
          logError("v-------------Ignoring update for non-existant host series------------")
          logError(line)
          logError("^----------Perhaps the host was deleted and is still reporting--------")
        #print seriess[host][field]
         
    # Record removal of a single field
    elif format == 'R' and res.__len__()>=2:
      host = res.pop(0)
      field = res.pop(0)
      #if field in varList:
      #  if host in seriess:
      #    if field in seriess[host]:
      #      del seriess[host][field]
    elif lastHost is not None:
      
      if res.__len__() > 0 or format.__len__()>0:
        field = format
        if field in staticFields:
          pass
        else:
          staticFields.append(field)
        if field in varList:
          value = " ".join(res)
          try:
            value = int(value)
          except ValueError:
            pass
          seriesTmp[field] = value
          
      else:
        seriess[lastHost] = seriesTmp
        lastHost = None

    else:
      logError("v-----------------Error on last line below ("+str(logTime)+")----------------")
      logError('\n'.join(prevLines))
      logError("^------------------------------End of error------------------------------")
      
    if prevLines.__len__()>20:
      prevLines.pop(0)
    

reportNumber += 1
handleReport()

#print "#Static Fields: "+str(staticFields)
#print "#Dynamic Fields: "+str(dynamicFields)




#print seriess

"""
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
"""




