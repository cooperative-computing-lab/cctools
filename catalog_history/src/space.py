import sys
import os
import time
import datetime

checkpoint_size = 216273
percent_used_timestamps = 0.763356481
seconds_per_year = 365*24*3600
checkpoints_per_year = seconds_per_year*percent_used_timestamps

log_space_per_second = 356.970485794

#space_for_only_checkpoints = checkpoints_per_year*checkpoint_size
#print space_for_only_checkpoints/1000000



checkpoint_span = 1
while (checkpoint_span<seconds_per_year):
  space_for_checkpoints = checkpoint_size * (seconds_per_year/checkpoint_span)
  space_for_log = checkpoint_size * (seconds_per_year/checkpoint_span)
  log_space_between_checkpoints = log_space_per_second * checkpoint_span
  print("%d\t%d\t%f") % (checkpoint_span, (space_for_checkpoints), log_space_between_checkpoints)
  checkpoint_span *= 2
  
