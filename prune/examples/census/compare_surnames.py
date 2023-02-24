#!/usr/bin/env python

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.
import os, sys, random, time

from prune import client
from os.path import expanduser


HOME = expanduser("~")
prune = client.Connect(base_dir = HOME+'/.prune') #Prune data is stored in base_dir


years = [3000,3010]


###############################
##########  STAGE 0 ##########
###############################
print('Stage 0')

folder = './simulated_data/'
f = []
for (dirpath, dirnames, filenames) in os.walk(folder):
    f.extend(filenames)
    break
f.sort()

normalized_data_keys = {}
for year in years:
    normalized_data_keys[year] = []
for fname in f:
    year = int(fname[0:4])
    if year in years:
        original_file_id = prune.file_add( folder+fname )
        normalized_data_keys[year].append( original_file_id )
        print('Input: ' + original_file_id + ' (' + folder+fname + ')')




###############################
##########  STAGE 1-2 #########
###############################
# Stage 1 = Decompression
# Stage 2 = Normalization
# These stages are currently unnecessary with the simulated census data


###############################
##########  STAGE 3 ##########
###############################

counts = {}
for year in years:
    counts[year] = []

print('Stage 3')

# Count words ocurrences in the data
counter = prune.file_add( 'count' )
for year in years:
    for k,nkey in enumerate(normalized_data_keys[year]):
        cmd = "python count < input_data_%i_%i > output" % (year,k)
        ckey, = prune.task_add( returns=['output'],
            env=prune.nil, cmd=cmd,
            args=[counter, nkey], params=['count','input_data_%i_%i'%(year,k)] )
        counts[year].append( ckey )

        # prune.file_dump( counts[year][-1], 'count%i.txt'%year )

for year in years:
    print('counts[%i] = %s' % (year, counts[year]))


##############################
##########  STAGE 4 ##########
##############################


print('Stage 4')

# Summarize words ocurrence counts by year
countsummer = prune.file_add( 'count_sum' )
for year in years:
    ar = counts[year]
    ar2 = ['input'+str(i) for i in range(1,len(ar)+1)]
    cmd = "python count_sum.%i %s > output" % (year, ' '.join(ar2))
    skey, = prune.task_add( returns=['output'],
        env=prune.nil, cmd=cmd,
        args=[countsummer]+ar, params=['count_sum.%i'%year]+ar2 )
    counts[year] = skey

    #prune.file_dump( counts[year], 'counts_%i.txt'%year )


for year in years:
    print('counts[%i] = \'%s\'' % (year, counts[year]))






##############################
##########  STAGE 5 ##########
##############################

frequencies = {}

print('Stage 5')

# Summarize total words ocurrence counts
countsummer = prune.file_add( 'count_sum' )
ar = []
for year in years:
    ar += [counts[year]]
ar2 = ['input'+str(i) for i in range(1,len(ar)+1)]
cmd = "python count_sum.all %s > output" % (' '.join(ar2))
print(cmd)
counts_all, = prune.task_add( returns=['output'],
                    env=prune.nil, cmd=cmd,
                    args=[countsummer]+ar, params=['count_sum.all']+ar2 )


###### Execute the workflow ######
prune.execute( worker_type='local', cores=8 )
# prune.execute( worker_type='work_queue', name='prune_census_example' )

prune.file_dump( counts_all, 'counts_all.txt' )

print('counts_all = \'%s\'' % (counts_all))






##############################
##########  STAGE 6 ##########
##############################

fields = ['CY','CS','CC','CT','HS','FM','PN', 'FN','GN','BY','BP','SX', 'RL','ET','RC','AG']
filtered = {}

print('Stage 6')

for field in fields:
    # Filter into field types
    filtered[field], = prune.task_add( returns=['output_data'],
                        #env=umbrella_env, cmd="grep '^%s' /tmp/input > output_data" % (field),
                        env=prune.nil, cmd="grep '^%s' input > output_data" % (field),
                        args=[counts_all], params=['input'] )
    #prune.file_dump( filtered[field], 'filtered_%s.txt' % (field) )



for field in fields:
    print('filtered[\'%s\'] = \'%s\'' % (field, filtered[field]))






##############################
##########  STAGE 7 ##########
##############################

frequent_values = {}

print('Stage 7')

for field in fields:
    frequent_values[field], = prune.task_add( returns=['output'],
                        env=prune.nil, cmd="sort -t\| -rnk3  input%s > output"%field,
                        args=[filtered[field]], params=['input'+field] )

for field in fields:
    print('frequent_values[\'%s\'] = \'%s\'' % (field, frequent_values[field]))


###### Execute the workflow ######
prune.execute( worker_type='local', cores=8 )
# prune.execute( worker_type='work_queue', name='prune_census_example' )

prune.file_dump( frequent_values['FN'], 'most_frequent_FN.txt' )





##############################
##########  STAGE 8 ##########
##############################
print('Stage 8')

zipped_jellyfish_folder = prune.file_add( 'jellyfish.tar.gz' )
jelly_env = prune.envi_add(engine='wrapper', open='tar -zxf jellyfish.tar.gz', close='rm -rf jellyfish', args=[zipped_jellyfish_folder], params=['jellyfish.tar.gz'])

final_keys = []
compare_words = prune.file_add( 'compare_word' )

max_comparison = 25
all_top_matches = []
for i in range(0, max_comparison):
    cmd = "python compare_word input_data %i  > output_data" % (i)
    print(cmd)
    top_matches, = prune.task_add( returns=['output_data'],
                            env=jelly_env, cmd=cmd,
                            args=[frequent_values['FN'],compare_words], params=['input_data','compare_word'] )
    all_top_matches.append( top_matches )
    #print top_matches
    prune.file_dump( top_matches, '%s_similarities_%i.txt' % ('FN',i) )
    #final_keys.append( top_matches )
    #if i == 9131:
    #   prune.file_dump( top_matches, 'tops/%ssimilarities_%i.txt' % ('FN',i) )


###### Execute the workflow ######
prune.execute( worker_type='local', cores=8 )
# prune.execute( worker_type='work_queue', name='prune_census_example' )

###### Save output data to local directory ######
prune.export( all_top_matches[0], 'similarity_results.%i.txt'%(0) )

