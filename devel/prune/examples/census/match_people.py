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
        print 'Input: ' + original_file_id + ' (' + folder+fname + ')'




###############################
##########  STAGE 1-2 #########
###############################
# Stage 1 = Decompression
# Stage 2 = Normalization
# These stages are currently unnecessary with the simulated census data



##############################
##########  STAGE 3 ##########
##############################

#fields = ['CY','CS','CC','CT','HS','FM','PN', 'FN','GN','BY','BP','SX', 'RL','ET','RC','AG']
#numbers = ['CY', 'HS','FM','PN', 'BY', 'AG']  # These fields should get sorted numerically (not alpha-numerically)

#concurrency = 256
concurrency = 16


returns = []
for i in range(0,concurrency):
    returns.append('result_'+str(i))
key_sorted_data = {}
for year in years:
    key_sorted_data[year] = []
    for i in range(0,concurrency):
        key_sorted_data[year].append( [] )

print('Stage 3')


map_all = prune.file_add( 'map_all' )
for year in years:

    for u,ukey in enumerate(normalized_data_keys[year]):

        param = 'input_%i_%i' % (year, u)
        cmd = "python map_all %i < %s " % (concurrency, param)
        sorteds = prune.task_add( returns=returns,
            env=prune.nil, cmd=cmd,
            args=[map_all,ukey], params=['map_all',param] )

        for i in range(0,concurrency):
            key_sorted_data[year][i].append( sorteds[i] )

for year in years:
    print 'key_sorted_data[%i] = %s' % (year, key_sorted_data[year])


###### Execute the workflow ######
# prune.execute( worker_type='local', cores=8 )
prune.execute( worker_type='work_queue', name='prune_census_example' )

prune.export( key_sorted_data[3000][1][1], '3000.1.1.txt' )




##############################
##########  STAGE 4 ##########
##############################

year_blocks = []
for u in range(0,concurrency):
    year_ar = {}
    for year in years:
        year_ar[ year ] = []
    year_blocks.append( year_ar )

print('Stage 4')

for u in range(0,concurrency):

    # year_args = []
    # year_params = []

    for year in years:
        all_args = []
        all_params = []
        for j,file in enumerate(key_sorted_data[year][u]):
            all_args.append( file )
            all_params.append( 'input_%i_%i_%i'%(u,year,j) )
        cmd = "sort -m -t\| -k1 input_* > output_%i_%i " % (u, year)
        full_year, = prune.task_add( returns=['output_%i_%i'%(u, year)],
            env=prune.nil, cmd=cmd,
            args=all_args, params=all_params )

        year_blocks[ u ][ year ] = full_year

        # #print cmd
        # #print all_params

        # year_args.append( full_year )
        # year_params.append( 'input_%i_%i'%(u,year) )

    # cmd = "sort -m -t\| -k1 -nk3 input_* > output_%i " % (u)
    # all_key, = prune.task_add( returns=['output_%i'%(u)],
    #   env=prune.nil, cmd=cmd,
    #   args=year_args, params=year_params )

    # blocks.append( all_key )
    # #print cmd
    # #print year_params

    # #prune.file_dump( all_key, 'all_key_sample_%i.txt'%u )

###### Execute the workflow ######
# prune.execute( worker_type='local', cores=8 )
prune.execute( worker_type='work_queue', name='prune_census_example' )

prune.export( year_blocks[ 0 ][ 3000 ], 'year_blocks.%i.%i.txt'%(0,3000) )





##############################
##########  STAGE 5 ##########
##############################

blocks = []

print('Stage 5')

for u in range(0,concurrency):
    blocks.append( {} )
    for y1 in range(0,len(years)):
        year1 = years[y1]
        for y2 in range(y1+1,len(years)):
            year2 = years[y2]
            ykey = year1+year2

            cmd = "sort -m -t\| -k1 -nk3 input_* > output_%i " % (u)
            all_key, = prune.task_add( returns=['output_%i'%(u)],
                env=prune.nil, cmd=cmd,
                args=[ year_blocks[u][year1], year_blocks[u][year2] ], params=['input_%i_%i'%(u,year1),'input_%i_%i'%(u,year2)] )

            # cmd = "sort -m -t\| -k1 -nk3 input_* > output_%i_%i" % (u,ykey)
            # all_key, = prune.task_add( returns=['output_%i_%i'%(u,ykey)],
            #   env=prune.nil, cmd=cmd,
            #   args=[ year_blocks[u][year1], year_blocks[u][year2] ], params=['input_%i_%i'%(u,year1),'input_%i_%i'%(u,year2)] )

            blocks[u][ykey] = all_key

            print 'blocks[%i][%i] = %s' % (u, ykey, all_key)



###### Execute the workflow ######
# prune.execute( worker_type='local', cores=8 )
prune.execute( worker_type='work_queue', name='prune_census_example' )

prune.export( blocks[ 0 ][ 6010 ], 'blocks.0.6010.txt' )





##############################
##########  STAGE 6 ##########
##############################

grouped_blocks = []

print('Stage 6')

dups = prune.file_add( 'dups' )
for u in range(0,concurrency):
    grouped_blocks.append( {} )
    for y1 in range(0,len(years)):
        year1 = years[y1]
        for y2 in range(y1+1,len(years)):
            year2 = years[y2]
            ykey = year1+year2

            cmd = "python dups < input_%i_%i > output_%i_%i" % (u,ykey, u,ykey)
            block_grouped, = prune.task_add( returns=['output_%i_%i'%(u,ykey)],
                env=prune.nil, cmd=cmd,
                args=[dups,blocks[u][ykey]], params=['dups','input_%i_%i'%(u,ykey)] )

            grouped_blocks[u][ykey] = block_grouped

            print 'grouped_blocks[%i][%i] = %s' % (u, ykey, block_grouped)


###### Execute the workflow ######
# prune.execute( worker_type='local', cores=8 )
prune.execute( worker_type='work_queue', name='prune_census_example' )

prune.export( grouped_blocks[ 1 ][ 6010 ], 'grouped_blocks.1.6010.txt' )





##############################
##########  STAGE 7 ##########
##############################

matched_blocks = []
final_ids = []
print('Stage 7')

matches = prune.file_add( 'matches' )
for u in range(0,concurrency):
    matched_blocks.append( {} )
    for y1 in range(0,len(years)):
        year1 = years[y1]
        for y2 in range(y1+1,len(years)):
            year2 = years[y2]
            ykey = year1+year2

            cmd = "python matches < input_%i_%i > output_%i_%i" % (u,ykey, u,ykey)
            block_matches, = prune.task_add( returns=['output_%i_%i'%(u,ykey)],
                env=prune.nil, cmd=cmd,
                args=[matches,grouped_blocks[u][ykey]], params=['matches','input_%i_%i'%(u,ykey)] )

            final_ids.append( block_matches )

print('final_ids = %s' % (final_ids))


###### Execute the workflow ######
# prune.execute( worker_type='local', cores=8 )
prune.execute( worker_type='work_queue', name='prune_census_example' )

###### Save output data to local directory ######
prune.export( final_ids[1], 'final_ids.1.txt' )

