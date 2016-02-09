# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, time, traceback
import dataset
import glob
from class_item import *
from utils import *
from db_sqlite import Database

#db = dataset.connect( 'sqlite:///mydatabase.db' )
glob.ready = True
db = Database()

it = Item(type='file',path='./utils.py')

print it

it = db.insert( it )

print it
