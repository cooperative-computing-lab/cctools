#!/usr/bin/python

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver script compiler """

from weaver.script import Script

import sys

if __name__ == '__main__':
    s = Script(sys.argv[1:])
    s.compile()

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
