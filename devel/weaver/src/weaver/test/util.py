# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver utility test units """

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + '/../..'))

from unittest import TestCase, TestLoader, TestSuite, TextTestRunner

from weaver.compat import map

from weaver.util import WeaverError
from weaver.util import find_executable
from weaver.util import normalize_path
from weaver.util import parse_string_list

# Find executable test case ----------------------------------------------------

class FindExecutableTestCase(TestCase):

    def test_00_cat(self):
        self.assertEqual(find_executable('cat'), '/bin/cat')

    def test_01_bin_cat(self):
        self.assertEqual(find_executable('/bin/cat'), '/bin/cat')

    def test_02_bin_cat(self):
        self.assertEqual(find_executable('weaver.py'),
                         os.path.abspath('weaver.py'))

    def test_03_asdffdsa(self):
        with self.assertRaises(WeaverError):
            find_executable('asdffdsa')

# Normalize path test case -----------------------------------------------------

class NormalizePathTestCase(TestCase):

    DIR_NAME = os.path.dirname(os.path.abspath(__file__))

    def test_00_readme(self):
        self.assertEqual(
            normalize_path('README.rst', self.DIR_NAME),
            '../../README.rst')

    def test_01_weaver_nest(self):
        self.assertEqual(
            normalize_path('weaver/nest.py', self.DIR_NAME),
            '../nest.py')

    def test_02_weaver_test_util(self):
        self.assertEqual(
            normalize_path('weaver/test/util.py', self.DIR_NAME),
            'util.py')

# Parse string list test case --------------------------------------------------

class ParseStringListTestCase(TestCase):

    def test_00_string(self):
        self.assertEqual(parse_string_list('a'), ['a'])

    def test_01_list(self):
        l = ['a']
        self.assertEqual(parse_string_list(l), l)

    def test_02_tuple(self):
        t = ('a',)
        self.assertEqual(parse_string_list(t), ['a'])

    def test_03_generator(self):
        g = range(10)
        self.assertEqual(parse_string_list(g), list(map(str, g)))

# Main execution ---------------------------------------------------------------

TestCases = [
    FindExecutableTestCase,
    NormalizePathTestCase,
    ParseStringListTestCase,
]

if __name__ == '__main__':
    test_runner = TextTestRunner(verbosity = 2)
    test_suite  = TestSuite(map(TestLoader().loadTestsFromTestCase, TestCases))
    test_runner.run(test_suite)

# vim: set sts=4 sw=4 ts=8 expandtab ft=python: --------------------------------
