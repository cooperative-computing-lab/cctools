# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver logger test units """

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + '/../..'))

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

from unittest import TestCase, TestLoader, TestSuite, TextTestRunner

from weaver.logger import Logger

# Logger test case -------------------------------------------------------------

class LoggerTestCase(TestCase):

    logger = Logger(StringIO(), '[{flag}] {system:<10} {message}')

    def test_00_enable(self):
        self.logger.enable('logger')
        self.logger.enable(['a', 'b', 'c'])

    def test_01_disable(self):
        self.logger.disable('a')
        self.logger.disable(['a', 'b', 'c'])

    def test_02_debug(self):
        self.logger.stream.close()
        self.logger.stream = StringIO()
        self.logger.debug('asdf', 'hello, world')
        self.assertEqual(self.logger.stream.getvalue(), '')

    def test_03_debug(self):
        self.logger.debug('logger', 'debug')
        self.assertEqual(self.logger.stream.getvalue(), '[D] LOGGER     debug\n')

    def test_04_fatal(self):
        self.logger.stream.close()
        self.logger.stream = StringIO()
        self.logger.exit_on_fatal = False

        self.assertRaises(Exception, self.logger.fatal, 'logger', 'fatal')
        self.assertEqual(self.logger.stream.getvalue(), '[F] LOGGER     fatal\n')

    def test_05_warn(self):
        self.logger.stream.close()
        self.logger.stream = StringIO()

        self.logger.warn('logger', 'warn')
        self.assertEqual(self.logger.stream.getvalue(), '[W] LOGGER     warn\n')

# Main execution ---------------------------------------------------------------

if __name__ == '__main__':
    test_runner = TextTestRunner(verbosity = 2)
    test_suite  = TestLoader().loadTestsFromTestCase(LoggerTestCase)
    test_runner.run(test_suite)

# vim: set sts=4 sw=4 ts=8 expandtab ft=python: --------------------------------
