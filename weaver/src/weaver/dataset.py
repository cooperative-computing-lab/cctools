# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver dataset module """

from weaver.compat  import map
from weaver.data    import MakeFile
from weaver.logger  import D_DATASET, debug, warn, fatal
from weaver.stack   import CurrentNest, CurrentScript
from weaver.util    import Cloneable, flatten, normalize_path, type_str

import fnmatch
import functools
import glob
import os

try:
    from MySQLdb import connect as MySQLConnect
    from MySQLdb.cursors import SSDictCursor as MySQLSSDictCursor
except ImportError as e:
    warn(D_DATASET, 'Unable to import MySQL: {0}'.format(e))


# Base Dataset class

class Dataset(object):
    """ Weaver abstract Dataset class.

    In Weaver, a :class:`Dataset` is an :func:`~weaver.util.iterable`
    collection of :class:`weaver.data.File` objects.

    Each :class:`Dataset` object has a *cursor* field ``c`` which can be used to
    construct a query on the :class:`Dataset` object.
    """
    def __init__(self, cache_path=None, cursor=None):
        self.c = cursor or ObjectCursor()
        self.nest = CurrentNest()
        self.cache_path = cache_path or next(self.nest.stash)

        debug(D_DATASET, 'Created Dataset {0}'.format(self))

    def __iter__(self):
        # Generate the cache under any of the following conditions:
        #
        #   1. Cache file does not exist
        #   2. Cache file exists, is older than compile start time, and we are
        #      forced to do so
        debug(D_DATASET, 'Iterating on Dataset {0}'.format(self))
        if os.path.exists(self.cache_path):
            # If cache file is made after we started compiling, then it is
            # valid, so don't bother generating.
            if CurrentScript().start_time <= os.stat(self.cache_path).st_ctime:
                debug(D_DATASET, 'Loading Dataset {0}'.format(self))
                return (MakeFile(f.strip(), self.nest) \
                    for f in open(self.cache_path, 'r'))

            message = 'Cache file {0} already exists'.format(self.cache_path)
            if CurrentScript().force:
                warn(D_DATASET, message)
            else:
                fatal(D_DATASET, message)

        debug(D_DATASET, 'Generating Dataset {0}'.format(self))
        return self._generate()

    def __str__(self):
        return '{0}({1})'.format(type_str(self), self.cache_path)

    def _generate(self):
        raise NotImplementedError

    def _query(self, filters, **parameters):
        debug(D_DATASET, 'Querying Dataset: {0}'.format(self.cache_path))
        try:
            limit = parameters['limit']
        except KeyError:
            limit = None

        # For each item in the Dataset, apply each filter; if all filters
        # succeed, then yield item.
        count = 0
        for o in iter(self):
            do_yield = True

            for f in filters:
                if not f(o):
                    do_yield = False
                    break

            if do_yield:
                count += 1
                yield o

            # Break out if we reach limit.
            if limit is not None and count == limit:
                break


# Cache generation decorator

def cache_generation(method):
    """ This decorator executes the iterator (i.e. ``method``) and stores the
    results in a file (i.e. ``self.cache_path``)
    """
    @functools.wraps(method)
    def wrapper(self):
        with open(self.cache_path, 'w') as fs:
            for i in flatten(method(self)):
                fs.write(i.path + '\n')
                yield i
    return wrapper


# File Dataset classes

class FileList(Dataset):
    """ This :class:`Dataset` consists of file paths specified from a text
    file.
    """
    def __init__(self, path):
        Dataset.__init__(self, path)

    def __iter__(self):
        # Skip checking and generating cache file, since we are given file
        debug(D_DATASET, 'Loading Dataset: {0}'.format(self.cache_path))
        return (MakeFile(normalize_path(f.strip(), os.curdir), self.nest)
                for f in open(self.cache_path, 'r'))


class Glob(Dataset):
    """ This :class:`Dataset` consists of file paths specified by a ``glob``
    expression.
    """
    def __init__(self, expr):
        Dataset.__init__(self)
        self.expr = expr

    @cache_generation
    def _generate(self):
        return (MakeFile(normalize_path(f.strip(), os.curdir), self.nest)
                for f in glob.glob(self.expr))


# ObjectCursor class

class ObjectCursor(Cloneable):
    """ Weaver ObjectCursor class.

    This class allows the user to create a curried query for objects.  Each
    :class:`ObjectCursor` object has a ``_field`` attribute which is used to
    store the ``field`` to be queried against.  This attribute is set when
    first accessed (i.e. ``__getattr__``).  Subsequent operators can then used
    this attribute to recall the ``field`` to utilize in comparison or as part
    of the specified operation.

    Normally a series of :class:`ObjectCursor` objects are collected and applied
    to an iterable to filter the dataset.
    """
    def __init__(self, field=None):
        self._field = field

    def __getattr__(self, name):
        # We return a clone of the current object in order to allow for the
        # :class:`ObjectCursor` object to be used multiple times in a query.
        debug(D_DATASET, 'getattr({0})'.format(name))
        return self.clone(_field=name)

    def __eq__(self, other):
        debug(D_DATASET, 'eq: {0} {1}'.format(self._field, other))
        return lambda x: getattr(x, self._field) == other

    def __ne__(self, other):
        debug(D_DATASET, 'ne: {0} {1}'.format(self._field, other))
        return lambda x: getattr(x, self._field) != other

    def __ge__(self, other):
        debug(D_DATASET, 'ge: {0} {1}'.format(self._field, other))
        return lambda x: getattr(x, self._field) >= other

    def __gt__(self, other):
        debug(D_DATASET, 'gt: {0} {1}'.format(self._field, other))
        return lambda x: getattr(x, self._field) > other

    def __le__(self, other):
        debug(D_DATASET, 'le: {0} {1}'.format(self._field, other))
        return lambda x: getattr(x, self._field) <= other

    def __lt__(self, other):
        debug(D_DATASET, 'lt: {0} {1}'.format(self._field, other))
        return lambda x: getattr(x, self._field) < other

    def __mod__(self, other):
        debug(D_DATASET, 'mod: {0} {1}'.format(self._field, other))
        return lambda x: fnmatch.fnmatch(getattr(x, self._field), other)


# SQL Dataset

class SQLDataset(Dataset):
    DB_HOST   = 'localhost'
    DB_USER   = 'anonymous'
    DB_PASS   = ''
    DB_NAME   = 'db'
    DB_TABLE  = 'table'
    DB_FIELDS = ['*']
    DB_QUERY_FORMAT = 'SELECT {fields} FROM {table} WHERE {filters}'

    def __init__(self, host=None, name=None, table=None, user=None,
        password=None, fields=None, query_format=None, keep_alive=False,
        path=None):
        self.db_host   = host     or SQLDataset.DB_HOST
        self.db_name   = name     or SQLDataset.DB_NAME
        self.db_user   = user     or SQLDataset.DB_USER
        self.db_pass   = password or SQLDataset.DB_PASS
        self.db_table  = table    or SQLDataset.DB_TABLE
        self.db_fields = fields   or SQLDataset.DB_FIELDS
        self.db_conn   = None

        self.db_query_format    = query_format or SQLDataset.DB_QUERY_FORMAT
        self.db_conn_keep_alive = keep_alive

        self.path = path or self._path

        # Initialize after setting database attributes for __str__
        Dataset.__init__(self, cursor=SQLCursor())

    def connect(self):
        raise NotImplementedError

    def disconnect(self):
        if self.db_conn:
            self.db_conn.close()
            self.db_conn = None

    def _path(self, row):
        raise NotImplementedError

    def _query(self, filters, **parameters):
        cursor = None
        try:
            if self.db_conn is None:
                self.connect()

            try:
                fields = parameters['fields']
            except KeyError:
                fields = self.db_fields
            try:
                limit = int(parameters['limit'])
            except KeyError:
                limit = None
            try:
                path = parameters['path']
            except KeyError:
                path = self.path

            cursor = self.db_conn.cursor()
            query  = self.db_query_format.format(
                fields  = ','.join(fields),
                table   = self.db_table,
                filters = ' AND '.join(filters))

            if limit:
                query = '{0} LIMIT {1}'.format(query, limit)

            debug(D_DATASET, 'Executing SQL query: {0}'.format(query))
            cursor.execute(query)
            for row in cursor.fetchall():
                yield MakeFile(path(self, row), self.nest)
        except Exception as e:
            fatal(D_DATASET, 'Unable to perform SQL query: {0}'.format(e), print_traceback=True)
        finally:
            if cursor:
                cursor.close()
            if not self.db_conn_keep_alive:
                self.disconnect()

        raise StopIteration

    def __str__(self):
        return '{0}({1},{2},{3})'.format(type_str(self), self.db_host, self.db_name, self.db_table)


# SQL Cursor

class SQLCursor(ObjectCursor):
    def is_(self, other):
        debug(D_DATASET, 'is: {0} {1}'.format(self._field, other))
        return "{0} is {1}".format(self._field, other)

    def __eq__(self, other):
        debug(D_DATASET, 'eq: {0} {1}'.format(self._field, other))
        if other is None:
            return "{0} IS NULL".format(self._field)
        else:
            return "{0} = '{1}'".format(self._field, other)

    def __ne__(self, other):
        debug(D_DATASET, 'ne: {0} {1}'.format(self._field, other))
        if other is None:
            return "{0} IS NOT NULL".format(self._field)
        else:
            return "{0} != '{1}'".format(self._field, other)

    def __le__(self, other):
        debug(D_DATASET, 'le: {0} {1}'.format(self._field, other))
        return "{0} <= '{1}'".format(self._field, other)

    def __lt__(self, other):
        debug(D_DATASET, 'lt: {0} {1}'.format(self._field, other))
        return "{0} < '{1}'".format(self._field, other)

    def __ge__(self, other):
        debug(D_DATASET, 'ge: {0} {1}'.format(self._field, other))
        return "{0} >= '{1}'".format(self._field, other)

    def __gt__(self, other):
        debug(D_DATASET, 'gt: {0} {1}'.format(self._field, other))
        return "{0} > '{1}'".format(self._field, other)

    def __mod__(self, other):
        debug(D_DATASET, 'mod: {0}'.format(self._field))
        return "{0} like '{1}'".format(self._field, other)

    def __or__(self, other):
        debug(D_DATASET, 'or: {0} {1}'.format(self._field, other))
        return Or(*map(lambda o: self == o, other))

    def __and__(self, other):
        debug(D_DATASET, 'and: {0} {1}'.format(self._field, other))
        return And(*map(lambda o: self == o, other))


# MySQL Dataset

class MySQLDataset(SQLDataset):
    def connect(self):
        debug(D_DATASET, 'Connecting to {0}'.format(self))

        self.db_conn = MySQLConnect(
            host        = self.db_host,
            db          = self.db_name,
            user        = self.db_user,
            passwd      = self.db_pass,
            cursorclass = MySQLSSDictCursor)

# Logical combinators

def And(*filters):
    if not filters: return ''

    if isinstance(filters[0], str):
        return '(' + ' AND '.join(filters) + ')'
    else:
        return lambda filters: all(map(lambda d: f(d), filters))

def Or(*filters):
    if not filters: return ''

    if isinstance(filters[0], str):
        return '(' + ' OR '.join(filters) + ')'
    else:
        return lambda filters: any(map(lambda d: f(d), filters))


# Query class

class Query(Dataset):
    """ Weaver Query class.

    This class let's you construct a query (i.e. filter or selection) on a
    :class:`Dataset` using the internal ORM DSL.
    """
    def __init__(self, dataset, *filters, **parameters):
        Dataset.__init__(self, cursor=dataset.c)
        self._dataset    = dataset
        self._filters    = filters
        self._parameters = parameters

        debug(D_DATASET, 'Created Query: {0}'.format(self.cache_path))

    @cache_generation
    def _generate(self):
        return self._dataset._query(self._filters, **self._parameters)

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
