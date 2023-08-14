# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver abstraction module """

from weaver.data     import parse_input_list, parse_output_list
from weaver.dataset  import Dataset, cache_generation
from weaver.function import parse_function, PythonFunction
from weaver.logger   import D_ABSTRACTION, debug
from weaver.options  import Options
from weaver.stack    import CurrentNest, WeaverAbstractions
from weaver.stack    import stack_context_manager
from weaver.util     import groups, type_str, Container, iterable

import functools
import itertools
import os


# Base Abstraction class

@stack_context_manager(D_ABSTRACTION, WeaverAbstractions)
class Abstraction(Dataset):
    """ The base Abstraction class.

    **Positional Arguments**:

    - `function`    -- Function to apply (Function, string, string format)

    **Keyword Arguments**:

    - `inputs`      -- Inputs to function
    - `outputs`     -- Output of function
    - `includes`    -- Files to include for each task.
    - `native`      -- Whether or not to use native abstraction if available.
    - `group`       -- Number of tasks to inline.
    - `collect`     -- Whether or not to mark files for garbage collection.
    - `local`       -- Whether or not to force local execution.

    `inputs` and `includes` are parsed using
    :func:`~weaver.data.parse_input_list` and must be in a form acceptable tot
    hat function.  Likewise, `outputs` is parsed by
    :func:`~weaver.data.parse_output_list` and `function` is parsed by
    :func:`~weaver.function.parse_function`.
    """
    Counter = None

    def __init__(self, function, inputs=None, outputs=None, includes=None,
        native=False, group=None, collect=False, local=False):
        # Must set id before we call Dataset.__init__ due to debugging
        # statement in said function.
        self.id         = next(self.Counter)
        self.function   = function
        self.inputs     = inputs
        self.outputs    = outputs or '{stash}'
        self.includes   = includes
        self.native     = native
        self.group      = group or 0
        self.local      = local
        Dataset.__init__(self)

        if collect:
            self.collect = parse_input_list(self.inputs)
        else:
            self.collect = None
        self.options = Options(local=self.local, collect=self.collect)

        self.nest.futures.append((self, False))
        debug(D_ABSTRACTION, 'Registered Abstraction {0} with {1}'.format(self, self.nest))

    def compile(self):
        """ Compile Abstraction to produce scheduled tasks. """
        debug(D_ABSTRACTION, 'Compiling Abstraction {0}'.format(self))
        for _ in self:
            pass

    def __str__(self):
        return '{0}[{1}]({2},{3})'.format(
            type_str(self), self.id, self.function, self.cache_path)


# AllPairs Abstraction

class AllPairs(Abstraction):
    """ Weaver AllPairs Abstraction.

    This Abstraction enables the following pattern of execution:

        AllPairs(f, set_a, set_b)

    In this case, the :class:`Function` *f* is applied to each pair-wise
    combination of *set_a* and *set_b*.
    """

    DEFAULT_PORT = 9098

    Counter = itertools.count()

    def __init__(self, function, inputs_a, inputs_b, outputs=None, includes=None,
        native=False, group=None, collect=False, local=False, port=None):
        Abstraction.__init__(self, function, None, outputs, includes, native,
            group, collect, local)
        self.inputs_a = inputs_a
        self.inputs_b = inputs_b
        self.port     = port or AllPairs.DEFAULT_PORT

    @cache_generation
    def _generate(self):
        with self:
            debug(D_ABSTRACTION, 'Generating Abstraction {0}'.format(self))

            function = parse_function(self.function)
            inputs_a = parse_input_list(self.inputs_a)
            inputs_b = parse_input_list(self.inputs_b)
            includes = parse_input_list(self.includes)

            # If native is enabled, then use allpairs_master, otherwise
            # generate tasks as part of the DAG.
            #
            # Note: parse_output_list flattens inputs, so we need to manually
            # translate pairs into a single string.
            if self.native:
                # Store inputs A and B lists as required by allpairs_master
                inputs_a_file = next(self.nest.stash)
                with open(inputs_a_file, 'w') as fs:
                    for input_file in map(str, inputs_a):
                        fs.write(input_file + '\n')

                inputs_b_file = next(self.nest.stash)
                with open(inputs_b_file, 'w') as fs:
                    for input_file in map(str, inputs_b):
                        fs.write(input_file + '\n')

                inputs  = [inputs_a_file, inputs_b_file]
                outputs = parse_output_list(self.outputs,
                            map(lambda p: '_'.join(
                                map(lambda s: os.path.basename(str(s)), p)),inputs))

                # Schedule allpairs_master
                with Options(local=True, collect=[i] if self.collect else None):
                    allpairs_master = parse_function(
                        'allpairs_master -p {0} {{IN}} {{ARG}} > {{OUT}}'.format(self.port))
                    yield allpairs_master(inputs, outputs, function.path, includes + [function.path])
            else:
                inputs  = list(itertools.product(inputs_a, inputs_b))
                outputs = parse_output_list(self.outputs,
                            map(lambda p: '_'.join(
                                map(lambda s: os.path.basename(str(s)), p)),inputs))

                # We use a wrapper script to collect the output of the
                # comparison and put in {INPUT_A} {INPUT_B} {OUTPUT} format, as
                # used by allpairs_master.
                for i, o in zip(inputs, outputs):
                    tmp_output = next(self.nest.stash)

                    with Options(local=self.options.local, collect=[i] if self.collect else None):
                        output = function(i, tmp_output, None, includes)

                    # Wrapper script should run locally and we should always
                    # try to collect the temporary intermediate output file.
                    with Options(local=True, collect=[tmp_output]):
                        yield AllPairsCompareWrapper(output, o, map(lambda p: os.path.basename(str(p)), i), None)


AllPairsCompareWrapper = parse_function('printf "%s\\t%s\\t%s\\n" {ARG} `cat {IN}` > {OUT}')


# Iterate Abstraction

class Iterate(Abstraction):
    """ Weaver Iterate Abstraction.

    This Abstraction enables the following pattern of execution:

        Iterate(f, limit or range, outputs)

    In this case, the :class:`Function` *f* is applied for the given *limit* or
    *range* to generate the corresponding *outputs*.
    """

    Counter = itertools.count()

    @cache_generation
    def _generate(self):
        with self:
            debug(D_ABSTRACTION, 'Generating Abstraction {0}'.format(self))

            function = parse_function(self.function)
            if iterable(self.inputs):
                inputs = self.inputs
            else:
                inputs = range(self.inputs)
            outputs  = parse_output_list(self.outputs, inputs)
            includes = parse_input_list(self.includes)

            for i, o in zip(inputs, outputs):
                with Options(local=self.options.local, collect=[i] if self.collect else None):
                    yield function(includes, o, i)


# Map Abstraction

class Map(Abstraction):
    """ Weaver Map Abstraction.

    This Abstraction enables the following pattern of execution:

        Map(f, inputs, outputs)

    In this case, the :class:`Function` *f* is applied to each item in
    *inputs* to generate the corresponding *outputs*.
    """

    Counter = itertools.count()

    @cache_generation
    def _generate(self):
        with self:
            debug(D_ABSTRACTION, 'Generating Abstraction {0}'.format(self))

            function = parse_function(self.function)
            inputs   = parse_input_list(self.inputs)
            outputs  = parse_output_list(self.outputs, inputs)
            includes = parse_input_list(self.includes)

            for i, o in zip(inputs, outputs):
                with Options(local=self.options.local, collect=[i] if self.collect else None):
                    yield function(i, o, None, includes)


# MapReduce Abstraction

class MapReduce(Abstraction):
    """ Weaver MapReduce Abstraction. """

    #: Default Mapper group size
    MAPPER_SIZE = 16
    #: Default number of Reducers
    REDUCERS    = 4

    Counter = itertools.count()

    def __init__(self, mapper, reducer, inputs, outputs=None, includes=None,
        native=False, group=None, collect=False, local=False,
        reducers=None):
        Abstraction.__init__(self, None, inputs, outputs, includes, native,
            group or MapReduce.MAPPER_SIZE, collect, local)
        self.mapper   = mapper
        self.reducer  = reducer
        self.reducers = reducers or MapReduce.REDUCERS

    @cache_generation
    def _generate(self):
        with self:
            debug(D_ABSTRACTION, 'Generating Abstraction {0}'.format(self))

            mapper   = parse_function(self.mapper, PythonMapper)
            inputs   = parse_input_list(self.inputs)
            includes = parse_input_list(self.includes)
            output   = self.outputs
            nest     = CurrentNest()

            for map_input in groups(inputs, self.group):
                map_output = next(nest.stash)
                with Options(local=self.options.local, collect=map_input if self.collect else None):
                    yield mapper(map_input, map_output, includes)

    def __str__(self):
        return '{0}[{1}]({2},{3},{4})'.format(
            type_str(self), self.id, self.mapper, self.reducer, self.cache_path)


class PythonMapper(PythonFunction):
    PYTHON_TEMPLATE = '''#!/usr/bin/env python
import {0}
{1}
if __name__ == '__main__':
    for file in sys.argv[1:]:
        for line in open(file):
            {2}(file, line.strip())
'''

class PythonReducer(PythonFunction):
    PYTHON_TEMPLATE = '''#!/usr/bin/env python
import {0}
{1}
if __name__ == '__main__':
    key    = None
    values = None
    for line in open(sys.argv[1]):
        k, v = line.strip().split('\t')
        if key is None:
            key, values = k, [v]
        else:
            if key != k:
                {2}(key, values)
            else:
                values.append(v)
    if key:
        {2}(key, values)
'''

# Merge Abstraction

class Merge(Abstraction):
    """ Weaver Merge Abstraction. """

    #: Default Merge group size
    GROUP_SIZE = 16

    Counter = itertools.count()

    def __init__(self, inputs, outputs, function=None, includes=None,
        native=False, group=None, collect=False, local=True):
        Abstraction.__init__(self, function or 'cat {IN} > {OUT}',
            inputs, outputs, includes, native,
            group or Merge.GROUP_SIZE, collect, local)

    @cache_generation
    def _generate(self):
        with self:
            debug(D_ABSTRACTION, 'Generating Abstraction {0}'.format(self))

            function = parse_function(self.function)
            inputs   = parse_input_list(self.inputs)
            includes = parse_input_list(self.includes)
            output   = self.outputs
            nest     = CurrentNest()

            if not os.path.isabs(output):
                output = os.path.join(nest.work_dir, output)

            while len(inputs) > self.group:
                next_inputs = []
                for group in groups(inputs, self.group):
                    output_file = next(nest.stash)
                    next_inputs.append(output_file)
                    with Options(local=self.options.local, collect=group if self.collect else None):
                        yield function(group, output_file, None, includes)
                inputs = next_inputs

            with Options(local=self.options.local, collect=inputs if self.collect else None):
                yield function(inputs, output, None, includes)


# Sentinel Abstraction

SENTINEL = Container(group=0)

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
