This build script performs a build-and-test using only
the standard developer dependencies from conda in thebase
environment, so as to provide a github test environment
that matches what happens in conda-forge

Note that the conda packages given as developer dependencies
here are repeated in several different places.  We should have
a single location where they are clearly specified.

