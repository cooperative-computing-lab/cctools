






















# allpairs_multicore(1)

## NAME
**allpairs_multicore** - executes All-Pairs workflow in parallel on a multicore machine

## SYNOPSIS
**allparis_multicore [options] _&lt;set A&gt;_ _&lt;set B&gt;_ _&lt;compare function&gt;_**

## DESCRIPTION

**allpairs_multicore** computes the Cartesian product of two sets
(**_&lt;set A&gt;_** and **_&lt;set B&gt;_**), generating a matrix where each cell
M[i,j] contains the output of the function F (**_&lt;compare function&gt;_**) on
objects A[i] (an item in **_&lt;set A&gt;_**) and B[j] (an item in
**_&lt;set B&gt;_**). The resulting matrix is displayed on the standard output,
one comparison result per line along with the associated X and Y indices.

For large sets of objects, **allpairs_multicore** will use as many cores as
you have available, and will carefully manage virtual memory to exploit
locality and avoid thrashing. Because of this, you should be prepared for the
results to come back in any order. If you want to further exploit the
parallelism of executing All-Pairs workflows on multiple (multicore) machines,
please refer to the [allpairs_master(1)](allpairs_master.md) utility.

## OPTIONS


- **-b**,**--block-size=_&lt;items&gt;_**<br />Block size: number of items to hold in memory at once. (default: 50% of RAM)
- **-c**,**--cores=_&lt;cores&gt;_**<br />Number of cores to be used. (default: # of cores in machine)
- **-e**,**--extra-args=_&lt;args&gt;_**<br />Extra arguments to pass to the comparison program.
- **-d**,**--debug=_&lt;flag&gt;_**<br />Enable debugging for this subsystem.
- **-v**,**--version**<br />Show program version.
- **-h**,**--help**<br />Display this message.


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

Let's suppose you have a whole lot of files that you want to compare all to
each other, named **a**, **b**, **c**, and so on. Suppose that you also
have a program named **compareit**) that when invoked as **compareit a b**
will compare files **a** and **b** and produce some output summarizing the
difference between the two, like this:

```
 a b are 45 percent similar
```

To use the allpairs framework, create a file called **set.list** that lists each of
your files, one per line:

```
 a
 b
 c
 ...
```

Then, invoke **allpairs_multicore** like this:

```
 % allpairs_multicore set.list set.list compareit
```

The framework will carry out all possible comparisons of the objects, and print
the results one by one (note that the first two columns are X and Y indices in
the resulting matrix):

```
 1	1	a a are 100 percent similar
 1	2	a b are 45 percent similar
 1	3	a c are 37 percent similar
 ...
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [The Cooperative Computing Tools]("http://ccl.cse.nd.edu/software/manuals")
- [All-Pairs User Manual]("http://ccl.cse.nd.edu/software/manuals/allpairs.html")
- [allpairs_master(1)](allpairs_master.md)


CCTools
