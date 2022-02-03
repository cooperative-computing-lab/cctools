#import work_queue
import work_queue as wq

# Example funtion for the Map Abstraction
def dblfunc(x):
    return 2*x

# Example function for the Pair Abstraction
# Note: function must accept a tuple
def mulfunc(p):
    return p[0] * p[1]

# Example function for the Tree Reduce Abstraction  
# Note: function must accept a iterable
def maxfunc(p):
    return p[0] if p[0] > p[1] else p[1]



# Set up WorkQueue on port 9123
q = wq.WorkQueue(9123)

# Example arrays/sequences
a = [1, 2, 3, 4]
b = [2, 4, 6, 8]

# Map
results = q.map(dblfunc, a, 1)
print(f'Map: {results}')

# Pair
results = q.pair(mulfunc, a, b, 2)
print(f'Pair: {results}')

# Tree reduce
results = q.tree_reduce(maxfunc, b, 2)
print(f'Tree: {results}')
