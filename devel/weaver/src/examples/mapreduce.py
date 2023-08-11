from weaver.abstraction import PythonMapper

def wordcount_mapper(key, value):
    for word in value.split():
        print('{0}\t{1}'.format(word, 1))

def wordcount_reducer(key, values):
    print('{0}\t{1}'.format(key, sum(int(v) for v in values)))

MapReduce(
    mapper  = wordcount_mapper,
    reducer = wordcount_reducer,
    inputs  = Glob('weaver/*.py'),
)
