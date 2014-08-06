def compare_files(file_a, file_b):
    line_count_a = len(open(file_a, 'r').readlines())
    line_count_b = len(open(file_b, 'r').readlines())
    print(line_count_a - line_count_b)

dataset = Glob('weaver/*.py')

# Generic
results = AllPairs(compare_files, dataset, dataset)
table_0 = Merge(results, 'table_0.txt')

# Native
worker  = ParseFunction('work_queue_worker {ARG}')
worker(arguments='-t 5 localhost {0}'.format(AllPairs.DEFAULT_PORT))
table_1 = AllPairs(compare_files, dataset, dataset, 'table_1.txt', native=True)
