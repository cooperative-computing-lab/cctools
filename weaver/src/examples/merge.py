stat	= ParseFunction('stat {IN} > {OUT}')
dataset = Glob('weaver/*.py')
stats	= Map(stat, dataset, '{basename}.stat')
output  = Merge(stats, 'output.txt')
