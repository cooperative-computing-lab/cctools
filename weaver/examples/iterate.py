files = Iterate('touch {OUT}', range(5), '{BASE}')
Map('stat {IN} > {OUT}', files, '{BASE}.stat')
