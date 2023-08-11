dataset = Glob('weaver/*.py')

with Nest('stats0'):
    stats0 = Map('stat {IN} > {OUT}', dataset, '{basename}.stat')
    with Nest('stats00'):
        stats00 = Map('stat {IN} > {OUT}', stats0, '{basename}.stat')

with Nest('stats1'):
    stats1 = Map('stat {IN} > {OUT}', dataset, '{basename}.stat')
    with Nest('stats10'):
        stats10 = Map('stat {IN} > {OUT}', stats1, '{basename}.stat')

Merge([stats0, stats1], '01.stats')
Merge([stats00, stats10], '0010.stats')
