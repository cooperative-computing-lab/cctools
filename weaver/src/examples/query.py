ds = Glob('weaver/*.py')
print('ds:')
for f in sorted(ds):
    print(f)
Map('stat {IN} > {OUT}', ds, '{basename}.stat')

qy = Query(ds, ds.c.size >= 4096)
print('qy:')
for f in sorted(qy):
    print(f)
Map('stat -L {IN} > {OUT}', qy, '{basename}.lstat')

qy = Query(ds, limit=5)
print('qy:')
for f in sorted(qy):
    print(f)
Map('md5sum {IN} > {OUT}', qy, '{basename}.md5sum')
