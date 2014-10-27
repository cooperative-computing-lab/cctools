def bxgrid_path(self, object):
    return '/bxgrid/ccldb.cse.nd.edu/fileid/{0}'.format(object['fileid'])

bxgrid_dataset = MySQLDataset(host='ccldb.cse.nd.edu', name='biometrics', table='files', path=bxgrid_path)
bxgrid_files   = Query(bxgrid_dataset,
                    Or(And(bxgrid_dataset.c.fileid == '321', bxgrid_dataset.c.size > 1040000),
                       And(bxgrid_dataset.c.fileid == '322', bxgrid_dataset.c.size > 1040000)))
Map('stat {IN} > {OUT}', bxgrid_files, '{BASE}.stat0')

bxgrid_files   = Query(bxgrid_dataset, bxgrid_dataset.c.fileid == None, limit=10)
Map('stat {IN} > {OUT}', bxgrid_files, '{BASE}.stat1')

bxgrid_files   = Query(bxgrid_dataset, bxgrid_dataset.c.fileid % '12%', limit=10)
Map('stat {IN} > {OUT}', bxgrid_files, '{BASE}.stat2')

bxgrid_files   = Query(bxgrid_dataset, bxgrid_dataset.c.extension | ('gz', 'abs.gz'), limit=10)
Map('stat {IN} > {OUT}', bxgrid_files, '{BASE}.stat3')
