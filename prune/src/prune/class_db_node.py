class DbNode(object):
  __slots__ = ( 'obj', 'lineage', 'progeny' )
  def __init__(self, obj={}, **kwargs):
    self.obj = obj

    if 'lineage' in kwargs:
        self.lineage = kwargs['lineage']
    else:
        self.lineage = []

    if 'progeny' in kwargs:
        self.progeny = kwargs['progeny']
    else:
        self.progeny = []
