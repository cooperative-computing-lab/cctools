from weaver.function    import ParseFunction
from weaver.nest        import Nest

import glob

touch = ParseFunction('touch {OUT}')

for i in range(5):
    with Nest() as nest:
        touch(outputs='{0}.dat'.format(i))
        nest.compile()
        nest.execute()

    if len(glob.glob('*.dat')) == 2:
        break
