pngs = Glob('/usr/share/pixmaps/*.png')
xpms = Glob('/usr/share/pixmaps/*.xpm')

convert = ParseFunction('convert {IN} {OUT}')
chmod   = ParseFunction('chmod {ARG} {IN}')

for name, ds in (('pngs', pngs), ('xpms', xpms)):
    with Nest(name):
        with Nest('ppms'):
            ppms = Map(convert, ds, '{basename_woext}.ppm')
            chmod(ppms, arguments=0o644, local=True)

        with Nest('gifs'):
            gifs = Map(convert, ds, '{basename_woext}.gif')
            chmod(gifs, arguments=0o644, local=True)
