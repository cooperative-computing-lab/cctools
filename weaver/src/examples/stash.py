convert = ParseFunction('convert {IN} {OUT}')
dataset = Glob('/usr/share/pixmaps/*.xpm')
jpgs    = Map(convert, dataset, '{stash}')

stat	= ParseFunction('stat {IN} > {OUT}')
stats	= Map(stat, jpgs, '{stash}')
