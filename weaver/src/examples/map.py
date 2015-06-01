convert = ParseFunction('convert {IN} {OUT}')
dataset = Glob('/usr/share/pixmaps/*.xpm')
jpgs    = Map(convert, dataset, '{basename_woext}.jpg')

stat	= ParseFunction('stat {IN} > {OUT}')
dataset = Glob('weaver/*.py')
stats	= Map(stat, dataset, '{basename}.stat')
