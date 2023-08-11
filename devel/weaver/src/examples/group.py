convert = ParseFunction('convert {IN} {OUT}')
dataset = Glob('/usr/share/pixmaps/*.png')
jpgs    = Map(convert, dataset, '{basename_woext}.jpg', group=4)

stat	= ParseFunction('stat {IN} > {OUT}')
dataset = Glob('weaver/*.py')
stats	= Map(stat, dataset, '{basename}.stat', group=4)
