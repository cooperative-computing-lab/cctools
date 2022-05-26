dataset = Glob('/usr/share/pixmaps/*.xpm')
jpgs    = Map('convert {IN} {OUT}', dataset, '{basename_woext}.jpg')
stats	= Map('stat {IN} > {OUT}', jpgs, '{basename}.stat', collect=True)   # Denotes that jpgs can be removed when not needed
