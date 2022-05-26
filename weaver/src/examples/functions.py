stat = ParseFunction('stat {IN} > {OUT}')
file = stat('/etc/hosts', '{basename}.stat')
stat(file, '{FULL}.stat', collect=True)

Define('MYVAR1', 1)
Export(['MYVAR1', 'MYVAR2'])

env = ParseFunction('env > {OUT}', environment={'MYVAR2': 2})
env(outputs='env0.txt')
env(outputs='env1.txt', environment={'MYVAR3': 3})
