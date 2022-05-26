uname = ParseFunction('uname -a > {OUT}')

for group in ('disc', 'ccl', 'gh'):
    batch_options = 'requirements = MachineGroup == "{0}"'.format(group)
    uname(outputs='uname.{0}'.format(group), environment={'BATCH_OPTIONS': batch_options})

#for group in ('disc', 'ccl', 'gh'):
#    with Options(batch='requirements = MachineGroup == "{0}"'.format(group)):
#        uname(outputs='uname.{0}'.format(group))
