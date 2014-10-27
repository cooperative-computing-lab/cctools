date_sh = ShellFunction('date +%Y%m%d')
date_sh(outputs='date.txt')

ls_and_ps_sh = ShellFunction('''
ls
ps
''')
ls_and_ps_sh(outputs='ls_and_ps.txt')


def sum_func(*args):
    print(sum(map(int, args)))

Sum = PythonFunction(sum_func)
Sum(outputs='sum.txt', arguments=[0, 1, 2, 3, 4, 5])

Touch_SH = ShellFunction('touch $2 && chmod $1 $2', cmd_format='{EXE} {ARG} {OUT}')
Touch_SH(outputs='touch.txt', arguments='600')

GetPids = Pipeline(["ps aux", "grep {ARG}", "awk \"{{print \\$2}}\" > {OUT}"], separator='|')
GetPids(outputs='makeflow.pids', arguments='makeflow')
