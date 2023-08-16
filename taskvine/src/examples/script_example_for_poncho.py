import numpy as np
import matplotlib.pyplot as plt

x = np.linspace(-5,5,10000)
y = 18*(x**2) + 19*(x) + 1070

fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)

ax.spines['left'].set_position('center')
ax.spines['bottom'].set_position('zero')
ax.spines['right'].set_color('none')
ax.spines['top'].set_color('none')

ax.xaxis.set_ticks_position('bottom')
ax.yaxis.set_ticks_position('left')

plt.plot(x,y, 'r')
plt.savefig('fig.png')

print("done")

# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
