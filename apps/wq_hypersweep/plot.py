import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import csv
import seaborn as sns



x = []
y = []
z = []
x_label = ''
y_label = ''
z_label = ''

with open('output.csv') as csv_file:
	csv_reader = csv.reader(csv_file, delimiter=',')
	line_count = 0
	for row in csv_reader:
		if line_count == 0:
			print(f'Column names are{row[1]},{row[3]}, and{row[4]}')
			x_label = row[3]
			y_label = row[4]
			z_label = row[1]
			line_count += 1
		else:
			x.append(float(row[3]))
			y.append(float(row[4]))
			z.append(float(row[1]))
			line_count += 1
	print(f'Processed {line_count-1} executions.')

x = np.array(x)
y = np.array(y)
z = np.array(z)

df = pd.DataFrame.from_dict(np.array([x,y,z]).T)
df.columns = [x_label,y_label,z_label]

pivotted = df.pivot(y_label,x_label,z_label)

sns.heatmap(pivotted, cmap='RdBu')
plt.title("ResNet model"+z_label+" as a function of"+x_label+" and"+y_label, fontsize=10)
plt.savefig('sweep_output.png')
