# wq_hypersweep

The wq_hypersweep program trains multiple residual neural network (ResNet) instances with varied hyperparameter values over a collection of images provided by the [CIFAR-10 dataset](https://www.cs.toronto.edu/~kriz/cifar.html). The program uses the Work Queue framework for distributed execution on available resources.


## Build:

1. Install [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/).
2. Install [conda-pack](https://conda.github.io/conda-pack/). This can easily be done using `conda` directly with:
```bash
conda install -c conda-forge conda-pack
```
Or, from PyPI using `pip`:
```bash
pip install conda-pack
```
3. Install [CCTools](https://cctools.readthedocs.io/en/latest/install/). This can easily be done using `conda` directly with:
```bash
conda install -y -c conda-forge ndcctools
```

4. Install Tensorflow, Keras, and plotting dependencies. Using `conda`:
```bash
conda install numpy=1.18.1 tensorflow=1.15.0 certifi=2020.6.20 pandas=0.25.3 seaborn=0.10.1
```
Or, from PyPI using `pip`:
```bash
pip install tensorflow numpy scipy scikit-learn pillow h5py keras matplotlib numpy seaborn
```
Note: To allow the application to contact the dataset's host server, you may need to update your python security certificates:
```bash
pip install --upgrade certifi
```

5. Download the CIFAR-10 dataset  locally using the command line and extract it for use by the application.
```bash
curl https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz --create-dirs -o datasets/cifar-10-batches-py.tar.gz
tar -xvf datasets/cifar-10-batches-py.tar.gz --directory datasets/
```
The same tasks can be accomplished by running the `resnet.py` locally:
```bash
python resnet.py
rm results.csv
```

6. Create and pack a `conda` environment to allow wq_hypersweep to run when distributed with Work Queue using the packaging scripts provided by CCTools. (This might not work, use conda-pack)
```bash
python_package_analyze resnet.py env.yaml
python_package_create env.yaml env.tar.gz
rm env.yaml
```
Or, using `conda-pack` directly:
```bash
conda create -n env python=3.7 numpy=1.18.1 tensorflow=1.15.0
conda pack -n env -o env.tar.gz
```

## Working Directory:
At this point, your working directory should contain the following:
```
/datasets   - Directory holding CIFAR-10 dataset
env.tar.gz  - Packed conda environment that hold all dependencies required by resnet.py for remote execution
output.png  - Sample plot provided by distribution
plot.py     - Python script used to convert application results into heatmaps
README.md   - README provided by distribution
resnet.py   - Python script to build/train/validate one ResNet model
script.sh   - Pilot script used by test.py to unpack a conda environment at a remote worker and execute resnet.py
sweep.sh    - Sample bash script that performs a local hyperparameter sweep
test.py     - Work Queue manager program used to distribute training tasks to remote workers
plot_resources.py     	- Plot resources and hyperparameters of Resnet
plot_resources_100.py  	- Plot resources and 4 hyperparameters of Resnet
read_summary_log.py		- Read summmary log from running dist_sweep.py and write to resources_all.txtread_summary_log_100.py - Read summary log from running dist_sweep_100.py and write to resources_all_100.txt   
```

## Run:
### Local, Single Execution, Command line:
Run `python resnet.py` to execute a single instance of the program at it's default hyperparamter values. This is the equivalent of running `python resnet.py -b 64 -r 10 -d 0.5 -e 30 -s 10 -o results.csv`.

### Local, Sweep, Bash Script:
Run `./sweep.sh` to perform a pairwise hyperparameter sweep over wq_hypersweep's default hyperparameter values. Specifically, this will execute 90 instances of `resnet.py` sequentially, varying dropout rate between 0.1 to 0.9 at 0.1 increments and number of epochs between 1 and 10 at 1 epoch increments. The results of all 90 executions are collated into `results.png` and a heatmap of the resulting sweep is produced as `sweep_output.png`.

### Distributed, Sweep, Work Queue on Condor:
Run `python dist_sweep.py` to perform a pairwise hyperparameter optimization sweep over wq_hypersweep's default hyperparameter values using Work Queue over Condor. Specifically, this will generate 400 instances of `resnet.py` varying dropout rate between 0.05 to 0.95 at 0.05 increments and number of residual blocks between 1 and 20 at 1 block increments.

In a second terminal, run the following command to create workers to complete the Work Queue tasks created by the previous command:
```bash
condor_submit_workers $HOSTNAME 9213 400
```
The results of all 400 executions are reported back to the Work Queue manager un-collated. To collate these results for plotting, run:
```bash
echo "loss, accuracy, batch_size, num_res_net_blocks, dropout_rate, epochs, steps_per_epoch, validation_steps" > output.csv
awk 'FNR==2{print $0 >> "output.csv"}' results*.csv
```
For a visualization using heatmap, run `python plot.py`. This which will produce a heatmap reporting model accuracy as a function of dropout rate and number or residual blocks.

To collect resources usage from running dist_sweep.py and dist_sweep_100.py, run respectively `python read_summary_log.py; python plot_resources.py` and `python read_summary_log_100.py; python plot_resources_100.py`. These commands will collect resources usage into resources_all.txt and resources_all_100.txt, and save 8 graphs plotting the resources usage and hyperparameters variations.
## Further Info:
For listing the command-line options, run: python resnet.py -h
```
$ python resnet.py -h
usage: resnet.py -b <batch size> -r <number of ResNet blocks> -d <dropout rate> -e <number of epochs> -s <steps per epoch> -v <validation steps> -o <output file>
```

When the application completes, you will find the collated results of the collections (.csv) along with plots of each collection's results as a function of their hyperparameters (.png) as output files in the application's directory. A sample output is provided below:

![Output](https://github.com/tjuedema/cctools/blob/master/apps/wq_hypersweep/output.png)
