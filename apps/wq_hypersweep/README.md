The wq_hypersweep program trains multiple residual neural network (ResNet) instances with varied hyperparameter values over a collection of images provided by the [CIFAR-10 dataset](https://www.cs.toronto.edu/~kriz/cifar.html). ~~The program uses the Work Queueframework for distributed execution on available resources.~~

To run:

~~1. Install CCTools~~

2. Install Tensorflow, Keras, and plotting dependencies
```python
pip install tensorflow numpy scipy scikit-learn pillow h5py
pip install keras
pip install matplotlib numpy seaborn
```

3. To allow the application to contact the dataset's host server, you may need to update your python security certificates:
```
pip install --upgrade certifi
```

4. Run 'python sweep.sh' to perform a pairwise hyperparameter optimization sweep over wq_hypersweep's default hyperparameter values


For listing the command-line options, do: ./wq_hypersweep -h


When the application completes, you will find the collated results of the collections (.csv) along with plots of each collection's results as a function of their hyperparameters (.png) as output files in the application's directory.
