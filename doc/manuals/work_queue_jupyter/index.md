# WorkQueue Jupyter Tutorial

## Overview

Welcome to WorkQueue!  This tutorial is made for first time users who want to unlock the power of distributed computing, in tools that you're already familiar with, like Python and Jupyter.  If that's you, then you've come to the right place.  In this tutorial, you will learn about the need for distributed computing, the power of WorkQueue, and how we can write Python programs to use WorkQueue.

This tutorial will be entirely within a Jupyter Notebook.  If you already have Jupyter and WorkQueue installed on your computer, and if you are experienced in Jupyter already, go ahead a click <u style="background-color: yellow;">here</u> to download the tutorial notebook; or, you can type `wget `<u style="background-color: yellow;">LOCATION/OF/NOTEBOOK</u> in your terminal.

If you are brand new, no worries!  You can take the following steps to make sure you have everything installed and set up correctly.

## Installation and Setup

1. Make sure that you have the ability to install things using conda.
    - In your terminal, run `conda list`
    - If it fails, then you need to install either <a href="https://docs.conda.io/projects/conda/en/latest/user-guide/install/">miniconda</a> or <a href="https://docs.anaconda.com/anaconda/install/">anaconda</a>. Miniconda is a light version of anaconda, and we recommend it as it is much faster to install. Install the versions for `Python 3.7` in your home directory.
    - Descend into your conda directory: `cd <your_conda_directory>`
2. Before doing anything else, unset your PYTHONPATH. Run: `unset PYTHONPATH`
3. Once you have conda installed and you are in the directory, run this command to install cctools (which includes WorkQueue): `conda install -y -c conda-forge ndcctools`
4. Install jupyterlab, which allows you to use Jupyter Notebooks: `conda install jupyterlab`
5. Now, it is essential to set your PYTHONPATH to include the path to where WorkQueue exists as a library.
    - In order to set your PYTHONPATH to the correct directory, run `export PYTHONPATH=~/<your_conda_directory>/pkgs/<your_ndcctools_directory>/lib/python3.7/site-packages`, but replace `<your_conda_directory>` with the conda directory you chose (it should look like `miniconda3`, `anaconda`, or something similar), and replace `<your_ndcctools_directory>` with the name of the ndcctools directory that was installed for you (it should look like `ndcctools-7.0.21-abcdef123456`).  You can run `ls pkgs | grep ndcctools` to discover what your directory is called.
6. To get the notebook for the tutorial, either click <u style="background-color: yellow;">here</u> to download it, then move the file into your conda directory; or, run `wget `<u style="background-color: yellow;">LOCATION/OF/NOTEBOOK</u>.
7. Finally, run `jupyter notebook`, and when the directory tree pops up in your browser, click on the file you just downloaded to open it up!
