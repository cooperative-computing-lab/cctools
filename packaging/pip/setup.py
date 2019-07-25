import setuptools
from setuptools.command.install import install
import os

# Import code to compile
class PostInstallCommand(install):
    def run(self):
        git_clone = "git clone https://github.com/cooperative-computing-lab/ccto                                                                                        ols.git"
        prog_configure = "./configure --without-system-{allpairs,parrot,prune,sa                                                                                        nd,umbrella,wavefront,weaver}"
        make_command = "make && make install"
        os.system(git_clone)
        os.system(prog_configure)
        os.system(make_command)
        install.run(self)

# Standard setup
setuptools.setup(
    name='CCTools',
    version='7.0.16',
    author='Cooperative Computing Lab',
    author_email='dthain@nd.edu',
    packages=setuptools.find_packages(),
    url='https://github.com/cooperative-computing-lab/cctools',
    download_url='http://ccl.cse.nd.edu/software/files/cctools-{}-source.tar.gz'                                                                                        .format('7.0.16'),
    license='LICENSE.txt',
    description='The Cooperative Computing Tools (cctools) contains Parrot, Chir                                                                                        p, Makeflow, Work Queue, SAND, and other software',
    long_description='The Cooperative Computing Tools (cctools) contains Parrot,                                                                                         Chirp, Makeflow, Work Queue, SAND, and other software',
    long_description_content_type='text/markdown',
    cmdclass={
        'install': PostInstallCommand,
    },
    #install_requires=['swig'],
    #dependency_links=['https://github.com/swig/swig']
)
