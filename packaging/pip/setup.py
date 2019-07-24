import setuptools
import setuptools.command.install

# Import code to compile
class CustomInstall(install):
    def run(self):
        command = "git clone https://github.com/cooperative-computing-lab/cctools.git"
        process = subprocess.Popen(command, shell=True, cwd="CCTools")
        process.wait()
        install.run(self)

# Implements C code
ext_module = Extension(
    
)

# Standard setup
setuptools.setup(
    name='CCTools',
    version='7.0.15',
    author='Cooperative Computing Lab',
    author_email='dthain@nd.edu',
    packages=setuptools.find_packages(),
    url='https://github.com/cooperative-computing-lab/cctools',
    download_url='http://ccl.cse.nd.edu/software/files/cctools-{}-source.tar.gz'.format('7.0.15'),
    license='LICENSE.txt',
    description='The Cooperative Computing Tools (cctools) contains Parrot, Chirp, Makeflow, Work Queue, SAND, and other software',
    long_description='The Cooperative Computing Tools (cctools) contains Parrot, Chirp, Makeflow, Work Queue, SAND, and other software',
    long_description_content_type='text/markdown',
    ext_modules = [ext_module],
    install_requires=['swig'],
    dependency_links=['https://github.com/swig/swig']
)
