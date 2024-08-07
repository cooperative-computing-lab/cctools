from setuptools import setup, find_packages

setup(
    name='shepherd',
    version='0.1.0',
    description='Shepherd - Service Orchestration and Monitoring Tool',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'pyyaml',
        'jinja2',
        'graphviz',
        'matplotlib',
        'pandas',
    ],
    entry_points={
        'console_scripts': [
            'shepherd=shepherd.shepherd:main',
            'shepherd_viz=shepherd.shepherd_viz:main',
        ],
    },
    python_requires='>=3.6',
)
