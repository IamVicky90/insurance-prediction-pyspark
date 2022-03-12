from unicodedata import name
from setuptools import setup, find_packages
from utility import read_params
params=read_params()
setup(
    name=params['base']['project_name'],
    version=params['base']['version'],
    description=params["base"]['description'],
    author=params['base']['author'],
    packages=find_packages(),
    license=params['base']['license'],
)