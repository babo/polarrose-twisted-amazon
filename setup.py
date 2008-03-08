
from distutils.core import setup
from glob import glob

setup(
    name = 'polarrose-twisted-amazon',
    version = '0.1',
    author = 'Stefan Arentz',
    author_email = 'stefan@arentz.nl',
    packages = ['polarrose', 'polarrose.amazon'],
    package_dir = { 'polarrose': 'src/polarrose', 'polarrose.amazon': 'src/polarrose/amazon' },
    scripts = glob("scripts/*")
)

