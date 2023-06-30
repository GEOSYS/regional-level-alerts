from setuptools import find_packages, setup
with open('VERSION') as fh:
    version = fh.readline()

setup(
    name='regionallevelalerts',
    packages = find_packages('src'),
    package_dir={"":"src"},
    version=version,
    description='Raise weather or vegetation alerts on a regional entity based on a defined parameter (weather or vegetation) and threshold',
    author='EarthDaily Agro',
)
