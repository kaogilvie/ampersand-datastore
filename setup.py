from setuptools import setup, find_packages
from ampersand_datastore import __version__

setup(name='ampersand_datastore',
      description='An extensible, generic interface to datastores to make loading data much easier',
      author='Kyle Ogilvie',
      author_email='kyle@kyleogilve.com',
      url='https://www.kyleogilvie.com/',
      packages=find_packages(
        where='ampersand_datastore'
      ),
      version=__version__,
      install_requires=[
        "psycopg2==2.9.3"
      ]
)
