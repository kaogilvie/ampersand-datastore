from setuptools import setup, find_packages
from ampersand_datastore import __version__

setup(name='ampersand_datastore',
      description='An extensible, generic interface to datastores to make loading data much easier',
      author='Kyle Ogilvie',
      author_email='kyle@kyleogilve.com',
      url='https://www.kyleogilvie.com/',
      packages=find_packages(),
      version=__version__,
      install_requires=[
        "psycopg2-binary==2.9.3",
        "snowflake-connector-python==3.15.0",
        "requests==2.27.1",
        "google-cloud-bigquery==3.12.0"
      ]
)
