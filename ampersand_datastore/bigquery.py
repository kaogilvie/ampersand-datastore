from .datastore import Database

import os

def import_bigquery():
    from google.cloud import bigquery
    return bigquery

class BigQuery(Database):
    '''Connection to a particular BigQuery instance.'''
    def __init__(self):
        super().__init__()
        self.bigquery = import_bigquery()
        self.type_conversion_dict = {}

    def open_connection(self, creds: str = None):
        ''''
        Open connection to bigquery. It uses the application default credentials
        unless you explicitly pass it a JSON keyfile.

        To set the application default credentials for BQ, run:
        `gcloud auth application-default login`
        from the command line.

        creds: string to the JSON keyfile for the service account you want to use
        '''
        if creds is not None:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds
        self.cxn = self.bigquery.Client()
        self.logger.info(f"Set up connection to {self.cxn.project} BigQuery instance successfully.")

    def get_cursor(self, creds, connection_name=''):
        raise NotImplementedError("We don't currently support BQ cursors.")