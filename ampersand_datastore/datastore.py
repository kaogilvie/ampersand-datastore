'''Configurable interface to a datastore.'''

import logging
import json

class Database(object):
    '''Generic database connector for any interface that implements DB-API standards.'''
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        # override to convert generic API types to specific database types
        self.type_conversion_dict = {}

    def stage_object(self, target: object, target_table=False):
        if target_table is False:
            if not hasattr(target, 'target_table'):
                raise AttributeError(f"Target_table not defined in function call or target object -- set one of those.")
            target_table = target.target_table

        self.logger.info(f"Loading in {target} instance for interface with {target_table}.")

        self.logger.info(f"Trimming out columns not in {target}.model_columns. Starting with {len(target.data[0].keys())} columns...")
        target.formatted_data = []
        for entry in target.data:
            formatted_dict = {col: entry[col] for col in list(target.model_columns.keys()) if col in entry}
            target.formatted_data.append(formatted_dict)
        self.logger.info(f"""Ended with {len(target.formatted_data[0].keys())} columns. Trimmed the following columns out:
                            {set(target.data[0].keys()) - set(target.formatted_data[0].keys())}""")

        self.target = target

    def open_connection(self, creds: dict, connection_name=''):
        raise NotImplementedError("Implement the open_connection method on a per-connector basis.")

    def get_cursor(self, creds, connection_name=''):
        self.open_connection(creds, connection_name)
        self.cursor = self.cxn.cursor()
        self.logger.info("Cursor retrieved.")

    def close_connection(self):
        self.cursor.close()
        self.cxn.close()
        self.logger.info("Connection closed.")
