from ampersand_datastore.datastore import Database
import os
import logging

def import_snowflake():
    import snowflake.connector
    return snowflake.connector

class Snowflake(Database):
    '''Connection to a particular Snowflake instance.'''
    def __init__(self):
        super().__init__()
        self.snow = import_snowflake()
        self.type_conversion_dict = {
            'text[]': 'ARRAY',
        }

    def check_safe(self, string):
        '''
        Snowflake doesn't implement anything like psycopg2's SQL safe types,
        so you have to roll your own. This is my first stab at it, and I'm
        sure that it's not good enough yet.
        '''
        if type(string) != str:
            return string
        if ';' in string:
            self.logger.warning(f"Removing suspicious semicolon from {string} before insertion.")
            string = string.replace(';', '')
        return string

    def open_connection(self, creds: dict):
        ''''
        Open connection to snowflake with the following parameters in creds:

        :user: username
        :password: see name of parameter
        :account: the part of the snowflake URL before .snowflakecomputing.com
        :database: the target database name
        :warehouse: don't use too many credits, hny

        Schema is not set in the connection string to allow for maximum flexibility
        in SQL operations down the line.
        '''
        if not set(['user', 'password', 'account', 'database', 'warehouse']).issubset(set(creds.keys())):
            raise AttributeError("Required basic params for Snowflake connection not included in creds dict (user, password, account).")
        self.cxn = self.snow.connect(**creds)
        self.logger.info(f"Set up connection to {creds['account']} Snowflake instance successfully.")

    def get_cursor(self, creds, cursor_type=False):
        '''
        Interface to connect to database and get a cursor. Will only connect
        if there is no existing connection.

        Can pass keywords to cursor type to get different kinds of cursors. Currently
        implemented: default cursor type.
        '''
        if not hasattr(self, 'cxn'):
            self.open_connection(creds)

        self.cursor = self.cxn.cursor()
        self.logger.info("Cursor retrieved.")

    def create_object(self, target_table: str, schema: str, primary_key_list: list):
        '''Create table corresponding to object in target database.'''
        if not hasattr(self, 'target'):
            raise AttributeError("Target object not staged within Database object. Run stage_object first.")

        if len(self.type_conversion_dict) > 0:
            for col, type in self.target.model_columns.items():
                converted_type = self.type_conversion_dict.get(type, None)
                if converted_type is not None:
                    self.logger.info(f"Debugging: converting {type} to {converted_type}")
                    self.target.model_columns[col] = converted_type

        columns = ",".join([
            "{col} {type}".format(col = self.check_safe(col), type = self.check_safe(type)) for col, type in self.target.model_columns.items()
        ])

        if len(primary_key_list) > 0:
            columns = "{columns}, PRIMARY KEY ({pk_list})".format(
                pk_list = (','.join([
                        self.check_safe(pk) for pk in primary_key_list
                    ])),
                columns = columns)

        create_if_not_exists = "CREATE TABLE IF NOT EXISTS {schema}.{target_table} ({columns})".format(
            schema = self.check_safe(schema),
            target_table = self.check_safe(target_table),
            columns = columns
        )
        self.logger.info(f"Creating table using the following SQL: {create_if_not_exists}")
        self.cursor.execute(create_if_not_exists)
        self.cxn.commit()
        self.logger.info("Created.")

    def drop_object(self, target_table, schema):
        '''Drop table corresponding to object in target database.'''
        if not hasattr(self, 'target'):
            raise AttributeError("Target object not staged within Database object. Run stage_object first.")

        drop_table = "DROP TABLE {schema}.{target_table}".format(schema=self.check_safe(schema),target_table=self.check_safe(target_table))
        self.cursor.execute(drop_table)
        self.cxn.commit()
        self.logger.info(f"Table {schema}.{target_table} dropped.")

    def upsert_object(self, target_table, schema, primary_key_list):
        '''Convenience wrapper to perform checks, drops and upserts as needed.'''
        try:
            self.create_object(target_table, schema, primary_key_list)

            if len(primary_key_list) == 0:
                self.logger.error("No primary keys declared for table -- you cannot upsert without at least one. Appending is still an option.")
                raise ValueError

            ## LOAD TEMP TABLE
            self.logger.info("Creating temp table")
            self.append_object(f"{target_table}_temp", schema, primary_key_list)

            ## MERGE INTO instead of ON CONFLICT
            upsert_sql = """MERGE INTO {schema}.{target_table} as a
            USING {schema}.{target_table}_temp as b
            ON {primary_key_expression}
            WHEN MATCHED THEN UPDATE SET {update_cols}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """.format(
                        schema = self.check_safe(schema),
                        target_table = self.check_safe(target_table),
                        col_string = ','.join([
                            self.check_safe(field) for field in self.target.model_columns.keys()
                        ]),
                        primary_key_expression = ','.join([
                            f"a.{self.check_safe(pk)} = b.{self.check_safe(pk)}" for pk in primary_key_list
                        ]),
                        update_cols = ','.join([
                            "a.{field} = b.{field}".format(field = self.check_safe(field)) for field in self.target.model_columns if field not in primary_key_list
                        ]),
                        insert_cols = ",".join([
                            field for field in self.target.model_columns
                        ]),
                        insert_vals = ",".join([
                            f"b.{field}" for field in self.target.model_columns
                        ])
                       )

            # self.logger.info("Upserting {len}") # length of rows to upsert
            self.cursor.execute(upsert_sql)
            self.cxn.commit()
            self.logger.info("Committed upsert.")
        except self.snow.ProgrammingError:
            self.logger.exception("Something went wrong with the upsert.")
            self.cxn.rollback()
        finally:
            self.logger.info("Cleaning up temp table...")
            self.cursor.execute(f"DROP TABLE IF EXISTS {schema}.{target_table}_temp")
            self.cxn.commit()

    def append_object(self, target_table, schema, primary_key_list):
        self.logger.info("Creating table if does not exist")
        self.create_object(target_table, schema, primary_key_list)

        val_string = ''
        for row in self.target.formatted_data:
            new_row = "("
            for col, typ in self.target.model_columns.items():
                safe_col = self.check_safe(row[col])
                # handle type idiosyncrasies
                if typ == 'varchar':
                    if safe_col is not None:
                        if safe_col[0] != "'":
                            safe_col = f"'{safe_col}"
                        if safe_col[-1] != "'":
                            safe_col = f"{safe_col}'"
                # you cannot insert a python array directly into snowflake yet
                # this makes exclusively str arrays
                if typ == 'ARRAY':
                    safe_col = self.check_safe(f"{str(row[col])}").replace("'", '"')
                    safe_col = f"'{safe_col}'"
                # format the insert vals statement
                if new_row == "(":
                    new_row = f"{new_row}{safe_col}"
                else:
                    new_row = ','.join([new_row, str(safe_col)])
            new_row = f"{new_row})"
            if val_string == '':
                val_string = new_row
            else:
                val_string = ','.join([val_string, new_row])
        self.logger.debug(f"Values string: {val_string}")


        select_str = ''
        countah = 1
        for type in self.target.model_columns.values():
            counter = f"${countah}"
            if type == 'ARRAY':
                counter = f"PARSE_JSON({counter})"
            counter = f"{counter},"
            select_str = f"{select_str}{counter}"
            countah += 1
        select_str = select_str[:-1]

        insert_sql = """INSERT INTO {schema}.{target_table}
        ({col_string})
        SELECT {select_str}
        FROM VALUES {val_string}
        """.format(
                    schema = self.check_safe(schema),
                    target_table = self.check_safe(target_table),
                    col_string = ','.join([
                        self.check_safe(field) for field in self.target.model_columns.keys()
                    ]),
                    select_str = select_str,
                    val_string = val_string
                )
        self.logger.info(f"Inserting {len(self.target.formatted_data)} rows into {schema}.{target_table}...")
        self.logger.info(f"Insert query: {insert_sql}")
        self.cursor.execute(insert_sql)
        self.cxn.commit()
        self.logger.info("Committed insert.")

    def recreate_object(self, target_table, schema, primary_key_list):
        '''Convenience wrapper for drop and create methods.'''
        self.drop_object(target_table, schema)
        self.create_object(target_table, schema, primary_key_list)
