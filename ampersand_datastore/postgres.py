from .datastore import Database

## this is clumsy but its better than before
def import_psycopg2():
    import psycopg2
    return psycopg2

def import_dictcursor():
    from psycopg2.extras import DictCursor
    return DictCursor

def import_execute():
    from psycopg2.extras import execute_values
    return execute_values

class Postgres(Database):
    '''Connection to a postgres database.'''
    def __init__(self):
        self.psycopg2 = import_psycopg2()
        self.dictCursor = import_dictcursor()
        self.execute_values = import_execute()
        super().__init__()

    def open_connection(self, creds: dict):
        if not set(['dbname', 'user', 'password', 'host', 'port']).issubset(set(creds.keys())):
            raise AttributeError("Required basic params for postgres connection not included in creds dict.")
        self.cxn = psycopg2.connect(**creds)
        self.logger.info(f"Set up connection to {creds['dbname']} Postgres db successfully.")

    def get_cursor(self, creds, cursor_type=False):
        '''
        Interface to connect to database and get a cursor. Will only connect
        if there is no existing connection.

        Can pass keywords to cursor type to get different kinds of cursors. Currently
        implemented: 'dictcursor' and default cursor type.
        '''
        if not hasattr(self, 'cxn'):
            self.open_connection(creds)

        if cursor_type == 'dictcursor':
            from psycopg2.extras import DictCursor
            self.cursor = self.cxn.cursor(cursor_factory=DictCursor)
        else:
            self.cursor = self.cxn.cursor()
        self.logger.info("Cursor retrieved.")

    def create_object(self, target_table: str, schema: str, primary_key_list: list):
        if not hasattr(self, 'target'):
            raise AttributeError("Target object not staged within Database object. Run stage_object first.")

        # override datatypes here if needed
        if len(self.type_conversion_dict) > 0:
            raise NotImplementedError("Type conversion not yet implemented for Postgres!")

        columns = self.psycopg2.sql.SQL("{columns}").format(
            columns = self.psycopg2.sql.SQL(",").join([
                    (self.psycopg2.sql.SQL("{col} {type}").format(col = self.psycopg2.sql.Identifier(col), type = self.psycopg2.sql.SQL(type))) for col, type in self.target.model_columns.items()
                ])
            )
        if len(primary_key_list) > 0:
            columns = self.psycopg2.sql.SQL("{columns}, PRIMARY KEY ({pk_list})").format(
                columns = columns,
                pk_list = self.psycopg2.sql.SQL(',').join([
                    self.psycopg2.sql.Identifier(pk) for pk in primary_key_list
                ])
            )
        create_if_not_exists = self.psycopg2.sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{target_table} ({columns})").format(
            schema = self.psycopg2.sql.Identifier(schema),
            target_table = self.psycopg2.sql.Identifier(target_table),
            columns = columns
        )
        self.logger.info(f"Creating table using the following SQL: {create_if_not_exists.as_string(self.cursor)}")
        self.cursor.execute(create_if_not_exists)
        self.cxn.commit()
        self.logger.info("Created.")

    def drop_object(self, target_table, schema):
        if not hasattr(self, 'target'):
            raise AttributeError("Target object not staged within Database object. Run stage_object first.")

        drop_table = self.psycopg2.sql.SQL("DROP TABLE {schema}.{target_table}").format(schema=self.psycopg2.sql.Identifier(schema),target_table=self.psycopg2.sql.Identifier(target_table))
        self.cursor.execute(drop_table)
        self.cxn.commit()
        self.logger.info(f"Table {schema}.{target_table} dropped.")

    def upsert_object(self, target_table, schema, primary_key_list):
        '''Convenience wrapper to perform checks, drops and upserts as needed.'''
        self.create_object(target_table, schema, primary_key_list)

        upsert_sql = self.psycopg2.sql.SQL("""INSERT INTO {schema}.{target_table}
        ({col_string})
        VALUES {val_string}
        ON CONFLICT ({primary_keys})
        DO
        UPDATE SET {update_cols}
        """).format(
                    schema = self.psycopg2.sql.Identifier(schema),
                    target_table = self.psycopg2.sql.Identifier(target_table),
                    col_string = self.psycopg2.sql.SQL(',').join([
                        self.psycopg2.sql.Identifier(field) for field in self.target.model_columns.keys()
                    ]),
                    val_string = self.psycopg2.sql.Placeholder(),
                    primary_keys = self.psycopg2.sql.SQL(',').join([
                        self.psycopg2.sql.Identifier(pk) for pk in primary_key_list
                    ]),
                    update_cols = self.psycopg2.sql.SQL(',').join([
                        (self.psycopg2.sql.SQL("{field} = EXCLUDED.{field}").format(field = self.psycopg2.sql.Identifier(field))) for field in self.target.model_columns if field not in primary_key_list
                    ])
                   )

        self.logger.info(f"Using this SQL to upsert: {upsert_sql.as_string(self.cursor)}")
        self.execute_values(
            self.cursor,
            upsert_sql,
            self.target.formatted_data,
            self.psycopg2.sql.SQL("({arglist})").format(arglist=self.psycopg2.sql.SQL(',').join([self.psycopg2.sql.Placeholder(col) for col in self.target.model_columns.keys()]))
        )
        self.cxn.commit()

    def recreate_object(self, target_table, schema, primary_key_list):
        '''Convenience wrapper for drop and create methods.'''
        self.drop_object(target_table, schema)
        self.create_object(target_table, schema, primary_key_list)
