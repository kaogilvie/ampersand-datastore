# dev only
creds = {
    'user': os.environ['SNOWFLAKE_USER'],
    'password': os.environ['SNOWFLAKE_PASSWORD'],
    'account': os.environ['SNOWFLAKE_ACCOUNT'],
    'database': 'COACH_KATIE',
    'warehouse': 'COMPUTE_WH',
    'schema': 'MT'
}

sno = Snowflake()
sno.get_cursor(creds)

from marianatek.admin import AdminClient
admin = AdminClient()
admin.model_columns = {'test': 'text[]', 'prikey': 'int', 'tim': 'timestamp'}
admin.data = [{'test': ['yesgirl'], 'prikey': 1, 'tim': '2020-09-02T11:01:07.676355Z'}]

sno.stage_object(admin, 'test')
# sno.create_object('test_table', 'MT', '')
# sno.drop_object('test_table', 'MT')
# sno.append_object('test_table', 'MT', '')
sno.recreate_object('test_table', 'MT', ['prikey'])
sno.upsert_object('test_table', 'MT', ['prikey'])

test_array = ["this", 'is', 'a', 'test', 'array']
str(test_array)
