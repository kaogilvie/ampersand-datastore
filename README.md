# AMPERSAND DATASTORE
&amp;KO upload interface to any arbitrary amount of datastores

### Python Version
Until [this PR](https://github.com/snowflakedb/snowflake-connector-python/pull/1294) has been merged, we are stuck on python 3.10.x
for this repo. The snowflake-connector-python cannot be built without the
python 3.11 wheels being published (fails at pyarrow).