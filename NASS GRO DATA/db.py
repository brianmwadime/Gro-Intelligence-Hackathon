import psycopg2

_connection = None

def get_connection(database_host, database_name, database_user, database_password):
    global _connection
    if not _connection:
        _connection = psycopg2.connect("dbname='"+database_name+"'user='"+database_user+"'password='"+database_password+"'host='" + database_host +"'")
    return _connection

# List of stuff accessible to importers of this module. Just in case
__all__ = ['getConnection']

## Edit: actually you can still refer to db._connection
##         if you know that's the name of the variable.
## It's just left out from enumeration if you inspect the module
