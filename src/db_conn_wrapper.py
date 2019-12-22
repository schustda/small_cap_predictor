from functools import wraps
import psycopg2
import json

def psycopg2_cur(conn_info):
    """
    Wrap function to setup and tear down a Postgres connection while
    providing a cursor object to make queries with.
    """
    def wrap(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            try:
                # Setup postgres connection
                connection = psycopg2.connect(**conn_info)
                cursor = connection.cursor()

                # Call function passing in cursor
                return_val = f(cursor, *args, **kwargs)

            finally:
                # Close connection
                connection.commit()
                connection.close()

            return return_val
        return wrapper
    return wrap

json_file = open('src/connection.json')
json_str = json_file.read()
PSQL_CONN = json.loads(json_str)

@psycopg2_cur(PSQL_CONN)
def tester(cursor):
    """Test function that uses our psycopg2 decorator
    """
    cursor.execute('SELECT 1 + 1')
    return cursor.fetchall()
