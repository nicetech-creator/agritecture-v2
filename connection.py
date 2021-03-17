from psycopg2 import OperationalError
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor

# Specify SQL connection details
DEFAULT_DB_NAME = 'models'

CONNECTION_DATA = {
    'models': {
        'CONNECTION_NAME': 'agritecture-prototyping:us-east1:models-db',
        'DB_USER': 'db-user',
        'DB_PASSWORD': 'agritecture123',
        'DB_NAME': 'models'
    },
    'location': {
        'CONNECTION_NAME': 'agritecture-prototyping:us-east1:models-db',
        'DB_USER': 'db-user',
        'DB_PASSWORD': 'agritecture123',
        'DB_NAME': 'location'
    }
}


# Connection pools reuse connections between invocations,
# and handle dropped or expired connections automatically.
pg_pool = None
pg_pool_db_name = None


def __connect(host, dbname):
    """
    Helper function to connect to Postgres
    """
    global pg_pool, pg_pool_db_name
    pg_config = {
        'user': CONNECTION_DATA[dbname]['DB_USER'],
        'password': CONNECTION_DATA[dbname]['DB_PASSWORD'],
        'host': host,
        'dbname': dbname
    }

    pg_pool = ThreadedConnectionPool(1, 1, **pg_config)
    pg_pool_db_name = dbname


def fetch_all(query, query_args=None, dbname=DEFAULT_DB_NAME):
    """ Fetches all results for a given query (and query arguments).

    :param query: SQL query to be fetched
    :param query_args (optional): Arguments to be passed to the query,
            in pyformat syntax (see https://wiki.python.org/moin/DbApiFaq)
    :param dbname (optional): Name of the database to connect, default is assumed

    :return Results from calling fetchall() on the constructed query
    """
    global pg_pool, pg_pool_db_name

    # Initialize the pool lazily, in case SQL access isn't needed for this
    # GCF instance. Doing so minimizes the number of active SQL connections,
    # which helps keep your GCF instances under SQL connection limits.
    if not pg_pool or dbname != pg_pool_db_name:
        try:
            CONNECTION_NAME = CONNECTION_DATA[dbname]['CONNECTION_NAME']
            __connect(f'/cloudsql/{CONNECTION_NAME}', dbname)
        except OperationalError:
            # If production settings fail, use local development ones
            __connect('35.243.135.22', dbname)

    with pg_pool.getconn() as conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if query_args is None:
                    # print('Query: ' + query)
                    cursor.execute(query)
                else:
                    # print('Query: ' + query)
                    # print('Query args: ' + str(query_args))
                    cursor.execute(query, query_args)
                return cursor.fetchall()
        finally:
            pg_pool.putconn(conn)


def fetch_one(query, query_args=None, dbname=DEFAULT_DB_NAME):
    """Fetches the first result for a given query (and query arguments).

    :param query: SQL query to be fetched
    :param query_args (optional): Arguments to be passed to the query,
            in pyformat syntax (see https://wiki.python.org/moin/DbApiFaq)
    :param dbname (optional): Name of the database to connect, default is assumed

    :return Results from calling fetchone() on the constructed query
    """
    global pg_pool, pg_pool_db_name

    # Initialize the pool lazily, in case SQL access isn't needed for this
    # GCF instance. Doing so minimizes the number of active SQL connections,
    # which helps keep your GCF instances under SQL connection limits.
    if not pg_pool or dbname != pg_pool_db_name:
        try:
            CONNECTION_NAME = CONNECTION_DATA[dbname]['CONNECTION_NAME']
            __connect(f'/cloudsql/{CONNECTION_NAME}', dbname)
        except OperationalError:
            # If production settings fail, use local development ones
            __connect('35.243.135.22', dbname)

    with pg_pool.getconn() as conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if query_args is None:
                    # print('Query: ' + query)
                    cursor.execute(query)
                else:
                    # print('Query: ' + query)
                    # print('Query args: ' + str(query_args))
                    cursor.execute(query, query_args)
                return cursor.fetchone()
        finally:
            pg_pool.putconn(conn)


def execute(query, query_args=None, dbname=DEFAULT_DB_NAME):
    """Executes a query (with the given query arguments).

    :param query:   SQL query to be executed
    :param query_args:  Optional, arguments to be passed to the query
        in pyformat syntax (see https://wiki.python.org/moin/DbApiFaq)
    :param dbname (optional): Name of the database to connect, default is assumed
    """
    global pg_pool, pg_pool_db_name

    # Initialize the pool lazily, in case SQL access isn't needed for this
    # GCF instance. Doing so minimizes the number of active SQL connections,
    # which helps keep your GCF instances under SQL connection limits.
    if not pg_pool or dbname != pg_pool_db_name:
        try:
            CONNECTION_NAME = CONNECTION_DATA[dbname]['CONNECTION_NAME']
            __connect(f'/cloudsql/{CONNECTION_NAME}', dbname)
        except OperationalError:
            # If production settings fail, use local development ones
            __connect('localhost', dbname)

    with pg_pool.getconn() as conn:
        try:
            with conn.cursor() as cursor:
                if query_args is None:
                    # print('Query: ' + query)
                    cursor.execute(query)
                else:
                    # print('Query: ' + query)
                    # print('Query args: ' + str(query_args))
                    cursor.execute(query, query_args)

        finally:
            pg_pool.putconn(conn)


def executemany(query, query_args, dbname=DEFAULT_DB_NAME):
    """Executes a query many times with the given sequence of query arguments.

    :param query:   SQL query to be executed
    :param query_args:  Sequence of arguments to be passed to the query
        in pyformat syntax (see https://wiki.python.org/moin/DbApiFaq)
    :param dbname (optional): Name of the database to connect, default is assumed
    """
    global pg_pool, pg_pool_db_name

    # Initialize the pool lazily, in case SQL access isn't needed for this
    # GCF instance. Doing so minimizes the number of active SQL connections,
    # which helps keep your GCF instances under SQL connection limits.
    if not pg_pool or dbname != pg_pool_db_name:
        try:
            CONNECTION_NAME = CONNECTION_DATA[dbname]['CONNECTION_NAME']
            __connect(f'/cloudsql/{CONNECTION_NAME}', dbname)
        except OperationalError:
            # If production settings fail, use local development ones
            __connect('localhost', dbname)

    with pg_pool.getconn() as conn:
        try:
            with conn.cursor() as cursor:
                # print('Query: ' + query)
                cursor.executemany(query, query_args)

        finally:
            pg_pool.putconn(conn)
