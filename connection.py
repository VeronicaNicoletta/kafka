import psycopg2


def connect():
    conn = psycopg2.connect(
        host='localhost',
        port = 5432,
        db_name = 'document',
        user = 'postgres',
        password = 'Vera@')

    return conn