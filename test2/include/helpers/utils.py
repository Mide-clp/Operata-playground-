import psycopg2


def establish_connection(host, port, db, user, password):
    """
    Establish a connection to a PostgreSQL database.

    :param host: PostgreSQL host address
    :param port:  PostgreSQL port number. Default is 5432.
    :param db:  PostgreSQL database name.
    :param user:  PostgreSQL database user.
    :param password: PostgreSQL database password.
    :return: Cursor for executing SQL queries.
    """

    # connection for pandas
    # engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    # engine.connect()
    conn = psycopg2.connect(host=host, database=db, user=user, password=password, port=port)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    print(f"connected to {db} database")

    return cur


