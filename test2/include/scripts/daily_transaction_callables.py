import json
import hashlib
from sqlalchemy import create_engine
from airflow.decorators import task
from psycopg2.extras import execute_batch
import psycopg2
import pandas as pd
import awswrangler as wr
import datetime as dt


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

    conn = psycopg2.connect(host=host, database=db, user=user, password=password, port=port)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    print(f"connected to {db} database")

    return cur


def hash_row(row: pd.Series) -> str:
    """
    Generate an MD5 hash for a row.

    :param row: A pandas Series representing a row of data.
    :return: MD5 hash of the concatenated values in the row.
    """
    return hashlib.md5(
        (str(row["requestTimestamp"]) +
         str(row["agentPhoneNumber"]) +
         str(row["externalId"])
         ).encode("utf-8")).hexdigest()


@task()
def get_transaction_data(run_date: str, s3_path: str) -> pd.DataFrame:
    """
    Read transaction data from an S3 path.

    :param run_date: The date for which the data is being processed.
    :param s3_path: S3 path for the data.
    :return: Transaction data read from the specified S3 path.
    """
    # prev_file_date = str((dt.datetime.strptime(run_date, "%Y-%m-%d") - dt.timedelta(days=1)).strftime("%Y-%m-%d"))

    s3_file_path = [f"s3://{s3_path.format(run_date)}"]

    return wr.s3.read_csv(s3_file_path)


@task()
def transform_user_data(df: pd.DataFrame) -> list[dict]:
    """
     Transform user data by removing duplicates.

    :param df: User data DataFrame.
    :return: JSON representation of the transformed user data.
    """
    df.drop_duplicates(subset=["agentPhoneNumber"], inplace=True)

    return json.loads(df.to_json(orient="records"))


@task()
def transform_transaction_data(df: pd.DataFrame, uri: str) -> list[dict]:
    """
    Transform transaction data by applying necessary changes and joining with user data.

    :param df:  Transaction data DataFrame.
    :param uri: URI for connecting to the database.
    :return:  JSON representation of the transformed transaction data.
    """
    engine = create_engine(uri)
    engine.connect()
    transactions_columns = ["agentPhoneNumber", "receiverPhoneNumber", "transactionType", "userUuid", "balance",
                            "commission", "amount",
                            "requestTimestamp", "updateTimestamp", "externalId", "rowId"]

    df["date"] = df["date"].apply(
        lambda x: int(
            dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").timestamp() * 1000
        )
    )

    df["agentPhoneNumber_filter"] = df["agentPhoneNumber"].apply(lambda x: f"'{x}'")
    df["agentPhoneNumber"] = df["agentPhoneNumber"].map(str)
    df["requestTimestamp"] = df["date"]
    df["updateTimestamp"] = df["date"]
    df["rowId"] = df.apply(hash_row, axis=1)

    user_condition = ", ".join(list(df["agentPhoneNumber_filter"].unique()))
    sql = f'select * from public.user_airflow where "phoneNumber" in ({user_condition})'
    df_user = pd.read_sql(sql=sql, con=engine)
    df_join = df.merge(df_user, left_on="agentPhoneNumber", right_on=["phoneNumber"], how="inner")
    df_join["uuid"] = df_join["uuid"].map(str)
    df_join.rename(columns={"uuid": "userUuid"}, inplace=True)
    df_join.drop_duplicates(inplace=True)

    return json.loads(df_join[transactions_columns].to_json(orient="records"))


@task()
def write_data_to_db(database_obj: dict, query: str, data: list[dict]):
    """
    Write data to a PostgreSQL database.

    :param database_obj: Dictionary containing database connection details (host, user, port, db, password).
    :param query: SQL query for inserting data.
    :param data: List of dictionaries representing the data to be inserted.
    :return:
    """
    cur = establish_connection(host=database_obj["host"], user=database_obj["user"], port=database_obj["port"],
                               db=database_obj["db"],
                               password=database_obj["password"])
    execute_batch(cur, query, data, page_size=1000)
    print("batch insert complete")
