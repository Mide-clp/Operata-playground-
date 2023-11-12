import pandas as pd
import json
import os
import psycopg2
from copy import deepcopy
from datetime import datetime
from uuid import uuid4
from utils import (establish_connection, run_sql_query, write_batch_to_db)
from queries import (create_user_table, users_insert, create_transaction_table, transaction_insert)


def get_user_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function calculates the 'nTransactions' column and generates a 'userUuid'
    :param df: pandas.DataFrame
        Input DataFrame containing transaction data.

    :return: pandas.DataFrame
        Processed DataFrame containing user data.
    """
    df["nTransactions"] = 1
    df_users_comp = df.groupby("agentPhoneNumber").agg({"nTransactions": "sum"}).reset_index()
    df_users_comp["userUuid"] = df_users_comp["agentPhoneNumber"].apply(lambda x: str(uuid4()))
    df_users_comp.rename(columns={"userUuid": "uuid"}, inplace=True)

    return df_users_comp


def get_transaction_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function transforms the 'date' column to a timestamp format, updating 'requestTimestamp' and 'updateTimestamp'
    columns with the converted values.

    :param df: pandas.DataFrame
        Input DataFrame containing transaction data.

    :return: pandas.DataFrame
        Processed DataFrame containing transaction data.
    """
    df["date"] = df["date"].apply(
        lambda x: int(
            datetime.strptime(x, "%Y-%m-%d %H:%M:%S").timestamp() * 1000
        )
    )

    df["requestTimestamp"] = df["date"]
    df["updateTimestamp"] = df["date"]

    return df


def create_table(cur):
    """
    Create user and transaction tables in the database.
    :param cur:
        Database cursor for executing SQL queries.
    """
    print("creating user table........")
    run_sql_query(cur, create_user_table)
    print("creating transaction table........")
    run_sql_query(cur, create_transaction_table)


if __name__ == "__main__":

    transactions_columns = ["agentPhoneNumber", "receiverPhoneNumber", "transactionType", "userUuid", "balance",
                            "commission", "amount", "requestTimestamp", "updateTimestamp", "externalId"]
    dir = ["/Users/apple/Downloads/transactions-1.csv", "/Users/apple/Downloads/transactions-2.csv"]

    host = os.environ.get("DB_HOST")
    user = str(os.environ.get("USER"))
    port = str(os.environ.get("PORT"))
    db = os.environ.get("DB")
    password = os.environ.get("PASS")
    cur = establish_connection(host=host, user=user, port=port, db=db,
                               password=password)
    create_table(cur)

    df_transaction = pd.concat(map(pd.read_csv, dir))
    df_transaction.drop_duplicates(inplace=True)

    # users data
    df_users = deepcopy(df_transaction)
    users_df_data = get_user_data(df_users)
    print("writing data to user table........")
    write_batch_to_db(cur, users_insert, json.loads(users_df_data.to_json(orient="records")))

    # transaction_data
    # format transaction
    df_transaction_join = get_transaction_data(df_transaction).merge(users_df_data, on="agentPhoneNumber", how="inner")
    df_transaction_join.rename(columns={"uuid": "userUuid"}, inplace=True)
    transaction_df_data = df_transaction_join[transactions_columns].to_json(orient="records")
    print("writing data to transaction table........")
    write_batch_to_db(cur, transaction_insert, json.loads(transaction_df_data))
