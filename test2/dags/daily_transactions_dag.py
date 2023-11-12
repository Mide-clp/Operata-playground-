import os
import boto3
import datetime as dt
from sqlalchemy import create_engine
from pendulum import datetime, duration
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from include.helpers.slack import send_notification as slack_notify
# from include.helpers.utils import establish_connection
from include.sql.daily_transaction_sql import (
    users_insert,
    transaction_insert
)
from include.scripts.daily_transaction_callables import (
    get_transaction_data,
    write_data_to_db,
    transform_user_data,
    transform_transaction_data

)
from airflow.decorators import (
    dag,
    task,
    task_group
)

BUCKET = "mide-product-dump"
BUCKET_KEY = "raw/transactions/transactions-{}.csv"
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK")
host = os.environ.get("DB_HOST")
user = str(os.environ.get("USER"))
port = str(os.environ.get("PORT"))
db = os.environ.get("DB")
password = os.environ.get("PASS")

database_obj = {
    "host": host,
    "user": user,
    "port": port,
    "db": db,
    "password": password,
}
database_uri = f"postgresql://{user}:{password}@{host}:{port}/{db}"


@dag(
    start_date=datetime(2023, 11, 9),
    schedule="0 9 * * *",
    max_active_runs=1,
    catchup=True,
    default_args={
        "owner": "mide",
        "retries": 1,
        "retry_delay": duration(
            minutes=30
        )
    }

)
def daily_transaction_to_db():
    """
     DAG for processing daily transactions and writing to the database.
    """
    today_run_date = "{{ execution_date.strftime('%Y-%m-%d') }}"

    start = EmptyOperator(task_id="start_task")
    end = EmptyOperator(task_id="end_task", trigger_rule="none_failed")

    @task(retries=7)
    def check_file_availability(run_date: str):
        """
        Extract task to check file availability.

        :param run_date: The date for which the data is being processed.
        """

        # prev_file_date = str((dt.datetime.strptime(run_date, "%Y-%m-%d") - dt.timedelta(days=1)).strftime("%Y-%m-%d"))
        print("run for: ",run_date)
        s3 = boto3.resource("s3")

        context = get_current_context()
        ti = context["ti"]

        try:
            s3.Object(BUCKET, BUCKET_KEY.format(run_date)).get()

        except Exception as e:
            ti.xcom_push("check_file_error", str(e))
            raise e

    @task(trigger_rule="one_failed")
    def send_slack_notification():
        """
        Task to send a Slack notification in case of failure.
        """
        context = get_current_context()
        ti = context["ti"]

        dag_id = "daily_transaction_to_db"
        dag_url = f"http://localhost:8080/dags/{dag_id}/grid"
        title = "Failure Running Dag"
        message = ti.xcom_pull(key="check_file_error",
                               task_ids="check_file_availability")
        slack_notify(SLACK_WEBHOOK, dag_id, dag_url, title, message)

    @task_group()
    def write_to_user(run_date: str):
        """
        Transform task group for processing user data.

        :param run_date: The date for which the data is being processed.
        """
        s3_path = f"{BUCKET}/{BUCKET_KEY}"

        # Get new user transaction data
        transaction_data = get_transaction_data.override(
            task_id="get_new_user_transaction",
            trigger_rule="none_failed"
        )(run_date, s3_path)

        # Transform user data
        ready_user_data = transform_user_data.override(
            trigger_rule="none_failed"
        )(transaction_data)

        # Write user data to the user table
        write_user_data = write_data_to_db.override(
            task_id="write_user_data_to_user_table",
            trigger_rule="none_failed"
        )(database_obj, users_insert, ready_user_data)

        transaction_data >> ready_user_data >> write_user_data

    @task_group()
    def write_to_transactions(run_date):
        """
        Transform task group for processing transaction data.

        :param run_date: The date for which the data is being processed.
        """

        s3_path = f"{BUCKET}/{BUCKET_KEY}"

        # Get new transactions data
        transaction_data = get_transaction_data.override(
            task_id="get_new_transactions",
            trigger_rule="none_failed"
        )(run_date, s3_path)

        # Transform transaction data
        ready_transaction_data = transform_transaction_data.override(
            trigger_rule="none_failed"
        )(transaction_data, database_uri)

        # Write transaction data to the transaction table
        write_transaction_data = write_data_to_db.override(
            task_id="write_transaction_data_to_transaction_table",
            trigger_rule="none_failed"
        )(database_obj, transaction_insert, ready_transaction_data)

        transaction_data >> ready_transaction_data >> write_transaction_data

    check_file = check_file_availability(today_run_date)
    send_notification = send_slack_notification()
    write_to_user = write_to_user(today_run_date)
    write_to_transactions = write_to_transactions(today_run_date)

    start >> check_file >> send_notification >> end
    check_file >> write_to_user >> write_to_transactions >> end


daily_transaction_to_db()
