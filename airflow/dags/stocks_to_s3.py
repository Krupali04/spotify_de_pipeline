from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime
import requests, time, json

def fetch_and_load(**context):
    api_key = Variable.get("alpha_vantage_api_key")
    tickers = json.loads(Variable.get("stock_tickers"))
    s3 = S3Hook(aws_conn_id="aws_default")

    for symbol in tickers:
        resp = requests.get("https://www.alphavantage.co/query", params={
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": "compact",
            "apikey": api_key
        })
        resp.raise_for_status()
        data = resp.json()

        if "Time Series (Daily)" not in data:
            raise ValueError(f"Unexpected response for {symbol}: {data}")

        s3.load_string(
            json.dumps(data),
            key=f"stocks/symbol={symbol}/dt={context['ds']}/data.json",
            bucket_name="aplha-stocks-raw-data",  # <-- your actual bucket name
            replace=True
        )
        time.sleep(13)  # stay under 5 requests/minute

with DAG(
    "stocks_to_s3",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    task = PythonOperator(task_id="fetch_and_load", python_callable=fetch_and_load)