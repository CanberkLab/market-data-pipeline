from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator

SPARK = (
    'spark-submit --master "local[2]" --conf spark.jars.ivy=/tmp/.ivy '
    '--packages "$SPARK_PACKAGES"'
)

default_args = {"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="market_data_pipeline",
    description="Kafka -> S3 bronze -> Spark silver/gold -> Postgres -> analytics views",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    template_searchpath=["/opt/airflow/sql"],
    tags=["market-data", "spark", "kafka"],
) as dag:

    # Demo için: gerçek hayatta bu görev yoktur, producer sürekli çalışır.
    produce_ticks = BashOperator(
        task_id="produce_ticks",
        bash_command="python /opt/airflow/producer/producer.py --date {{ ds }} --count 20000",
    )

    stream_ingest = BashOperator(
        task_id="stream_ingest_to_bronze",
        bash_command=f"{SPARK} /opt/airflow/spark_jobs/streaming_ingest.py --once",
    )

    # Prod'da: DatabricksSubmitRunOperator ile databricks/silver_gold_notebook.py çalıştırılabilir.
    transform = BashOperator(
        task_id="spark_transform_silver_gold",
        bash_command=f"{SPARK} /opt/airflow/spark_jobs/batch_transform.py --date {{{{ ds }}}}",
    )

    upsert = SQLExecuteQueryOperator(
        task_id="upsert_into_postgres",
        conn_id="market_dw",
        sql="upsert_ohlcv.sql",
    )

    check_rows = SQLCheckOperator(
        task_id="dq_check_rows_loaded",
        conn_id="market_dw",
        sql="SELECT COUNT(*) > 0 FROM market.ohlcv_1m WHERE bar_start::date = '{{ ds }}'",
    )

    check_ohlc = SQLCheckOperator(
        task_id="dq_check_ohlc_consistency",
        conn_id="market_dw",
        sql="""
            SELECT COUNT(*) = 0 FROM market.ohlcv_1m
            WHERE bar_start::date = '{{ ds }}'
              AND (high < low OR open NOT BETWEEN low AND high OR close NOT BETWEEN low AND high)
        """,
    )

    produce_ticks >> stream_ingest >> transform >> upsert >> [check_rows, check_ohlc]
