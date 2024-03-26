from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
import pendulum


@dag(
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        schedule=None,          
        catchup=False,
        tags=['cnefe']
)
def cnefe_landing_to_curated():

    landing_to_raw= SparkSubmitOperator(
        task_id="landing_to_raw",
        conn_id="spark",
        application="../../../spark/app/landing_to_raw.py",
        name="cnefe_landing_to_raw",
        retries=2
    )

    base_a = SparkSubmitOperator(
        task_id="base_a",
        conn_id="spark",
        application="../../../spark/app/base_a.py",
        name="base_a",
        retries=2
    )


    start = EmptyOperator(task_id="start")
    
    done = EmptyOperator(task_id="done")

    start >> landing_to_raw >> base_a >> done
    