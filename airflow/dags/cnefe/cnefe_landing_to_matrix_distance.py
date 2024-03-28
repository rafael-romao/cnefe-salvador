from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
import pendulum


CONFIG = {
    "spark.jars.ivy":"/opt/bitnami/spark/ivy"
}




@dag(
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        schedule=None,          
        catchup=False,
        tags=['cnefe']
)
def cnefe_landing_to_matrix_distance():

    landing_to_raw= SparkSubmitOperator(
        task_id="landing_to_raw",
        conn_id="spark_default",
        application="./app/landing_to_raw.py",
        conf=CONFIG,
        name="cnefe_landing_to_raw",
        retries=2
    )

    base_a = SparkSubmitOperator(
        task_id="base_a",
        conn_id="spark_default",
        application="./app/base_a.py",
        conf=CONFIG,
        name="base_a",
        retries=2
    )

    base_b = SparkSubmitOperator(
        task_id="base_b",
        conn_id="spark_default",
        application="./app/src/base_b.py",
        conf=CONFIG,
        name="base_a",
        retries=2
    )

    base_matrix_distance = SparkSubmitOperator(
        task_id="create_distance_matrix",
        conn_id="spark_default",
        application="./app/src/create_distance_matrix.py",
        conf=CONFIG,
        name="base_a",
        retries=2
    )


    start = EmptyOperator(task_id="start")
    
    done = EmptyOperator(task_id="done")

    start >> landing_to_raw >> base_a >> base_b >> base_matrix_distance >> done

_ = cnefe_landing_to_matrix_distance()
    