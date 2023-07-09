from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import scripts

default_args = {
    'start_date': datetime(2023, 7, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False
}

# Definición del DAG
dag = DAG(
    'etl_dag_2',
    default_args=default_args,
    description='DAG for ETL process 2',
    schedule_interval='@daily',
)

extraer_copa_mundo = PythonOperator(
    task_id='extraer_copa_mundo',
    python_callable=scripts.extraer_copa_mundo,
    dag=dag,
)

extraer_ranking = PythonOperator(
    task_id='extraer_ranking',
    python_callable=scripts.extraer_ranking,
    dag=dag,
)


transformar = PythonOperator(
    task_id='transformar',
    python_callable=scripts.transformar,
    dag=dag,
)

cargar = PythonOperator(
    task_id='cargar',
    python_callable=scripts.cargar,
    dag=dag,
)



# Definición de los operadores (tareas) en el DAG
inicio = DummyOperator(task_id='inicio', dag=dag)
fin = DummyOperator(task_id='fin', dag=dag)

# Definición de las dependencias entre las tareas
inicio >>  (extraer_ranking,extraer_copa_mundo) >> transformar >> cargar >>  fin