from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import etl_script

default_args = {
    'start_date': datetime(2023, 7, 6),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='DAG for ETL process',
    schedule_interval='@daily',
)

connection_open = PythonOperator(
    task_id='connection_open',
    python_callable=etl_script.connection_open,
    dag=dag,
)

create_table = PythonOperator(
    task_id='create_table',
    python_callable=etl_script.create_table,
    dag=dag,
)

extract_data_worldcup = PythonOperator(
    task_id='extract_data_worldcup',
    python_callable=etl_script.extract_data_worldcup,
    dag=dag,
)

extract_data_ranking = PythonOperator(
    task_id='extract_data_ranking',
    python_callable=etl_script.extract_data_ranking,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=etl_script.transform_data,
    dag=dag,
)

check_duplicates = PythonOperator(
    task_id='check_duplicates',
    python_callable=etl_script.check_duplicates,
    dag=dag,
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=etl_script.load_data,
    dag=dag,
)




# Definición de los operadores (tareas) en el DAG
inicio = DummyOperator(task_id='inicio', dag=dag)
connection_open = PythonOperator(task_id='connection_open', python_callable=connection_open, dag=dag)
create_table = PythonOperator(task_id='create_table', python_callable=create_table, dag=dag)
extract_data_worldcup = PythonOperator(task_id='extract_data_worldcup', python_callable=extract_data_worldcup,dag=dag)
extract_data_ranking = PythonOperator(task_id='extract_data_ranking', python_callable=extract_data_ranking,dag=dag)
transform_data = PythonOperator(task_id='transform_data', python_callable=transform_data,dag=dag)
check_duplicates = PythonOperator(task_id='check_duplicates', python_callable=check_duplicates,dag=dag)
load_data = PythonOperator(task_id='load_data', python_callable=load_data,dag=dag)
fin = DummyOperator(task_id='fin', dag=dag)

# Definición de las dependencias entre las tareas
inicio >> connection_open >> create_table >> extract_data_worldcup >> extract_data_ranking >> transform_data >> check_duplicates >> load_data >> fin