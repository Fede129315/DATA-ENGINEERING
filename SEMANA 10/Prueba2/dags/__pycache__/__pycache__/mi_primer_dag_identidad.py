from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def tarea1():
    print("Mi país es Argentina")
    # Código adicional de la tarea 1
    pass

def tarea2():
    print("Mi nombre falso es Juan")
    # Código de la tarea 2
    pass

def tarea3():
    print("Mi apellido falso es Perez")
    # Código adicional de la tarea 3
    pass

# Definición del DAG
dag = DAG(
    'mi_primer_dag_identidad',
    description='Ejemplo de DAG en Airflow con nombre cambiado e impresiones modificadas',
    start_date=datetime(2023, 7, 4),
    schedule_interval='0 0 * * *'  # Programación diaria a las 00:00
)

# Definición de los operadores (tareas) en el DAG
inicio = DummyOperator(task_id='inicio', dag=dag)
tarea_1 = PythonOperator(task_id='tarea1', python_callable=tarea1, dag=dag)
tarea_2 = PythonOperator(task_id='tarea2', python_callable=tarea2, dag=dag)
tarea_3 = PythonOperator(task_id='tarea3', python_callable=tarea3, dag=dag)
fin = DummyOperator(task_id='fin', dag=dag)

# Definición de las dependencias entre las tareas
inicio >> tarea_1 >> tarea_2 >> tarea_3 >> fin