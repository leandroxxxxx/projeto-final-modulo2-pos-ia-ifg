from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime


def minha_funcao():
    print("Olá, Airflow!")
    print("Pipeline funcionando.")


with DAG(
    dag_id="primeira_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    tarefa = PythonOperator(
        task_id="teste_python",
        python_callable=minha_funcao,
    )