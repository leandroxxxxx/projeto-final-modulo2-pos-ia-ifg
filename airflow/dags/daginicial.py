from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    start_date=datetime(2026, 7, 3),
    schedule="0 20 * * *",
    catchup=False,
    tags=["exemplo_trigger"],
)
def dag_controladora():

    @task
    def tarefa_inicial():
        print("Preparando dados antes de chamar a próxima DAG...")
        return {"data_info": "processado_via_taskflow"}

    # Passamos os dados gerados pela task anterior via 'conf'
    # O operador tradicional funciona perfeitamente ao lado dos decorators
    chamar_proxima_dag = TriggerDagRunOperator(
        task_id="chamar_dag_alvo",
        trigger_dag_id="example_astronauts",
        conf={"payload": "{{ task_instance.xcom_pull(task_ids='tarefa_inicial') }}"},
        wait_for_completion=True, # Aguarda a DAG alvo terminar
    )

    @task
    def tarefa_final():
        print("DAG alvo finalizada com sucesso! Concluindo pipeline principal.")

    # Definindo o fluxo (as dependências)
    tarefa_inicial() >> chamar_proxima_dag >> tarefa_final()

# Instancia a DAG para o Airflow localizá-la
dag_controladora()