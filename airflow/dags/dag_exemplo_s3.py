import logging
from datetime import datetime
from airflow.decorators import dag, task

@dag(
    start_date=datetime(2026, 7, 3),
    schedule="0 20 * * *",
    catchup=False,
    tags=["exemplo_s3"],
)
def dag_exemplo_s3():

    @task
    def gerar_dados():
        import pandas as pd
        import random

        print("Gerando dados...")
        nomes = [
            "Ana Souza", "Carlos Oliveira", "Mariana Lima", "Pedro Silva",
            "Julia Alves", "Rafael Costa", "Beatriz Rocha", "Lucas Pereira",
            "Amanda Castro", "Fernando Gomes"
        ]
        cidades = [
            "São Paulo", "Rio de Janeiro", "Belo Horizonte", "Porto Alegre",
            "Brasília", "Curitiba", "Recife", "Fortaleza", "Salvador", "Natal"
        ]

        df = pd.DataFrame({
            "nome": nomes,
            "idade": [random.randint(18, 65) for _ in range(10)],
            "email": [nome.lower().replace(" ", ".") + "@exemplo.com" for nome in nomes],
            "cidade": cidades
        })
        caminho_arquivo = "/tmp/pessoas.csv"
        df.to_csv(caminho_arquivo, index=False)
        logging.info(f"Arquivo {caminho_arquivo} gerado com sucesso!")
   
        return caminho_arquivo

    @task
    def enviar_dados_para_s3(caminho_arquivo: str):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        logging.info("Enviando dados para S3...")
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_file(
            filename=caminho_arquivo,
            key="pessoas.csv",
            bucket_name="entrega-do-trabalho-em-15-de-agosto",
            replace=True
        )
        logging.info("Dados enviados para S3 com sucesso!")
   

    # Definindo o fluxo (as dependências)
    caminho_arquivo = gerar_dados()
    enviar_dados_para_s3(caminho_arquivo)

# Instancia a DAG para o Airflow localizá-la
dag_exemplo_s3()
