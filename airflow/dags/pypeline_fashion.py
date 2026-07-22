import io
import os
from datetime import datetime, timedelta
import boto3
import numpy as np
import pandas as pd
from PIL import Image
from airflow import DAG
from airflow.operators.python import PythonOperator
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
# Importações necessárias para decodificar a chave privada
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Configurações de Conexão - S3 / Floci
S3_ENDPOINT = "http://host.docker.internal:4566"  # Aponta para o Floci de dentro do Docker
BUCKET_NAME = "dataset-project"
CSV_FILE_KEY = "styles_sample.csv"

# Parâmetros oficiais do Snowflake
SNOWFLAKE_USER = "FOX"
SNOWFLAKE_ACCOUNT = None
SNOWFLAKE_ROLE = "TRAINING_ROLE"
SNOWFLAKE_WAREHOUSE = "FOX_WH"
SNOWFLAKE_DATABASE = "FOX_DB"
SNOWFLAKE_SCHEMA = "RAW_FASHION"

# Caminho interno do arquivo de chave privada dentro do contêiner Docker
KEY_PATH = "/usr/local/airflow/include/rsa_key.p8"


def check_or_create_bucket():
    print("Verificando se o bucket existe no S3...")
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"O bucket '{BUCKET_NAME}' já existe e está pronto!")
    except s3_client.exceptions.ClientError:
        print(f"Aviso: O bucket '{BUCKET_NAME}' não existia no S3 local. Criando-o agora...")
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print("Bucket vazio criado. Lembre-se de rodar o upload_floci.py para enviar os arquivos!")


def process_and_load_to_snowflake():
    # 1. Conectar ao S3 (Floci)
    print("Conectando ao S3 (Floci)...")
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

    # 2. Ler o arquivo CSV estruturado
    print("Lendo o arquivo CSV...")
    csv_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=CSV_FILE_KEY)
    df = pd.read_csv(io.BytesIO(csv_obj['Body'].read()), on_bad_lines='skip')

    mean_r_list, mean_g_list, mean_b_list = [], [], []

    print("Iniciando a extração de atributos das imagens...")
    for idx, row in df.iterrows():
        image_key = f"images/{int(row['id'])}.jpg"
        try:
            img_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=image_key)
            img = Image.open(io.BytesIO(img_obj['Body'].read())).convert('RGB')
            img_array = np.array(img)
            
            mean_r_list.append(float(np.mean(img_array[:, :, 0])))
            mean_g_list.append(float(np.mean(img_array[:, :, 1])))
            mean_b_list.append(float(np.mean(img_array[:, :, 2])))
        except Exception:
            mean_r_list.append(None)
            mean_g_list.append(None)
            mean_b_list.append(None)

    df['mean_r'] = mean_r_list
    df['mean_g'] = mean_g_list
    df['mean_b'] = mean_b_list

    # Tratar colunas para o Snowflake
    df_snowflake = df.rename(columns={
        'id': 'ID',
        'gender': 'GENDER',
        'masterCategory': 'MASTER_CATEGORY',
        'subCategory': 'SUB_CATEGORY',
        'articleType': 'ARTICLE_TYPE',
        'baseColour': 'BASE_COLOUR',
        'season': 'SEASON',
        'year': 'YEAR',
        'usage': 'USAGE',
        'productDisplayName': 'PRODUCT_DISPLAY_NAME',
        'mean_r': 'MEAN_R',
        'mean_g': 'MEAN_G',
        'mean_b': 'MEAN_B'
    })

    # 3. Ler a chave privada para a autenticação automática (Key-Pair)
    print("Carregando chave privada para autenticação...")
    with open(KEY_PATH, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    
    # Decodifica a chave para o formato binário (DER) exigido pelo Snowflake
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    print("Conectando ao Snowflake Online via Par de Chaves...")
    # 4. Conectar ao Snowflake usando a Chave Privada
    conn = connect(
        user=SNOWFLAKE_USER,
        account=SNOWFLAKE_ACCOUNT,
        private_key=pkb,  # <-- Autenticação sem senha e sem barreira de MFA!
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    try:
        print("Enviando dados consolidados para a tabela RAW...")
        sucesso, num_chunks, num_linhas, _ = write_pandas(
            conn=conn,
            df=df_snowflake,
            table_name='PRODUCTS_RAW',
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        print(f"Sucesso! {num_linhas} linhas carregadas no Snowflake.")
    finally:
        conn.close()


default_args = {
    'owner': 'leandro',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='pipeline_fashion_elt',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['fashion', 'elt', 'snowflake']
) as dag:

    # Task 1: Verifica ou cria o bucket no S3
    task_check_bucket = PythonOperator(
        task_id='check_or_create_s3_bucket',
        python_callable=check_or_create_bucket
    )

    # Task 2: Processa as imagens e carrega no Snowflake
    task_process_and_load = PythonOperator(
        task_id='process_images_and_load_to_snowflake',
        python_callable=process_and_load_to_snowflake
    )

    # Define a ordem de execução das tarefas
    task_check_bucket >> task_process_and_load