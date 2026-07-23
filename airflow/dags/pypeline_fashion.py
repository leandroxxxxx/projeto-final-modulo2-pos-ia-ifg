import io
import os
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

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
from airflow.operators.bash import BashOperator

# ── Carregar variáveis do .env (se existir) ──────────────────────────────
_env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=_env_path)

# ── Configurações S3 / Floci ─────────────────────────────────────────────
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://host.docker.internal:4566")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET_NAME = os.getenv("BUCKET_NAME", "dataset-project")
CSV_FILE_KEY = os.getenv("CSV_FILE_KEY", "styles_sample.csv")

# ── Parâmetros Snowflake ─────────────────────────────────────────────────
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "FOX")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "SFEDU02-GFB24387")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "TRAINING_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "FOX_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "FOX_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW_FASHION")
SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv(
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    "/usr/local/airflow/include/rsa_key.p8",
)

# ── Parâmetros dbt ───────────────────────────────────────────────────────
DBT_PROJECT_DIR = os.getenv(
    "DBT_PROJECT_DIR",
    "/usr/local/airflow/include/dbt/floci_dbt",
)
DBT_PROFILES_DIR = os.getenv(
    "DBT_PROFILES_DIR",
    "/usr/local/airflow/include/.dbt",
)

# ── Airflow ──────────────────────────────────────────────────────────────
AIRFLOW_OWNER = os.getenv("AIRFLOW__CORE__DAG_OWNER", "leandro")

# ── Schema / tabelas ─────────────────────────────────────────────────────
SNOWFLAKE_TABLE_RAW = os.getenv("SNOWFLAKE_TABLE_RAW", "PRODUCTS_RAW")
SNOWFLAKE_TABLE_RAW_FQN = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE_RAW}"


# ── Funções das tasks ────────────────────────────────────────────────────

def check_or_create_bucket():
    print("Verificando se o bucket existe no S3...")
    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"O bucket '{BUCKET_NAME}' já existe e está pronto!")
    except s3_client.exceptions.ClientError:
        print(
            f"Aviso: O bucket '{BUCKET_NAME}' não existia no S3 local. "
            "Criando-o agora..."
        )
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print(
            "Bucket vazio criado. Lembre-se de rodar o upload_floci.py "
            "para enviar os arquivos!"
        )


def process_and_load_to_snowflake():
    # 1. Conectar ao S3 (Floci)
    print("Conectando ao S3 (Floci)...")
    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    # 2. Ler o arquivo CSV estruturado
    print("Lendo o arquivo CSV...")
    csv_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=CSV_FILE_KEY)
    df = pd.read_csv(io.BytesIO(csv_obj["Body"].read()), on_bad_lines="skip")

    mean_r_list, mean_g_list, mean_b_list = [], [], []
    std_r_list, std_g_list, std_b_list = [], [], []
    brightness_list = []
    file_size_list = []

    print("Iniciando a extração de atributos das imagens...")
    for idx, row in df.iterrows():
        image_key = f"images/{int(row['id'])}.jpg"
        try:
            img_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=image_key)
            img_bytes = img_obj["Body"].read()

            # Guardar o tamanho em bytes
            file_size_list.append(len(img_bytes))

            # Abrir imagem e converter para RGB
            img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
            img_array = np.array(img)

            r_channel = img_array[:, :, 0]
            g_channel = img_array[:, :, 1]
            b_channel = img_array[:, :, 2]

            mean_r_list.append(float(np.mean(r_channel)))
            mean_g_list.append(float(np.mean(g_channel)))
            mean_b_list.append(float(np.mean(b_channel)))

            # Calcular o desvio padrão (contraste)
            std_r_list.append(float(np.std(r_channel)))
            std_g_list.append(float(np.std(g_channel)))
            std_b_list.append(float(np.std(b_channel)))

            # Calcular a luminosidade (brilho)
            brightness_val = float(
                np.mean(
                    0.299 * r_channel + 0.587 * g_channel + 0.114 * b_channel
                )
            )
            brightness_list.append(brightness_val)
        except Exception:
            file_size_list.append(None)
            mean_r_list.append(None)
            mean_g_list.append(None)
            mean_b_list.append(None)
            std_r_list.append(None)
            std_g_list.append(None)
            std_b_list.append(None)
            brightness_list.append(None)

    df["file_size_bytes"] = file_size_list
    df["mean_r"] = mean_r_list
    df["mean_g"] = mean_g_list
    df["mean_b"] = mean_b_list
    df["std_r"] = std_r_list
    df["std_g"] = std_g_list
    df["std_b"] = std_b_list
    df["brightness"] = brightness_list

    df_snowflake = df.rename(columns={
        "id": "ID",
        "gender": "GENDER",
        "masterCategory": "MASTER_CATEGORY",
        "subCategory": "SUB_CATEGORY",
        "articleType": "ARTICLE_TYPE",
        "baseColour": "BASE_COLOUR",
        "season": "SEASON",
        "year": "YEAR",
        "usage": "USAGE",
        "productDisplayName": "PRODUCT_DISPLAY_NAME",
        "mean_r": "MEAN_R",
        "mean_g": "MEAN_G",
        "mean_b": "MEAN_B",
        "file_size_bytes": "FILE_SIZE_BYTES",
        "brightness": "BRIGHTNESS",
        "std_r": "STD_R",
        "std_g": "STD_G",
        "std_b": "STD_B",
    })

    # 3. Ler a chave privada para a autenticação automática (Key-Pair)
    print("Carregando chave privada para autenticação...")
    with open(SNOWFLAKE_PRIVATE_KEY_PATH, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend(),
        )

    # Decodifica a chave para o formato binário (DER) exigido pelo Snowflake
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    print("Conectando ao Snowflake Online via Par de Chaves...")
    # 4. Conectar ao Snowflake usando a Chave Privada
    conn = connect(
        user=SNOWFLAKE_USER,
        account=SNOWFLAKE_ACCOUNT,
        private_key=pkb,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

    try:
        # Limpar a tabela antes de carregar os novos dados (evita duplicação)
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {SNOWFLAKE_TABLE_RAW_FQN};")
        cursor.close()
        print(f"Tabela {SNOWFLAKE_TABLE_RAW_FQN} truncada com sucesso.")

        print("Enviando dados consolidados para a tabela RAW...")
        sucesso, num_chunks, num_linhas, _ = write_pandas(
            conn=conn,
            df=df_snowflake,
            table_name=SNOWFLAKE_TABLE_RAW,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        print(f"Sucesso! {num_linhas} linhas carregadas no Snowflake.")
    finally:
        conn.close()


# ── Configuração da DAG ──────────────────────────────────────────────────

default_args = {
    "owner": AIRFLOW_OWNER,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="pipeline_fashion_elt",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["fashion", "elt", "snowflake"],
) as dag:

    # Task 1: Verifica ou cria o bucket no S3
    task_check_bucket = PythonOperator(
        task_id="check_or_create_s3_bucket",
        python_callable=check_or_create_bucket,
    )

    # Task 2: Processa as imagens e carrega no Snowflake
    task_process_and_load = PythonOperator(
        task_id="process_images_and_load_to_snowflake",
        python_callable=process_and_load_to_snowflake,
    )

    # Task 3: Rodar as transformações da camada Silver no dbt
    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            f"dbt run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # Task 4: Validar os dados com os testes do dbt
    test_dbt = BashOperator(
        task_id="test_dbt_models",
        bash_command=(
            f"dbt test "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # Define a ordem de execução das tarefas
    task_check_bucket >> task_process_and_load >> run_dbt >> test_dbt