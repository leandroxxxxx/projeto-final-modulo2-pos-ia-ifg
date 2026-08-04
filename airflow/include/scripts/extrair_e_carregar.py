import io
import boto3
import numpy as np
import pandas as pd
from PIL import Image
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

# Configurações do S3 Local (Floci)
# Como estamos rodando LOCALMENTE fora do Docker, usamos 'localhost' novamente!
S3_ENDPOINT = "http://localhost:4566"  
BUCKET_NAME = "dataset-project"
CSV_FILE_KEY = "styles_sample.csv"

# Seus parâmetros oficiais do Snowflake acadêmico
SNOWFLAKE_USER = "FOX"
SNOWFLAKE_ACCOUNT = "SFEDU02-GFB24387"
SNOWFLAKE_ROLE = "TRAINING_ROLE"
SNOWFLAKE_WAREHOUSE = "FOX_WH"
SNOWFLAKE_DATABASE = "FOX_DB"
SNOWFLAKE_SCHEMA = "RAW_FASHION"

def executar_pipeline():
    # 1. Conectar ao S3 (Floci)
    print("Conectando ao S3 (Floci local)...")
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

    # 2. Ler o arquivo CSV
    print("Lendo o arquivo CSV do S3...")
    csv_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=CSV_FILE_KEY)
    df = pd.read_csv(io.BytesIO(csv_obj['Body'].read()), on_bad_lines='skip')

    mean_r_list, mean_g_list, mean_b_list = [], [], []
    std_r_list, std_g_list, std_b_list = [], [], []
    brightness_list = []
    saturacao_media_list = []
    complexidade_visual_list = []
    contraste_luminancia_list = []

    print("Iniciando a extração de atributos das imagens no S3...")
    for idx, row in df.iterrows():
        image_key = f"images/{int(row['id'])}.jpg"
        try:
            img_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=image_key)
            img = Image.open(io.BytesIO(img_obj['Body'].read())).convert('RGB')
            img_array = np.array(img, dtype=np.float64)
            
            r_channel = img_array[:, :, 0]
            g_channel = img_array[:, :, 1]
            b_channel = img_array[:, :, 2]

            mean_r_list.append(float(np.mean(r_channel)))
            mean_g_list.append(float(np.mean(g_channel)))
            mean_b_list.append(float(np.mean(b_channel)))

            std_r_list.append(float(np.std(r_channel)))
            std_g_list.append(float(np.std(g_channel)))
            std_b_list.append(float(np.std(b_channel)))

            # Luminância (brilho)
            luminancia = 0.299 * r_channel + 0.587 * g_channel + 0.114 * b_channel
            brightness_list.append(float(np.mean(luminancia)))

            # Contraste (desvio padrão da luminância)
            contraste_luminancia_list.append(float(np.std(luminancia)))

            # Saturação média (espaço HSV)
            hsv = np.array(img.convert("HSV"), dtype=np.float64)
            saturacao_media_list.append(float(hsv[:, :, 1].mean()))

            # Complexidade visual (gradiente horizontal da luminância)
            diffs_horizontais = np.abs(np.diff(luminancia, axis=1))
            complexidade_visual_list.append(float(diffs_horizontais.mean()))
        except Exception:
            mean_r_list.append(None)
            mean_g_list.append(None)
            mean_b_list.append(None)
            std_r_list.append(None)
            std_g_list.append(None)
            std_b_list.append(None)
            brightness_list.append(None)
            saturacao_media_list.append(None)
            complexidade_visual_list.append(None)
            contraste_luminancia_list.append(None)

    # Adiciona as colunas extraídas
    df['mean_r'] = mean_r_list
    df['mean_g'] = mean_g_list
    df['mean_b'] = mean_b_list
    df['std_r'] = std_r_list
    df['std_g'] = std_g_list
    df['std_b'] = std_b_list
    df['brightness'] = brightness_list
    df['saturacao_media'] = saturacao_media_list
    df['complexidade_visual'] = complexidade_visual_list
    df['contraste_luminancia'] = contraste_luminancia_list

    # Renomeia colunas para caixa alta (padrão Snowflake)
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
        'mean_b': 'MEAN_B',
        'std_r': 'STD_R',
        'std_g': 'STD_G',
        'std_b': 'STD_B',
        'brightness': 'BRIGHTNESS',
        'saturacao_media': 'SATURACAO_MEDIA',
        'complexidade_visual': 'COMPLEXIDADE_VISUAL',
        'contraste_luminancia': 'CONTRASTE_LUMINANCIA',
    })

    # 3. Conectar ao Snowflake usando o navegador externo (externalbrowser)
    print("Conectando ao Snowflake (Seu navegador abrirá para login)...")
    conn = connect(
        user=SNOWFLAKE_USER,
        account=SNOWFLAKE_ACCOUNT,
        authenticator='externalbrowser',  # Irá funcionar perfeitamente fora do Docker!
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    try:
        print("Enviando dados consolidados para o Snowflake...")
        sucesso, num_chunks, num_linhas, _ = write_pandas(
            conn=conn,
            df=df_snowflake,
            table_name='PRODUCTS_RAW',
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        print(f"Sucesso! {num_linhas} linhas carregadas na tabela PRODUCTS_RAW do Snowflake.")
    finally:
        conn.close()

if __name__ == "__main__":
    executar_pipeline()