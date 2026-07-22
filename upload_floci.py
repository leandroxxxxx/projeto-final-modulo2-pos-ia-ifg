import os
import boto3

# Configurado exatamente para o seu Floci local na porta 4500
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',  # Ajustado para a porta 4500
    aws_access_key_id='test',              # Credenciais genéricas necessárias para emuladores
    aws_secret_access_key='test',
    region_name='us-east-1'
)

bucket_name = "dataset-project"            # Nome do bucket
diretorio_local = "airflow/data/dataset" # Sua pasta com os dados

# 1. Tenta criar o bucket se ele ainda não existir no Floci
try:
    s3.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' verificado/criado com sucesso.")
except Exception as e:
    print(f"Aviso sobre o bucket: {e}")

# 2. Varre a pasta local e faz o upload de tudo
print("Iniciando upload de arquivos...")
arquivos_enviados = 0

if not os.path.exists(diretorio_local):
    print(f"Erro: O diretório local '{diretorio_local}' não foi encontrado!")
else:
    for raiz, pastas, arquivos in os.walk(diretorio_local):
        for arquivo in arquivos:
            caminho_completo = os.path.join(raiz, arquivo)
            # Mantém a estrutura de subpastas no S3 (ex: images/123.jpg)
            caminho_s3 = os.path.relpath(caminho_completo, diretorio_local)
            
            s3.upload_file(caminho_completo, bucket_name, caminho_s3)
            arquivos_enviados += 1
            
            if arquivos_enviados % 100 == 0:
                print(f"{arquivos_enviados} arquivos enviados...")

    print(f"Concluído! {arquivos_enviados} arquivos enviados para o Floci na porta 4500.")