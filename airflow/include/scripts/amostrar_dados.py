import os
import shutil
import pandas as pd

# 1. Defina os caminhos das pastas originais (ajuste se necessário)
pasta_origem = "/home/leandro/Área de trabalho/dataset/"  # pasta onde descompactou o zip
csv_origem = os.path.join(pasta_origem, "styles.csv")
imagens_origem = os.path.join(pasta_origem, "images")

# 2. Defina onde quer salvar a amostra limpa
pasta_destino = ".airflow/data/dataset"
imagens_destino = os.path.join(pasta_destino, "images")

# Criar as novas pastas caso não existam
os.makedirs(imagens_destino, exist_ok=True)

# 3. Ler o CSV original e selecionar 1000 produtos aleatórios
# (O parâmetro on_bad_lines serve para ignorar possíveis linhas corrompidas no arquivo)
df = pd.read_csv(csv_origem, on_bad_lines='skip')
df_amostra = df.sample(n=1000, random_state=42)  # random_state garante reprodutibilidade

# Salvar o novo CSV de amostra na pasta de destino
df_amostra.to_csv(os.path.join(pasta_destino, "styles_sample.csv"), index=False)

# 4. Copiar apenas as imagens que pertencem à amostra selecionada
copiadas = 0
for idx, row in df_amostra.iterrows():
    # O id do produto é o nome da imagem (ex: 15970 -> 15970.jpg)
    nome_imagem = f"{int(row['id'])}.jpg"
    caminho_src = os.path.join(imagens_origem, nome_imagem)
    caminho_dest = os.path.join(imagens_destino, nome_imagem)
    
    if os.path.exists(caminho_src):
        shutil.copy(caminho_src, caminho_dest)
        copiadas += 1

print(f"Processo concluído com sucesso!")
print(f"Total de registros salvos no CSV: {len(df_amostra)}")
print(f"Total de imagens copiadas: {copiadas}")