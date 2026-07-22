# Projeto Final Integrado: Pipeline de Dados em Nuvem para Aprendizagem de Máquina

Este repositório contém a solução desenvolvida para o projeto integrado do Módulo 2 da Pós-Graduação em Inteligência Artificial Aplicada (IFG).

O pipeline realiza a extração de dados estruturados (metadados de moda) e dados não estruturados (imagens de produtos), extrai atributos de cores das imagens em memória no Airflow e os carrega de forma automatizada em um banco de dados analítico na nuvem para futuras modelagens e análises.

---

## 🚀 Como Executar o Projeto (Guia Passo a Passo)

### 1. Requisitos Prévios
Antes de começar, certifique-se de ter instalado em sua máquina:
- **Docker** e **Docker Compose**
- **Astro CLI** (Astronomer Airflow local)
- **Floci** (Emulador local do AWS S3)
- **Python 3.12+**

---

### 2. Configuração do Ambiente Local
Crie e ative o seu ambiente virtual Python para rodar os scripts de preparação na sua máquina física:

```bash
# Criar o ambiente virtual
python -m venv .venv

# Ativar o ambiente virtual (Linux/macOS)
source .venv/bin/activate

# Instalar as dependências necessárias localmente
pip install pillow pandas numpy boto3 snowflake-connector-python[pandas] cryptography
```

---

### 3. Preparação e Amostragem dos Dados (Kaggle)
Para otimizar o armazenamento e o desempenho computacional, trabalhamos com uma amostra de **1.000 imagens** extraídas do dataset *Fashion Product Images (Small)* do Kaggle.

1. Baixe o dataset simplificado (~575 MB) no Kaggle.
2. Descompacte o arquivo `.zip` localmente.
3. Crie o diretório `airflow/include/data/dataset/` no seu projeto.
4. Execute o script de amostragem local para gerar a amostra de 1.000 produtos:
   ```bash
   python airflow/include/scripts/amostrar_dados.py
   ```
   *Este script filtra aleatoriamente 1.000 registros, gera o arquivo `styles_sample.csv` e copia apenas as 1.000 imagens correspondentes para a pasta do Astronomer.*

---

### 4. Simulação da Infraestrutura de Nuvem (Floci / AWS S3)
Utilizamos o **Floci** para simular o serviço de armazenamento AWS S3 localmente na porta **4566**.

1. Certifique-se de que o Floci está rodando na sua máquina.
2. Execute o script Python de upload para criar o bucket e subir os dados da sua máquina para o S3 simulado:
   ```bash
   python upload_floci.py
   ```
   *O script criará o bucket `dataset-project` e enviará a estrutura:*
   - `s3://dataset-project/styles_sample.csv`
   - `s3://dataset-project/images/`

---

### 5. Introdução ao Snowflake e Configuração de Segurança (Key-Pair)

#### **Qual é a função do Snowflake no projeto?**
O **Snowflake** é o nosso **Cloud Data Warehouse (DW)**, ou seja, o nosso banco de dados analítico centralizado na nuvem [6]. Sua função é armazenar de forma segura e performática todos os dados estruturados e os novos atributos que extraímos das imagens [6]. Ele organiza os dados em três camadas lógicas de maturação [6]:
1. **Bronze (RAW):** Armazena os dados brutos exatamente como vieram do S3 [6].
2. **Silver (STAGING):** Armazena os dados limpos, padronizados e tipados via dbt [6].
3. **Gold (CORE/ANALYTICS):** Armazena as tabelas finais prontas para alimentar os modelos de Machine Learning e o painel de visualização do Metabase [6, 8].

---

#### **Configurando a segurança de acesso (Snowflake Key-Pair):**
Como a conta do Snowflake exige obrigatoriamente a verificação de duas etapas (MFA com Token/TOTP), configuramos a **Autenticação por Par de Chaves (Key-Pair)** para que o Airflow (Docker) consiga se conectar ao banco de forma automatizada e sem a necessidade de digitação manual de senhas ou tokens de 30 segundos [1.1.2, 1.2.1].

1. No terminal do seu notebook, acesse a pasta `include` do projeto e gere as chaves criptográficas:
   ```bash
   cd "airflow/include"
   # Gerar chave privada descriptografada
   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
   # Gerar chave pública correspondente
   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
   ```
2. Abra o arquivo `rsa_key.pub` gerado, copie o texto interno da chave e execute o SQL abaixo na sua planilha web do Snowflake para associar a chave ao seu usuário:
   ```sql
   ALTER USER FOX SET RSA_PUBLIC_KEY='SUA_CHAVE_COPIADA_AQUI';
   ```

---

### 6. Inicialização das Camadas no Snowflake
Acesse a planilha web do seu Snowflake online e execute o script SQL abaixo para preparar a infraestrutura lógica do seu Data Warehouse e criar a tabela da camada Bronze (`RAW`) que receberá o processamento do Airflow [6]:

```sql
-- Criando o Banco e as três camadas lógicas do projeto (Bronze, Silver e Gold)
CREATE DATABASE IF NOT EXISTS FOX_DB;
CREATE SCHEMA IF NOT EXISTS FOX_DB.RAW_FASHION; -- Bronze (Dados Brutos)
CREATE SCHEMA IF NOT EXISTS FOX_DB.STAGING;     -- Silver (Dados Tratados)
CREATE SCHEMA IF NOT EXISTS FOX_DB.CORE;        -- Gold (Pronto para ML e Dashboards)

-- Criando a tabela bruta que receberá a carga do Airflow (com os atributos já extraídos das imagens)
CREATE OR REPLACE TABLE FOX_DB.RAW_FASHION.PRODUCTS_RAW (
    ID INT,
    GENDER VARCHAR,
    MASTER_CATEGORY VARCHAR,
    SUB_CATEGORY VARCHAR,
    ARTICLE_TYPE VARCHAR,
    BASE_COLOUR VARCHAR,
    SEASON VARCHAR,
    YEAR INT,
    USAGE VARCHAR,
    PRODUCT_DISPLAY_NAME VARCHAR,
    MEAN_R FLOAT,
    MEAN_G FLOAT,
    MEAN_B FLOAT
);
```

---

### 7. Orquestração e Carga Automatizada (Airflow)
A orquestração do pipeline está consolidada no arquivo **`airflow/dags/pypeline_fashion.py`**. O fluxo de trabalho é composto por duas tarefas interligadas:

1. **`check_or_create_s3_bucket`**: Uma tarefa de verificação automática (*self-healing*) que checa se o bucket `dataset-project` existe no S3 local e o recria vazio caso o Floci tenha sido reiniciado.
2. **`process_images_and_load_to_snowflake`**: Baixa o CSV estruturado, varre as imagens do S3 local na memória, processa os pixels para extrair os atributos de cores médias (R, G, B) utilizando a biblioteca *Pillow*, une os dados e faz o upload otimizado (`write_pandas`) diretamente para a nuvem do Snowflake via chave privada (`rsa_key.p8`).

Para rodar o pipeline:
1. Navegue até a pasta do Astronomer e inicie o Airflow:
   ```bash
   cd "airflow"
   astro dev start
   ```
2. Acesse a interface gráfica no navegador (`http://localhost:8080`).
3. Ative a DAG `pipeline_fashion_elt` (deixe o botão azul) e clique em **Trigger DAG** (Play) para ver a execução terminar com sucesso.

---

## 🛠️ Próximas Etapas do Projeto (A Fazer)

- [ ] **Modelagem Analítica (dbt):** Configurar o dbt conectado ao Snowflake. Desenvolver as transformações para organizar os dados em tabelas de Staging, Dimensões e Fatos, aplicando testes básicos de restrição no dbt.
- [ ] **Aprendizagem de Máquina (Python):** Criar uma tarefa de ML (como a classificação da categoria do produto `MASTER_CATEGORY` com base nos atributos de cores extraídos). O modelo deve ser comparado entre a versão desenvolvida do zero (**Hard-code**) e a versão com bibliotecas (**Scikit-learn**).
- [ ] **Visualização de Dados (Metabase):** Conectar o Metabase ao Snowflake para estruturar um dashboard de apoio à decisão, exibindo a classificação do modelo para os gestores de negócio.
- [ ] **Documentação:** Criar o arquivo de template `cloudformation.yaml` na raiz do projeto para documentar a criação do S3 na nuvem AWS real, além de estruturar o relatório final em PDF e os slides para apresentação.
```