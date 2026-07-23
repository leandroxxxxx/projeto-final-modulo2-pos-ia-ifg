# Projeto Final Integrado: Pipeline de Dados em Nuvem para Machine Learning

Este repositório reúne a implementação do Projeto Final Integrado desenvolvido para o Módulo 2 da Pós-Graduação em Inteligência Artificial Aplicada do IFG.

O objetivo do projeto é construir um pipeline de dados em nuvem capaz de integrar informações estruturadas (metadados de produtos de moda) e dados não estruturados (imagens). Durante o processamento, as imagens são analisadas diretamente em memória pelo Airflow para extrair atributos relacionados às cores predominantes. Em seguida, esses dados são consolidados e carregados automaticamente para um Data Warehouse na nuvem, onde ficam preparados para análises e futuras aplicações de Machine Learning.

---

# Como executar o projeto

## 1. Pré-requisitos

Antes de iniciar, verifique se os seguintes softwares estão instalados na sua máquina:

* Docker e Docker Compose;
* Astro CLI (Astronomer);
* Floci (emulador local do Amazon S3);
* Python 3.12 ou superior.

---

## 2. Configuração do ambiente

Crie um ambiente virtual Python para executar os scripts de preparação dos dados.

```bash
# Criar ambiente virtual
python -m venv .venv

# Ativar o ambiente (Linux/macOS)
source .venv/bin/activate

# Instalar dependências
pip install pillow pandas numpy boto3 snowflake-connector-python[pandas] cryptography
```

---

## 3. Preparação dos dados

Para reduzir o consumo de armazenamento e tornar o processamento mais eficiente, foi utilizada uma amostra de **1.000 imagens** do conjunto de dados *Fashion Product Images (Small)*, disponível no Kaggle.

Após baixar e descompactar o dataset:

1. Crie a pasta `airflow/include/data/dataset/`;
2. Execute o script de amostragem:

```bash
python airflow/include/scripts/amostrar_dados.py
```

Esse script seleciona aleatoriamente 1.000 produtos, gera o arquivo `styles_sample.csv` e copia apenas as imagens correspondentes para o diretório utilizado pelo Airflow.

---

## 4. Simulação do Amazon S3

Durante o desenvolvimento, o armazenamento em nuvem foi simulado utilizando o **Floci**, que disponibiliza uma implementação local compatível com o Amazon S3.

Certifique-se de que o serviço esteja em execução e execute:

```bash
python upload_floci.py
```

O script cria automaticamente o bucket `dataset-project` (caso ainda não exista) e envia os arquivos para a seguinte estrutura:

* `styles_sample.csv`
* `images/`

---

## 5. Configuração do Snowflake

### Papel do Snowflake no projeto

O Snowflake é utilizado como Data Warehouse em nuvem, responsável por armazenar todos os dados processados pelo pipeline.

A estrutura foi organizada seguindo uma arquitetura em três camadas:

* **Bronze (RAW):** armazenamento dos dados exatamente como foram recebidos;
* **Silver (STAGING):** dados tratados, padronizados e enriquecidos;
* **Gold (CORE):** tabelas analíticas prontas para alimentar modelos de Machine Learning e dashboards.

---

### Configuração da autenticação por chave

Como a conta do Snowflake utiliza autenticação multifator (MFA), foi adotada a autenticação por par de chaves (Key-Pair Authentication). Dessa forma, o Airflow consegue acessar o banco automaticamente, sem necessidade de inserir senhas ou códigos temporários durante a execução do pipeline.

Na pasta `airflow/include`, execute:

```bash
cd airflow/include

# Gerar chave privada
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Gerar chave pública
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Depois, copie o conteúdo do arquivo `rsa_key.pub` e associe-o ao seu usuário no Snowflake executando:

```sql
ALTER USER FOX SET RSA_PUBLIC_KEY='SUA_CHAVE_PUBLICA';
```

---

## 6. Preparação do Data Warehouse

Antes da execução do pipeline, é necessário criar a estrutura inicial do banco de dados.

Execute o script SQL abaixo na interface do Snowflake:

```sql
CREATE DATABASE IF NOT EXISTS FOX_DB;

CREATE SCHEMA IF NOT EXISTS FOX_DB.RAW_FASHION;
CREATE SCHEMA IF NOT EXISTS FOX_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS FOX_DB.CORE;

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

Essa tabela será utilizada como camada Bronze, recebendo diretamente os dados produzidos pelo Airflow.

---

## 7. Execução do pipeline

Toda a orquestração está concentrada na DAG `pipeline_fashion_elt`, localizada em:

```
airflow/dags/pypeline_fashion.py
```

O fluxo possui duas etapas principais.

A primeira verifica automaticamente a existência do bucket no S3 local. Caso o Floci tenha sido reiniciado e o bucket não exista mais, ele é recriado automaticamente.

Na sequência, o pipeline realiza todo o processamento dos dados: baixa o arquivo CSV, percorre as imagens armazenadas no S3, extrai os valores médios dos canais RGB utilizando a biblioteca Pillow, integra essas informações aos metadados e envia o resultado diretamente para o Snowflake utilizando a função `write_pandas`.

Para executar o pipeline:

```bash
cd airflow
astro dev start
```

Depois:

1. Acesse `http://localhost:8080`;
2. Ative a DAG `pipeline_fashion_elt`;
3. Clique em **Trigger DAG** para iniciar a execução.

Ao final, todos os dados processados estarão disponíveis na camada Bronze do Snowflake.

---

# Próximas etapas

Ainda estão previstas algumas melhorias para completar o projeto.

* **Modelagem analítica com dbt:** criar as camadas Silver e Gold, desenvolver dimensões e fatos, além de implementar testes de qualidade dos dados.

* **Machine Learning:** desenvolver um modelo capaz de classificar a categoria (`MASTER_CATEGORY`) dos produtos utilizando os atributos de cor extraídos das imagens. A proposta inclui comparar uma implementação desenvolvida manualmente com outra baseada na biblioteca Scikit-learn.

* **Visualização de dados:** conectar o Metabase ao Snowflake para construir dashboards que permitam acompanhar os resultados do processamento e das previsões do modelo.

* **Documentação:** elaborar o arquivo `cloudformation.yaml` para representar a infraestrutura em uma AWS real, além de produzir o relatório técnico e os slides da apresentação final.
