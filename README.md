# Projeto Final Integrado: Pipeline de Dados em Nuvem para Aprendizagem de Máquina

Este repositório contém a solução desenvolvida para o projeto integrado do Módulo 2 da Pós-Graduação em Inteligência Artificial Aplicada (IFG).

---

## 🚀 Como Executar o Projeto (Guia Passo a Passo)

### 1. Requisitos Prévios
Antes de começar, certifique-se de ter instalado em sua máquina:
- **Docker** e **Docker Compose**
- **Astro CLI** (Astronomer)
- **Floci** (Emulador local da AWS)
- **Python 3.12+**

---

### 2. Configuração do Ambiente Local
Crie e ative o seu ambiente virtual Python para rodar os scripts de preparação:

```bash
# Criar o ambiente virtual
python -m venv .venv

# Ativar o ambiente virtual (Linux/macOS)
source .venv/bin/activate

# Instalar as dependências necessárias
pip install -r requirements.txt
```

---

### 3. Preparação e Amostragem dos Dados (Kaggle)
Para evitar problemas de armazenamento e desempenho, utilizamos uma amostra de **1.000 imagens** do dataset *Fashion Product Images (Small)* do Kaggle.

1. Baixe o dataset simplificado (~575 MB) no Kaggle.
2. Descompacte o arquivo `.zip` e salve os dados originais temporariamente na sua máquina.
3. Crie a pasta no projeto: `include/data/dataset/`
4. Execute o script de amostragem local (caso queira gerar uma nova amostra):
   ```bash
   python include/scripts/amostrar_dados.py
   ```
   *Este script filtrará 1.000 registros aleatórios e suas respectivas imagens, salvando-os em `include/data/dataset/`.*

---

### 4. Simulação da Infraestrutura de Nuvem (Floci / AWS S3)
Utilizamos o **Floci** para simular o serviço de armazenamento S3 da AWS localmente na porta **4566**.

1. Certifique-se de que o Floci está rodando em sua máquina.
2. Para enviar a amostra local de 1.000 itens de forma automatizada para o S3 simulado, execute o script de upload:
   ```bash
   python upload_floci.py
   ```
3. O script criará o bucket `dataset-project` e enviará a estrutura:
   - `s3://dataset-project/styles_sample.csv` (Metadados tabulares)
   - `s3://dataset-project/images/` (Pasta contendo as 1.000 imagens .jpg)

---

## 🛠️ Próximas Etapas do Pipeline (A Fazer)

- [ ] **Orquestração (Airflow):** Criar a DAG em `dags/pipeline_fashion.py` para automatizar a leitura do S3 local, extração de atributos visuais e carga no Snowflake.
- [ ] **Modelagem Analítica (dbt + Snowflake):** Configurar o dbt para ler a tabela bruta do Snowflake e gerar as tabelas de staging, dimensões e fatos.
- [ ] **Aprendizagem de Máquina (Python):** Criar o modelo preditivo (KNN ou equivalente) em duas versões: implementando do zero (hard-code) e usando bibliotecas (scikit-learn).
- [ ] **Visualização de Dados (Metabase):** Conectar o painel ao Snowflake para exibir os dados e as predições do modelo para apoio à decisão.
```
