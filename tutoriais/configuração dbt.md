# Instalação e Configuração do dbt com Snowflake

> Este documento descreve o processo de instalação e configuração do **dbt Core** para utilização com **Snowflake**, integrado ao projeto de Engenharia de Dados.

---

# Pré-requisitos

Antes de iniciar, certifique-se de possuir:

* Python 3.12 ou superior
* Ambiente virtual (`venv`)
* Projeto já configurado
* Conta no Snowflake
* Chave privada (`.p8`) para autenticação
* Pacote `dbt-snowflake`

Estrutura do projeto:

```text
projeto-final-modulo2-pos-ia-ifg/
│
├── airflow/
├── dbt/
├── floci/
├── requirements.txt
└── .venv/
```

---

# 1. Ativar o ambiente virtual

Na raiz do projeto:

```bash
source .venv/bin/activate
```

---

# 2. Instalar o dbt

Instalar o adaptador para Snowflake:

```bash
pip install dbt-snowflake
```

Verificar a instalação:

```bash
dbt --version
```

Saída esperada (exemplo):

```text
Core:
  - installed: 1.12.0
```

---

# 3. Criar o projeto dbt

Na pasta `dbt`:

```bash
cd dbt
```

Criar o projeto:

```bash
dbt init floci_dbt
```

Durante a configuração foi informado:

| Campo          | Valor              |
| -------------- | ------------------ |
| Account        | `SFEDU02-GFB24387` |
| User           | `FOX`              |
| Authentication | Key Pair           |

Durante o assistente houve um problema na criação do perfil (`profiles.yml`), embora o projeto tenha sido criado corretamente.

A estrutura gerada foi semelhante a:

```text
dbt/
└── floci_dbt/
    ├── analyses/
    ├── macros/
    ├── models/
    ├── seeds/
    ├── snapshots/
    ├── tests/
    ├── dbt_project.yml
    └── README.md
```

---

# 4. Criar o diretório de configuração

Criar a pasta do dbt:

```bash
mkdir -p ~/.dbt
```

---

# 5. Criar o arquivo profiles.yml

Criar o arquivo:

```bash
nano ~/.dbt/profiles.yml
```

Conteúdo:

```yaml
floci_dbt:
  target: dev

  outputs:
    dev:
      type: snowflake

      account: SFEDU02-GFB24387
      user: FOX

      private_key_path: "/home/leandro/Área de trabalho/POS IFG/projeto-final-modulo2-pos-ia-ifg/airflow/include/rsa_key.p8"

      role: TRAINING_ROLE
      database: FOX_DB
      warehouse: FOX_WH
      schema: RAW_FASHION

      threads: 4
      client_session_keep_alive: false
```

---

# 6. Verificar o perfil do projeto

Confirmar que o projeto utiliza o perfil correto:

```bash
grep "^profile:" dbt/floci_dbt/dbt_project.yml
```

Resultado:

```yaml
profile: 'floci_dbt'
```

---

# 7. Testar a conexão

Entrar na pasta do projeto:

```bash
cd dbt/floci_dbt
```

Executar:

```bash
dbt debug
```

Resultado obtido:

```text
profiles.yml file [OK found and valid]

dbt_project.yml file [OK found and valid]

Connection test: [OK connection ok]

All checks passed!
```

Isso confirma que:

* ✔ O `profiles.yml` foi localizado.
* ✔ O projeto dbt está configurado corretamente.
* ✔ A chave privada foi carregada.
* ✔ A autenticação com o Snowflake funcionou.
* ✔ O acesso ao banco foi realizado com sucesso.

---

# Estrutura final do projeto

```text
projeto-final-modulo2-pos-ia-ifg/
│
├── airflow/
│   ├── dags/
│   └── include/
│
├── dbt/
│   └── floci_dbt/
│       ├── analyses/
│       ├── macros/
│       ├── models/
│       ├── seeds/
│       ├── snapshots/
│       ├── tests/
│       ├── dbt_project.yml
│       └── README.md
│
├── floci/
├── requirements.txt
└── .venv/
```
