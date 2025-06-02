# Pipeline de Cotação do Dólar (Airflow + Docker Compose)

Este repositório contém uma solução completa para extrair, transformar e carregar (ETL) cotações diárias do dólar comercial (PTAX) com Apache Airflow, usando Docker Compose. Há duas DAGs principais:

1. **fx_usd_live_dag.py**  
   – Roda **três vezes ao dia** (10h, 15h e 20h) e mantém um histórico contínuo de cotações desde 01/01/2021.  
   – Saída: `data/fx_usd_live.parquet`.

2. **fx_usd_monthly_snapshot_dag.py**  
   – Agendada para rodar **diariamente**, mas só gera um snapshot quando é **o primeiro dia útil** do mês, conforme o calendário em `plugins/includes/business_days.py`.  
   – Gera um arquivo Parquet com todo o mês anterior: `data/fx_usd_snapshot_YYYY_MM.parquet`.

---

## 🔧 Pré-requisitos

1. **Docker** (versão 20+)  
2. **Docker Compose** (formato `docker compose`, sem hífen)  
3. **Git** (para clonar o repositório)

---

## 🚀 Como subir o ambiente

### 1. Clone este repositório

```bash
git clone <URL_DO_REPOSITORIO>
cd <NOME_DO_REPOSITORIO>
```

### 2. Ajustes iniciais (se necessário)

- Se você quiser usar configurações personalizadas do Airflow, edite `config/airflow.cfg`.  
- Confirme que a pasta `data/` existe e que o usuário do Airflow tem permissão de escrita nela.

### 3. Build da imagem Docker

```bash
docker compose build
```

Isso criará uma imagem customizada do Airflow, instalada com as dependências listadas em `requirements.txt`.

### 4. Suba os containers

```bash
docker compose up -d
```

O Docker Compose iniciará, em modo “detached”:

- **postgres**  (ou o banco configurado; se você usar SQLite, basta omitir a seção de Postgres em `docker-compose.yaml`)  
- **airflow-webserver**  
- **airflow-scheduler**  

### 5. Inicializar o banco do Airflow (somente na primeira vez)

```bash
docker compose exec airflow-webserver airflow db init
```

### 6. Criar usuário administrador

```bash
docker compose exec airflow-webserver   airflow users create     --username admin     --firstname Admin     --lastname User     --role Admin     --email admin@example.com     --password admin
```

(Altere o usuário/senha conforme preferir.)

### 7. Acessar a UI do Airflow

Abra no navegador:
```
http://localhost:8080
```
Login padrão:  
```
Usuário: admin
Senha: admin
```

---

## ⚙️ Configuração das DAGs

### **1. fx_usd_live_dag.py** (atualizações diárias)

- **Local:** `dags/fx_usd_live_dag.py`  
- **Schedule:** `0 10,15,20 * * *` (10h, 15h e 20h todo dia)  
- **Objetivo:**  
  1. Obtém `logical_date` (data de execução).  
  2. Chama `get_quotation_data(start="01-01-2021", end=logical_date)` (em `plugins/includes/fetch_exchange_rate.py`).  
  3. Preenche datas ausentes e gera DataFrame completo.  
  4. Salva em `data/fx_usd_live.parquet`.

### **2. fx_usd_monthly_snapshot_dag.py** (snapshot mensal)

- **Local:** `dags/fx_usd_monthly_snapshot_dag.py`  
- **Schedule:** `@daily` (avaliada todo dia, mas só gera snapshot no primeiro dia útil do mês)  
- **Objetivo:**  
  1. Um **ShortCircuitOperator** (`is_first_business_day`) verifica se `logical_date` é o **primeiro dia útil** do mês (conforme `plugins/includes/business_days.py`).  
  2. Se for, a tarefa `generate_monthly_fx_snapshot`:  
     - Calcula intervalo do mês anterior (do dia 1 ao último dia).  
     - Chama `get_quotation_data(start, end)`.  
     - Salva em `data/fx_usd_snapshot_{YYYY}_{MM:02}.parquet`.  
  3. Se não for, a DAG é “curta‐circuitada” e não faz nada.

---

## 📑 Exemplo de `docker-compose.yaml`

```yaml
version: "3.7"

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  airflow-webserver:
    build:
      context: .
      dockerfile: Dockerfile
    restart: always
    depends_on:
      - postgres
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "False"
      AIRFLOW__API__AUTH_BACKENDS: airflow.api.auth.backend.basic_auth
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
    volumes:
      - ./dags:/opt/airflow/dags
      - ./plugins:/opt/airflow/plugins
      - ./data:/opt/airflow/data
      - ./logs:/opt/airflow/logs
      - ./config/airflow.cfg:/opt/airflow/airflow.cfg
    ports:
      - "8080:8080"
    command: >
      bash -c "airflow db init &&
               airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin &&
               airflow webserver"

  airflow-scheduler:
    build:
      context: .
      dockerfile: Dockerfile
    restart: always
    depends_on:
      - airflow-webserver
      - postgres
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
    volumes:
      - ./dags:/opt/airflow/dags
      - ./plugins:/opt/airflow/plugins
      - ./data:/opt/airflow/data
      - ./logs:/opt/airflow/logs
      - ./config/airflow.cfg:/opt/airflow/airflow.cfg
    command: >
      bash -c "airflow scheduler"

volumes:
  postgres_data:
```

---

## 📄 `requirements.txt`

```txt
apache-airflow==2.9.0
pandas==1.6.1
requests==2.31.0
psycopg2-binary==2.9.9
```

---

## 📋 Uso Offline (sem Docker)

Caso prefira rodar localmente sem Docker, crie um ambiente virtual:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

export AIRFLOW_HOME=$(pwd)/airflow_home
airflow db init

airflow users create   --username admin   --firstname Admin   --lastname User   --role Admin   --email admin@example.com   --password admin

mkdir -p "$AIRFLOW_HOME"/dags "$AIRFLOW_HOME"/plugins "$AIRFLOW_HOME"/logs
cp -r dags/* "$AIRFLOW_HOME"/dags/
cp -r plugins/* "$AIRFLOW_HOME"/plugins/

# Em terminais separados:
airflow webserver --port 8080
airflow scheduler
```

Após isso, acesse `http://localhost:8080` com usuário/senha `admin/admin`.

---

## 🎯 Conclusão

1. **Clone e entre na pasta**  
2. **`docker compose build`**  
3. **`docker compose up -d`**  
4. **`docker compose exec airflow-webserver airflow db init`**  
5. **`docker compose exec airflow-webserver airflow users create …`**  
6. **Acesse `http://localhost:8080`**  
7. Teste as DAGs conforme explicado  
8. Os Parquets serão salvos automaticamente em `data/`

Pronto! Com isso, seu pipeline de cotações do dólar está rodando em produção local via Airflow e Docker Compose. Qualquer dúvida, confira os logs em `logs/dag_processor/…` ou abra uma issue.

— Equipe de Engenharia 🚀
