### Prerequisites

- Docker Desktop with at least **8 GB RAM** allocated
- Docker Compose 
- Git

### 1. Clone the repository(Or Pull the Repository)

```bash
git clone <repo-url>
cd click_stream_lakehouse
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` and fill in:
```env
AIRFLOW__CORE__FERNET_KEY=<generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
AIRFLOW__API_AUTH__JWT_SECRET=<same value as FERNET_KEY>
```

### 3. Build custom Docker images

```bash
docker compose build
```

> This downloads Delta Lake and Hadoop AWS JARs (~200 MB). It may take a while.

### 4. Start all services

```bash
docker compose up
```

### 5. Trigger the pipeline

**Via Airflow UI** (`http://localhost:8081`):
1. Login with `admin` / `admin`
2. Find DAGs file
3. Toggle ON → click ▶ **Trigger DAG** to trigger the one you want to execute

**Via CLI:**
```bash
docker exec airflow-scheduler airflow dags trigger <name_of_the_dag>
```

### 6. Superset

**Via Superset UI** (`http://localhost:8088`):
1. Login with `admin` / `admin`
2. Go to the top left anggle and click on Settings>Database connection
3. Top left angle click on Add Database, select trino from the drop down trino
4. Add this connection 'trino://admin@trino:8080/delta' then test connection and connect
5. to check you see the database goo to the top, click on SQL>SQL Lab
6. Select in the right field trino, you should see the database from minIO


### INFO

If the machine do not have the resurces you can stop the containers based on what you are doing, see the comment in the docker-compose.yml
