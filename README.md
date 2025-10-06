# Scheduling Report System - Python Execution Engine

> Report execution engine and scheduler service for processing and delivering reports (Phases 7-8)

## 🎯 Purpose

This Python API serves as the **Execution Engine & Scheduler Layer** of the Scheduling Report System. It provides:

- **Phase 7:** Report execution engine (query execution, format conversion, file generation)
- **Phase 8:** Scheduler service (cron-based scheduling, job management)

The Python service reads configurations from the Go API and writes execution results back to the shared MySQL database.

## 🚀 Quick Start

### Prerequisites

- Python 3.11 or higher
- MySQL 8.0+ (shared with Go API)
- Access to target data sources (MySQL, PostgreSQL, BigQuery, etc.)
- Access to delivery targets (SMTP, SFTP, S3, etc.)

### Installation

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy environment configuration
cp .env.example .env

# Edit .env with your credentials
nano .env
```

### Configuration

Edit `.env`:

```env
# Database Configuration (shared with Go API)
DB_HOST=192.168.131.209
DB_PORT=3306
DB_USER=user_arif
DB_PASSWORD=your_password
DB_NAME=lab

# Execution Engine Configuration
EXECUTION_API_PORT=8000
MAX_CONCURRENT_EXECUTIONS=10
QUERY_TIMEOUT_SECONDS=300

# Scheduler Configuration
SCHEDULER_API_PORT=8001
SCHEDULER_TIMEZONE=UTC
SCHEDULER_CHECK_INTERVAL_SECONDS=60
```

### Running the Services

```bash
# Run Execution Engine (Phase 7)
python -m execution_engine.main

# Run Scheduler Service (Phase 8) - in separate terminal
python -m scheduler.main

# Or run both with process manager
python run_all.py
```

- Execution Engine API: `http://localhost:8000`
- Scheduler Service API: `http://localhost:8001`

## 📋 Architecture Overview

### Phase 7: Execution Engine

**Purpose:** Execute reports on-demand or via scheduler

**Key Components:**
- **FastAPI Application:** REST API for triggering executions
- **Query Executor:** Connects to data sources, runs SQL queries
- **Format Converter:** Converts results to CSV, Excel, JSON, PDF
- **File Generator:** Creates output files with metadata
- **Delivery Handler:** Sends files via configured delivery methods
- **Log Writer:** Records execution and delivery attempts

**API Endpoints:**
- `POST /api/execute/:config_id` - Execute report immediately
- `GET /api/execution/:execution_id` - Get execution status
- `POST /api/test-datasource/:datasource_id` - Test data source connection
- `POST /api/test-delivery/:delivery_id` - Test delivery configuration

### Phase 8: Scheduler Service

**Purpose:** Automatically execute reports based on schedules

**Key Components:**
- **APScheduler:** Cron-based job scheduler
- **Schedule Loader:** Reads active schedules from database
- **Job Manager:** Creates/updates/removes scheduled jobs
- **Execution Trigger:** Calls Execution Engine API
- **Health Monitor:** Tracks scheduler health and job status

**API Endpoints:**
- `GET /api/scheduler/status` - Get scheduler status
- `POST /api/scheduler/reload` - Reload schedules from database
- `GET /api/scheduler/jobs` - List active scheduled jobs
- `POST /api/scheduler/trigger/:schedule_id` - Manually trigger schedule

## 🏗️ Project Structure

```
python-api/
├── requirements.txt            # Python dependencies
├── .env                        # Environment configuration
├── run_all.py                  # Run both services
│
├── execution_engine/           # Phase 7: Execution Engine
│   ├── __init__.py
│   ├── main.py                 # FastAPI application entry
│   ├── api/
│   │   ├── routes.py           # API endpoints
│   │   └── schemas.py          # Pydantic models
│   ├── core/
│   │   ├── database.py         # Database connection
│   │   ├── config.py           # Configuration loader
│   │   └── exceptions.py       # Custom exceptions
│   ├── services/
│   │   ├── executor.py         # Main execution orchestrator
│   │   ├── query_runner.py     # SQL query execution
│   │   ├── format_converter.py # File format conversion
│   │   └── delivery_handler.py # Delivery methods
│   ├── connectors/             # Data source connectors
│   │   ├── mysql.py
│   │   ├── postgresql.py
│   │   ├── oracle.py
│   │   ├── sqlserver.py
│   │   ├── mongodb.py
│   │   ├── bigquery.py
│   │   └── snowflake.py
│   ├── deliverers/             # Delivery method handlers
│   │   ├── email.py
│   │   ├── sftp.py
│   │   ├── webhook.py
│   │   ├── s3.py
│   │   └── file_share.py
│   └── utils/
│       ├── logger.py           # Logging configuration
│       └── response.py         # Standard response format
│
├── scheduler/                  # Phase 8: Scheduler Service
│   ├── __init__.py
│   ├── main.py                 # Scheduler application entry
│   ├── api/
│   │   ├── routes.py           # Scheduler API endpoints
│   │   └── schemas.py          # Pydantic models
│   ├── core/
│   │   ├── database.py         # Database connection
│   │   ├── config.py           # Configuration loader
│   │   └── scheduler.py        # APScheduler setup
│   ├── services/
│   │   ├── schedule_loader.py  # Load schedules from DB
│   │   ├── job_manager.py      # Manage scheduled jobs
│   │   └── execution_trigger.py # Trigger executions
│   └── utils/
│       ├── logger.py           # Logging configuration
│       └── health.py           # Health monitoring
│
├── shared/                     # Shared utilities
│   ├── models.py               # SQLAlchemy models (8 tables)
│   ├── repositories.py         # Data access layer
│   └── constants.py            # Shared constants
│
└── tests/                      # Test suite
    ├── test_execution_engine.py
    ├── test_scheduler.py
    ├── test_connectors.py
    └── test_deliverers.py
```

## 🔧 Key Technologies

- **Framework:** FastAPI (REST API)
- **Scheduler:** APScheduler (cron-based scheduling)
- **ORM:** SQLAlchemy (database abstraction)
- **Database:** MySQL 8.0+ (shared with Go API)
- **Data Processing:** pandas (data manipulation)
- **File Formats:** openpyxl (Excel), reportlab (PDF)
- **Connectors:** pymysql, psycopg2, pymongo, google-cloud-bigquery, snowflake-connector-python
- **Deliverers:** smtplib (email), paramiko (SFTP), boto3 (S3), requests (webhook)

## 💾 Database Interaction

The Python service interacts with the shared MySQL database:

### Read Operations
- **report_datasources** - Data source connection info
- **report_configs** - Report query and format configuration
- **report_schedules** - Active schedules to process
- **report_deliveries** - Delivery method configurations
- **report_delivery_recipients** - Recipient lists

### Write Operations
- **report_executions** - Execution records (UUID-based)
- **report_delivery_logs** - Delivery attempt logs
- **report_schedules.last_run_at** - Update after execution

## 🔄 Execution Flow

### On-Demand Execution (Phase 7)

1. **API Request:** `POST /api/execute/:config_id`
2. **Load Config:** Read from `report_configs`, `report_datasources`, `report_deliveries`
3. **Create Execution Record:** Insert into `report_executions` (status: RUNNING)
4. **Connect to Datasource:** Use appropriate connector (MySQL, PostgreSQL, etc.)
5. **Execute Query:** Run SQL query with parameters
6. **Convert Format:** Convert results to CSV/Excel/JSON/PDF
7. **Generate File:** Create file with timestamp
8. **Deliver File:** Send via configured methods (email, SFTP, S3, webhook)
9. **Log Delivery:** Write to `report_delivery_logs`
10. **Update Execution:** Set status to COMPLETED/FAILED

### Scheduled Execution (Phase 8)

1. **Scheduler Startup:** Load all active schedules from `report_schedules`
2. **Create Jobs:** Create APScheduler jobs with cron expressions
3. **Job Trigger:** When cron time matches, trigger execution
4. **Call Execution API:** `POST /execution_engine/api/execute/:config_id`
5. **Update Last Run:** Set `report_schedules.last_run_at`
6. **Monitor Health:** Track job status and failures

## 📝 Response Format

All API responses follow this standard format:

### Success Response
```json
{
  "status": "success",
  "message": "Report execution started",
  "data": {
    "execution_id": "550e8400-e29b-41d4-a716-446655440001",
    "config_id": 1,
    "status": "RUNNING"
  }
}
```

### Error Response
```json
{
  "status": "error",
  "message": "Failed to connect to datasource",
  "error": "Connection refused: MySQL server not reachable"
}
```

## 🔍 Key Features

### Multi-Format Support
Reports can be generated in multiple formats:

```python
# Supports: CSV, Excel, JSON, PDF
converter = FormatConverter(results)
file_path = converter.convert(format="excel", output_path="/tmp/report.xlsx")
```

### Parameterized Queries
Queries support dynamic parameters:

```sql
-- Query with parameters
SELECT * FROM sales WHERE date >= :start_date AND date <= :end_date

-- Parameters from config
{
  "start_date": "2025-01-01",
  "end_date": "2025-01-31"
}
```

### Multi-Delivery Support
Single execution can deliver to multiple recipients:

```python
# Email to recipients list
for recipient in recipients:
    email_deliverer.send(file_path, recipient.email)

# Also upload to S3
s3_deliverer.upload(file_path, bucket, key)
```

### Automatic Retry Logic
Failed deliveries retry with exponential backoff:

```python
max_retry = delivery_config.max_retry  # From report_deliveries
retry_interval = delivery_config.retry_interval_minutes

for attempt in range(1, max_retry + 1):
    if deliver_success:
        break
    time.sleep(retry_interval * 60 * attempt)
```

### Execution Isolation
Each execution runs in isolated context:

```python
# Each execution gets unique ID and workspace
execution_id = str(uuid.uuid4())
workspace = f"/tmp/executions/{execution_id}/"
```

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=execution_engine --cov=scheduler

# Run specific test file
pytest tests/test_execution_engine.py

# Run integration tests (requires database)
pytest tests/integration/
```

## 🚢 Deployment

### Development Mode

```bash
# Execution Engine with auto-reload
uvicorn execution_engine.main:app --reload --port 8000

# Scheduler with auto-reload
uvicorn scheduler.main:app --reload --port 8001
```

### Production Mode

```bash
# Execution Engine
gunicorn execution_engine.main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000

# Scheduler
gunicorn scheduler.main:app -w 2 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8001
```

### Docker Deployment

```bash
# Build Docker images
docker build -t scheduling-report-executor -f Dockerfile.executor .
docker build -t scheduling-report-scheduler -f Dockerfile.scheduler .

# Run containers
docker run -p 8000:8000 --env-file .env scheduling-report-executor
docker run -p 8001:8001 --env-file .env scheduling-report-scheduler
```

## 📚 Documentation

- **Implementation Guide:** `/docs/PYTHON_PHASES_7_8.md` (comprehensive guide)
- **Database Schema:** `/docs/REPORT-HUB.md`
- **API Specification:** `/docs/api-docs.yaml`
- **Project Status:** `/docs/PROJECT_STATUS.md`

## 🔗 Integration with Go API

This Python service works alongside the Go Configuration API:

- **Go API** (`/golang-api/`): Configuration management (Phases 1-6)
- **Python API** (`/python-api/`): Report execution and scheduling (Phases 7-8)
- **Shared Database:** Both services access the same MySQL database
- **Communication:** Python reads configs created by Go API

## 🐛 Common Issues

### Database Connection Fails
```bash
# Verify shared database is accessible
mysql -h192.168.131.209 -uuser_arif -p lab

# Check .env credentials match Go API
```

### Data Source Connection Fails
```bash
# Test data source connection
curl -X POST http://localhost:8000/api/test-datasource/1

# Check connection_config in report_datasources
```

### Scheduler Not Picking Up New Schedules
```bash
# Reload schedules from database
curl -X POST http://localhost:8001/api/scheduler/reload

# Check scheduler logs
tail -f logs/scheduler.log
```

### Delivery Fails
```bash
# Test delivery configuration
curl -X POST http://localhost:8000/api/test-delivery/1

# Check delivery_config in report_deliveries
# Verify SMTP/SFTP/S3 credentials
```

## 📊 Implementation Status

| Phase | Component | Status |
|-------|-----------|--------|
| Phase 7 | Execution Engine API | 🔴 Not Started |
| Phase 7 | Query Executors (7 connectors) | 🔴 Not Started |
| Phase 7 | Format Converters (4 formats) | 🔴 Not Started |
| Phase 7 | Delivery Handlers (5 methods) | 🔴 Not Started |
| Phase 8 | Scheduler Service API | 🔴 Not Started |
| Phase 8 | APScheduler Integration | 🔴 Not Started |
| Phase 8 | Schedule Loader | 🔴 Not Started |
| Phase 8 | Job Manager | 🔴 Not Started |

**Next Steps:** Follow `/docs/PYTHON_PHASES_7_8.md` for implementation guide.

## 📄 License

This project is part of the Scheduling Report System.

## 🤝 Related Services

- **Go Configuration API:** `/golang-api/` (Phases 1-6)
- **Documentation:** `/docs/`

---

**Status:** Not Started 🔴 | **Ready:** Implementation guide available in `/docs/PYTHON_PHASES_7_8.md`
