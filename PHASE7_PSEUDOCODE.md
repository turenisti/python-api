# Phase 7: Execution Engine - Pseudocode Structure

## Directory Structure
```
python-api/
├── requirements.txt
├── .env
├── shared/
│   ├── __init__.py
│   ├── models.py          # SQLAlchemy models for all 8 tables
│   └── database.py        # Database connection
│
└── execution_engine/
    ├── __init__.py
    ├── main.py             # FastAPI application entry point
    │
    ├── api/
    │   ├── __init__.py
    │   ├── routes.py       # API endpoints
    │   └── schemas.py      # Pydantic request/response models
    │
    ├── core/
    │   ├── __init__.py
    │   ├── config.py       # Configuration loader from .env
    │   └── exceptions.py   # Custom exceptions
    │
    ├── services/
    │   ├── __init__.py
    │   ├── executor.py              # Main execution orchestrator
    │   ├── time_range_calculator.py # Calculate time ranges from cron
    │   ├── query_runner.py          # Execute SQL queries
    │   ├── format_converter.py      # CSV/Excel conversion
    │   └── delivery_handler.py      # Email delivery
    │
    ├── connectors/
    │   ├── __init__.py
    │   └── mysql_connector.py  # MySQL database connector (MVP)
    │
    ├── deliverers/
    │   ├── __init__.py
    │   └── email_deliverer.py  # Email delivery (MVP)
    │
    └── utils/
        ├── __init__.py
        ├── logger.py         # Logging configuration
        └── response.py       # Standard response format
```

---

## 1. Requirements (requirements.txt)

```python
# requirements.txt
fastapi==0.109.0
uvicorn[standard]==0.27.0
sqlalchemy==2.0.25
pymysql==1.1.0
pandas==2.2.0
openpyxl==3.1.2
python-dotenv==1.0.0
pydantic==2.5.0
croniter==2.0.1
```

---

## 2. Environment Configuration (.env)

```bash
# .env
# Database (shared with Go API)
DB_HOST=10.218.0.6
DB_PORT=3306
DB_USER=user_arif
DB_PASSWORD=Masarif_2092
DB_NAME=finpaycde

# Execution Engine
EXECUTION_API_PORT=8000
MAX_CONCURRENT_EXECUTIONS=10
QUERY_TIMEOUT_SECONDS=300

# Email Configuration
SMTP_HOST=smtp.company.com
SMTP_PORT=587
SMTP_USER=noreply@company.com
SMTP_PASSWORD=secret
SMTP_USE_TLS=true

# File Storage
REPORT_OUTPUT_PATH=/tmp/reports
```

---

## 3. Database Models (shared/models.py)

```python
# shared/models.py - PSEUDOCODE

from sqlalchemy import Column, Integer, String, Text, JSON, TIMESTAMP, Enum, Boolean, BigInteger, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class ReportDatasource(Base):
    """
    Maps to: report_datasources table
    Stores database connection info
    """
    __tablename__ = 'report_datasources'

    # FIELDS:
    # - id: INT AUTO_INCREMENT
    # - name: VARCHAR(100)
    # - connection_url: TEXT
    # - db_type: ENUM('mysql', 'postgresql', ...)
    # - connection_config: JSON
    # - is_active: BOOLEAN
    # - created_at, updated_at, created_by, updated_by

class ReportConfig(Base):
    """
    Maps to: report_configs table
    Stores report query and configuration
    """
    __tablename__ = 'report_configs'

    # FIELDS:
    # - id: INT AUTO_INCREMENT
    # - report_name: VARCHAR(200)
    # - report_query: TEXT (with {{template_variables}})
    # - output_format: ENUM('csv', 'xlsx', 'json', 'pdf')
    # - datasource_id: FK → report_datasources.id
    # - parameters: JSON
    # - timeout_seconds: INT
    # - max_rows: INT
    # - version: INT

class ReportSchedule(Base):
    """
    Maps to: report_schedules table
    Stores cron scheduling info
    """
    __tablename__ = 'report_schedules'

    # FIELDS:
    # - id: INT AUTO_INCREMENT
    # - config_id: FK → report_configs.id
    # - cron_expression: VARCHAR(100)
    # - timezone: VARCHAR(50)
    # - is_active: BOOLEAN
    # - last_run_at: TIMESTAMP (used for time range calculation!)

class ReportDelivery(Base):
    """
    Maps to: report_deliveries table
    Stores delivery method configuration
    """
    __tablename__ = 'report_deliveries'

    # FIELDS:
    # - id: INT AUTO_INCREMENT
    # - config_id: FK → report_configs.id
    # - delivery_name: VARCHAR(200)
    # - method: ENUM('email', 'sftp', 'webhook', 's3', 'file_share')
    # - delivery_config: JSON (email settings, SMTP config, etc.)
    # - max_retry: INT (default 3)
    # - retry_interval_minutes: INT (default 5)
    # - is_active: BOOLEAN

class ReportDeliveryRecipient(Base):
    """
    Maps to: report_delivery_recipients table
    Stores recipient email addresses
    """
    __tablename__ = 'report_delivery_recipients'

    # FIELDS:
    # - id: INT AUTO_INCREMENT
    # - delivery_id: FK → report_deliveries.id
    # - recipient_type: VARCHAR(20) ('email' for MVP)
    # - recipient_value: VARCHAR(500) (email address)
    # - is_active: BOOLEAN

class ReportExecution(Base):
    """
    Maps to: report_executions table
    Logs execution attempts
    """
    __tablename__ = 'report_executions'

    # FIELDS:
    # - id: VARCHAR(36) UUID PRIMARY KEY
    # - config_id: FK → report_configs.id
    # - schedule_id: FK → report_schedules.id (NULL for manual)
    # - status: ENUM('running', 'completed', 'failed', 'cancelled')
    # - started_at: TIMESTAMP
    # - completed_at: TIMESTAMP
    # - executed_by: VARCHAR(100)
    # - execution_context: JSON (stores time_range, variables, etc.)
    # - query_execution_time_ms: INT
    # - rows_returned: INT
    # - file_generated_path: TEXT
    # - file_size_bytes: BIGINT
    # - error_message: TEXT

class ReportDeliveryLog(Base):
    """
    Maps to: report_delivery_logs table
    Logs delivery attempts
    """
    __tablename__ = 'report_delivery_logs'

    # FIELDS:
    # - id: BIGINT AUTO_INCREMENT
    # - config_id: FK → report_configs.id
    # - delivery_id: FK → report_deliveries.id
    # - schedule_id: FK → report_schedules.id
    # - execution_id: FK → report_executions.id (UUID)
    # - status: ENUM('pending', 'success', 'failed', 'retry')
    # - sent_at: TIMESTAMP
    # - completed_at: TIMESTAMP
    # - recipient_count: INT
    # - success_count: INT
    # - failure_count: INT
    # - retry_count: INT
    # - error_message: TEXT
    # - delivery_details: JSON
    # - file_size_bytes: BIGINT
    # - processing_time_ms: INT
```

---

## 4. Main Execution Flow (execution_engine/services/executor.py)

```python
# execution_engine/services/executor.py - PSEUDOCODE

import uuid
from datetime import datetime
from typing import Dict, Optional

async def execute_report(
    config_id: int,
    schedule_id: Optional[int] = None,
    executed_by: str = "manual"
) -> str:
    """
    Main execution orchestrator - coordinates all steps

    FLOW:
    1. Create execution record (status='running')
    2. Load config + datasource + deliveries
    3. Calculate time range from schedule
    4. Replace template variables in query
    5. Execute query on datasource
    6. Convert to CSV/Excel format
    7. Deliver via email
    8. Log delivery attempts
    9. Update execution record (status='completed')
    10. Update schedule.last_run_at

    RETURNS: execution_id (UUID)
    """

    execution_id = str(uuid.uuid4())
    execution_start = datetime.now()

    try:
        # STEP 1: Create execution record
        execution = create_execution_record(
            execution_id=execution_id,
            config_id=config_id,
            schedule_id=schedule_id,
            status='running',
            started_at=execution_start,
            executed_by=executed_by
        )

        # STEP 2: Load configuration
        config = db.query(ReportConfig).filter_by(id=config_id).first()
        if not config:
            raise ValueError(f"Config {config_id} not found")

        datasource = db.query(ReportDatasource).filter_by(id=config.datasource_id).first()
        deliveries = db.query(ReportDelivery).filter_by(config_id=config_id, is_active=True).all()

        # STEP 3: Calculate time range
        schedule = None
        if schedule_id:
            schedule = db.query(ReportSchedule).filter_by(id=schedule_id).first()

        time_range = calculate_time_range(schedule, execution_start)

        # STEP 4: Replace template variables in query
        final_query = replace_template_variables(config.report_query, time_range)

        # Log the actual query executed
        update_execution_context(execution_id, {
            'original_query': config.report_query,
            'final_query': final_query,
            'time_range': time_range,
            'variables_used': time_range
        })

        # STEP 5: Execute query
        query_start = datetime.now()
        df = execute_query_on_datasource(datasource, final_query, config.timeout_seconds)
        query_time_ms = (datetime.now() - query_start).total_seconds() * 1000

        # STEP 6: Convert to format
        output_path = f"/tmp/reports/{execution_id}/{config.report_name}_{execution_start.strftime('%Y%m%d_%H%M%S')}.{config.output_format}"

        if config.output_format == 'csv':
            df.to_csv(output_path, index=False)
        elif config.output_format == 'xlsx':
            df.to_excel(output_path, index=False, engine='openpyxl')

        file_size = os.path.getsize(output_path)

        # STEP 7: Deliver to recipients
        for delivery in deliveries:
            deliver_report(
                delivery=delivery,
                file_path=output_path,
                execution_id=execution_id,
                config=config,
                time_range=time_range
            )

        # STEP 8: Update execution record
        update_execution_record(execution_id, {
            'status': 'completed',
            'completed_at': datetime.now(),
            'query_execution_time_ms': query_time_ms,
            'rows_returned': len(df),
            'file_generated_path': output_path,
            'file_size_bytes': file_size
        })

        # STEP 9: Update schedule last_run_at
        if schedule:
            schedule.last_run_at = execution_start
            db.commit()

        return execution_id

    except Exception as e:
        # Update execution as failed
        update_execution_record(execution_id, {
            'status': 'failed',
            'completed_at': datetime.now(),
            'error_message': str(e)
        })
        raise
```

---

## 5. Time Range Calculator (execution_engine/services/time_range_calculator.py)

```python
# execution_engine/services/time_range_calculator.py - PSEUDOCODE

from croniter import croniter
from datetime import datetime, timedelta

def calculate_time_range(schedule: Optional[ReportSchedule], execution_time: datetime) -> Dict[str, str]:
    """
    Calculate time range for query based on schedule

    LOGIC:
    1. If schedule.last_run_at exists → use it as start_time
    2. Else, calculate from cron expression (get previous scheduled time)
    3. End time is always execution_time

    RETURNS: {
        'start_datetime': '2025-10-06 12:00:00',
        'end_datetime': '2025-10-06 18:00:00',
        'start_date': '2025-10-06',
        'end_date': '2025-10-06',
        'interval_hours': 6.0,
        'calculation_method': 'last_run_at' | 'cron_detection' | 'default_daily',
        'yesterday': '2025-10-05',
        'execution_time': '2025-10-06 18:00:00'
    }
    """

    if schedule and schedule.last_run_at:
        # Use last run for accurate range
        start = schedule.last_run_at
        end = execution_time
        method = 'last_run_at'

    elif schedule and schedule.cron_expression:
        # Calculate from cron
        cron = croniter(schedule.cron_expression, execution_time)
        start = cron.get_prev(datetime)
        end = execution_time
        method = 'cron_detection'

    else:
        # Default to daily (yesterday)
        start = execution_time - timedelta(days=1)
        end = execution_time
        method = 'default_daily'

    interval_hours = (end - start).total_seconds() / 3600

    return {
        'start_datetime': start.strftime('%Y-%m-%d %H:%M:%S'),
        'end_datetime': end.strftime('%Y-%m-%d %H:%M:%S'),
        'start_date': start.strftime('%Y-%m-%d'),
        'end_date': end.strftime('%Y-%m-%d'),
        'interval_hours': round(interval_hours, 2),
        'calculation_method': method,
        'yesterday': (end - timedelta(days=1)).strftime('%Y-%m-%d'),
        'last_week': (end - timedelta(days=7)).strftime('%Y-%m-%d'),
        'execution_time': end.strftime('%Y-%m-%d %H:%M:%S')
    }
```

---

## 6. MySQL Connector (execution_engine/connectors/mysql_connector.py)

```python
# execution_engine/connectors/mysql_connector.py - PSEUDOCODE

import pymysql
import pandas as pd

def execute_query(datasource: ReportDatasource, query: str, timeout: int) -> pd.DataFrame:
    """
    Execute SQL query on MySQL database

    STEPS:
    1. Parse connection_url (mysql://user:pass@host:port/database)
    2. Merge with connection_config JSON (pool_size, read_timeout, etc.)
    3. Connect to MySQL
    4. Execute query with timeout
    5. Fetch results into pandas DataFrame
    6. Close connection

    RETURNS: pandas.DataFrame with query results
    """

    # Parse connection URL
    parsed = parse_connection_url(datasource.connection_url)

    # Create connection
    connection = pymysql.connect(
        host=parsed['host'],
        port=parsed['port'],
        user=parsed['user'],
        password=parsed['password'],
        database=parsed['database'],
        connect_timeout=timeout,
        read_timeout=timeout,
        **datasource.connection_config  # Additional configs
    )

    # Execute query
    df = pd.read_sql(query, connection)

    connection.close()

    return df
```

---

## 7. Email Deliverer (execution_engine/deliverers/email_deliverer.py)

```python
# execution_engine/deliverers/email_deliverer.py - PSEUDOCODE

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import time

def deliver_via_email(
    delivery: ReportDelivery,
    file_path: str,
    execution_id: str,
    config: ReportConfig,
    time_range: Dict
) -> None:
    """
    Send report via email with retry logic

    STEPS:
    1. Get recipients from report_delivery_recipients
    2. Parse delivery_config JSON for SMTP settings
    3. Create email with attachment
    4. Send to all recipients
    5. Retry on failure (max 3 attempts with exponential backoff)
    6. Log each delivery attempt

    RETURNS: None (logs to report_delivery_logs)
    """

    # Create delivery log record
    log_id = create_delivery_log(
        execution_id=execution_id,
        delivery_id=delivery.id,
        status='pending'
    )

    try:
        # Get recipients
        recipients = db.query(ReportDeliveryRecipient).filter_by(
            delivery_id=delivery.id,
            is_active=True
        ).all()

        email_addresses = [r.recipient_value for r in recipients]

        # Get SMTP config from delivery_config JSON
        smtp_config = delivery.delivery_config

        # Retry logic
        max_retry = delivery.max_retry
        retry_interval = delivery.retry_interval_minutes

        for attempt in range(1, max_retry + 1):
            try:
                # Connect to SMTP
                server = smtplib.SMTP(smtp_config['smtp_server'], smtp_config['smtp_port'])
                server.starttls()
                server.login(smtp_config['username'], smtp_config['password'])

                # Create email
                msg = MIMEMultipart()
                msg['From'] = smtp_config['from_email']
                msg['To'] = ', '.join(email_addresses)
                msg['Subject'] = smtp_config['subject_template'].format(**time_range)

                # Body
                body_text = smtp_config['body_template'].format(**time_range)
                msg.attach(MIMEText(body_text, 'plain'))

                # Attachment
                with open(file_path, 'rb') as f:
                    attachment = MIMEApplication(f.read())
                    attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_path))
                    msg.attach(attachment)

                # Send
                server.send_message(msg)
                server.quit()

                # Success - update log
                update_delivery_log(log_id, {
                    'status': 'success',
                    'completed_at': datetime.now(),
                    'recipient_count': len(email_addresses),
                    'success_count': len(email_addresses),
                    'retry_count': attempt - 1
                })

                return  # Exit on success

            except Exception as e:
                if attempt < max_retry:
                    # Wait before retry (exponential backoff)
                    time.sleep(retry_interval * 60 * attempt)
                    continue
                else:
                    # Max retries reached
                    raise

    except Exception as e:
        # Failed - update log
        update_delivery_log(log_id, {
            'status': 'failed',
            'completed_at': datetime.now(),
            'recipient_count': len(email_addresses),
            'success_count': 0,
            'failure_count': len(email_addresses),
            'retry_count': max_retry,
            'error_message': str(e)
        })
        raise
```

---

## 8. FastAPI Routes (execution_engine/api/routes.py)

```python
# execution_engine/api/routes.py - PSEUDOCODE

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api", tags=["execution"])

class ExecuteRequest(BaseModel):
    config_id: int
    schedule_id: Optional[int] = None
    executed_by: str = "manual"

@router.post("/execute/{config_id}")
async def execute_report_endpoint(config_id: int, request: ExecuteRequest):
    """
    POST /api/execute/5

    Trigger report execution manually or from scheduler

    RETURNS: {
        "status": "success",
        "message": "Report execution started",
        "data": {
            "execution_id": "uuid",
            "config_id": 5,
            "status": "running"
        }
    }
    """
    try:
        execution_id = await execute_report(
            config_id=config_id,
            schedule_id=request.schedule_id,
            executed_by=request.executed_by
        )

        return {
            "status": "success",
            "message": "Report execution completed",
            "data": {
                "execution_id": execution_id,
                "config_id": config_id
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/execution/{execution_id}")
async def get_execution_status(execution_id: str):
    """
    GET /api/execution/{execution_id}

    Get execution status and details

    RETURNS: execution record from report_executions table
    """
    execution = db.query(ReportExecution).filter_by(id=execution_id).first()
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")

    return {
        "status": "success",
        "data": execution
    }
```

---

## 9. Main FastAPI App (execution_engine/main.py)

```python
# execution_engine/main.py - PSEUDOCODE

from fastapi import FastAPI
from execution_engine.api import routes

app = FastAPI(title="Scheduling Report - Execution Engine", version="1.0.0")

# Include routes
app.include_router(routes.router)

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "execution-engine"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

---

## 📋 Implementation Checklist

- [ ] Create virtual environment and install requirements
- [ ] Set up .env file with database credentials
- [ ] Create SQLAlchemy models for 8 tables
- [ ] Implement MySQL connector
- [ ] Implement time range calculator
- [ ] Implement template variable replacement
- [ ] Implement CSV/Excel format converter
- [ ] Implement email deliverer with retry
- [ ] Create main executor orchestrator
- [ ] Create FastAPI routes
- [ ] Test with sample report configs
- [ ] Integration test with Go API

---

## 🚀 Next Steps After Phase 7

Once Phase 7 is complete, Phase 8 (Scheduler) will:
1. Use APScheduler to monitor `report_schedules` table
2. Create jobs from active schedules
3. Call Phase 7 API `POST /api/execute/{config_id}` when due
4. Update `last_run_at` after successful execution

---

**This pseudocode provides the complete structure. Next session: implement each file with actual Python code!**
