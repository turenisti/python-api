from sqlalchemy import Column, Integer, String, Text, JSON, TIMESTAMP, Enum, Boolean, BigInteger, ForeignKey, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class ReportDatasource(Base):
    """Maps to report_datasources table"""
    __tablename__ = 'report_datasources'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    connection_url = Column(Text, nullable=False)
    db_type = Column(Enum('mysql', 'postgresql', 'oracle', 'sqlserver', 'mongodb', 'bigquery', 'snowflake'), nullable=False)
    connection_config = Column(JSON, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by = Column(String(100))
    updated_by = Column(String(100))

class ReportConfig(Base):
    """Maps to report_configs table"""
    __tablename__ = 'report_configs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_name = Column(String(200), nullable=False)
    report_query = Column(Text, nullable=False)
    output_format = Column(Enum('csv', 'xlsx', 'json', 'pdf'), nullable=False)
    datasource_id = Column(Integer, ForeignKey('report_datasources.id'), nullable=False)
    parameters = Column(JSON, nullable=True)
    timeout_seconds = Column(Integer, default=300)
    max_rows = Column(Integer, default=100000)
    version = Column(Integer, default=1)
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by = Column(String(100))
    updated_by = Column(String(100))

    # Relationships
    datasource = relationship("ReportDatasource", backref="configs")

class ReportSchedule(Base):
    """Maps to report_schedules table"""
    __tablename__ = 'report_schedules'

    id = Column(Integer, primary_key=True, autoincrement=True)
    config_id = Column(Integer, ForeignKey('report_configs.id'), nullable=False)
    cron_expression = Column(String(100), nullable=False)
    timezone = Column(String(50), default='UTC')
    is_active = Column(Boolean, default=True)
    last_run_at = Column(TIMESTAMP, nullable=True)
    next_run_at = Column(TIMESTAMP, nullable=True)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by = Column(String(100))
    updated_by = Column(String(100))

    # Relationships
    config = relationship("ReportConfig", backref="schedules")

class ReportDelivery(Base):
    """Maps to report_deliveries table"""
    __tablename__ = 'report_deliveries'

    id = Column(Integer, primary_key=True, autoincrement=True)
    config_id = Column(Integer, ForeignKey('report_configs.id'), nullable=False)
    delivery_name = Column(String(200), nullable=False)
    method = Column(Enum('email', 'sftp', 'webhook', 's3', 'file_share'), nullable=False)
    delivery_config = Column(JSON, nullable=False)
    max_retry = Column(Integer, default=3)
    retry_interval_minutes = Column(Integer, default=5)
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by = Column(String(100))
    updated_by = Column(String(100))

    # Relationships
    config = relationship("ReportConfig", backref="deliveries")

class ReportDeliveryRecipient(Base):
    """Maps to report_delivery_recipients table"""
    __tablename__ = 'report_delivery_recipients'

    id = Column(Integer, primary_key=True, autoincrement=True)
    delivery_id = Column(Integer, ForeignKey('report_deliveries.id'), nullable=False)
    recipient_type = Column(String(20), default='email')
    recipient_value = Column(String(500), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    delivery = relationship("ReportDelivery", backref="recipients")

class ReportExecution(Base):
    """Maps to report_executions table"""
    __tablename__ = 'report_executions'

    id = Column(String(36), primary_key=True)  # UUID
    config_id = Column(Integer, ForeignKey('report_configs.id'), nullable=False)
    schedule_id = Column(Integer, ForeignKey('report_schedules.id'), nullable=True)
    status = Column(Enum('queued', 'running', 'completed', 'failed', 'cancelled'), default='running')
    started_at = Column(TIMESTAMP, default=datetime.utcnow)
    completed_at = Column(TIMESTAMP, nullable=True)
    executed_by = Column(String(100), default='system')
    execution_context = Column(JSON, nullable=True)
    query_execution_time_ms = Column(Integer, nullable=True)
    rows_returned = Column(Integer, nullable=True)
    file_generated_path = Column(Text, nullable=True)
    file_size_bytes = Column(BigInteger, nullable=True)
    error_message = Column(Text, nullable=True)

    # Relationships
    config = relationship("ReportConfig", backref="executions")
    schedule = relationship("ReportSchedule", backref="executions")

class ReportDeliveryLog(Base):
    """Maps to report_delivery_logs table"""
    __tablename__ = 'report_delivery_logs'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    config_id = Column(Integer, ForeignKey('report_configs.id'), nullable=False)
    delivery_id = Column(Integer, ForeignKey('report_deliveries.id'), nullable=False)
    schedule_id = Column(Integer, ForeignKey('report_schedules.id'), nullable=True)
    execution_id = Column(String(36), ForeignKey('report_executions.id'), nullable=False)
    status = Column(Enum('pending', 'success', 'failed', 'retry'), default='pending')
    sent_at = Column(TIMESTAMP, default=datetime.utcnow)
    completed_at = Column(TIMESTAMP, nullable=True)
    recipient_count = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    failure_count = Column(Integer, default=0)
    retry_count = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    delivery_details = Column(JSON, nullable=True)
    file_size_bytes = Column(BigInteger, nullable=True)
    processing_time_ms = Column(Integer, nullable=True)

    # Relationships
    config = relationship("ReportConfig", backref="delivery_logs")
    delivery = relationship("ReportDelivery", backref="logs")
    schedule = relationship("ReportSchedule", backref="delivery_logs")
    execution = relationship("ReportExecution", backref="delivery_logs")
