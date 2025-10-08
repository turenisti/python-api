import pymysql
import pandas as pd
from urllib.parse import urlparse
from typing import Dict
from sqlalchemy import create_engine, text
from shared.models import ReportDatasource

def parse_connection_url(url: str) -> Dict[str, str]:
    """
    Parse MySQL connection URL
    Format: mysql://user:password@host:port/database
    """
    parsed = urlparse(url)

    return {
        'host': parsed.hostname or 'localhost',
        'port': parsed.port or 3306,
        'user': parsed.username or 'root',
        'password': parsed.password or '',
        'database': parsed.path.lstrip('/') if parsed.path else ''
    }

def execute_query(datasource: ReportDatasource, query: str, timeout: int = 300) -> pd.DataFrame:
    """
    Execute SQL query on MySQL database

    Args:
        datasource: ReportDatasource model with connection info
        query: SQL query to execute
        timeout: Query timeout in seconds

    Returns:
        pandas.DataFrame with query results
    """

    # Parse connection URL
    conn_info = parse_connection_url(datasource.connection_url)

    # Build SQLAlchemy connection string for pandas (fixes the warning)
    connection_string = (
        f"mysql+pymysql://{conn_info['user']}:{conn_info['password']}"
        f"@{conn_info['host']}:{conn_info['port']}/{conn_info['database']}"
        f"?charset=utf8mb4&connect_timeout={timeout}"
    )

    # Create SQLAlchemy engine
    engine = create_engine(connection_string, pool_pre_ping=True)

    try:
        # Execute query and load into DataFrame using connection from engine
        # Wrap query in text() for SQLAlchemy 2.0 compatibility
        with engine.connect() as connection:
            df = pd.read_sql(text(query), connection)
        return df
    finally:
        engine.dispose()
