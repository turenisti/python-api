import uuid
import os
from datetime import datetime
from typing import Optional, Dict
from sqlalchemy.orm import Session
from dotenv import load_dotenv

# Load environment variables FIRST
load_dotenv()

from shared.models import (
    ReportConfig, ReportDatasource, ReportSchedule,
    ReportDelivery, ReportExecution
)
from shared.database import get_db_session
from shared.utils import now_jakarta
from shared.logger import setup_logger, log_with_context
from execution_engine.services.time_range_calculator import calculate_time_range, replace_template_variables
from execution_engine.services.query_builder import apply_filters_to_query, build_auto_date_filter
from execution_engine.connectors.mysql_connector import execute_query
from execution_engine.services.format_converter import convert_to_format, get_file_size
from execution_engine.deliverers.mailgun_deliverer import deliver_via_email
from execution_engine.deliverers.sftp_deliverer import deliver_via_sftp

# Secure path handling - REQUIRED environment variable
_configured_path = os.getenv('REPORT_OUTPUT_PATH')
if not _configured_path:
    raise RuntimeError("REPORT_OUTPUT_PATH environment variable is required but not set")

REPORT_OUTPUT_PATH = os.path.abspath(_configured_path)

# Ensure the directory exists and is writable
if not os.path.exists(REPORT_OUTPUT_PATH):
    os.makedirs(REPORT_OUTPUT_PATH, mode=0o755, exist_ok=True)

# Setup structured logger (after load_dotenv)
logger = setup_logger('executor')

async def execute_report(
    config_id: int,
    schedule_id: Optional[int] = None,
    executed_by: str = "system",
    execution_id: Optional[str] = None
) -> Dict:
    """
    Main execution orchestrator - coordinates all steps

    Flow:
    1. Create or update execution record (status='running')
    2. Load config + datasource + deliveries
    3. Calculate time range from schedule
    4. Replace template variables in query
    5. Apply pre-configured filters from parameters
    6. Execute query on datasource
    7. Convert to CSV/Excel format
    8. Deliver via email
    9. Log delivery attempts
    10. Update execution record (status='completed')
    11. Update schedule.last_run_at

    Args:
        config_id: Report configuration ID
        schedule_id: Optional schedule ID
        executed_by: User who triggered execution
        execution_id: Optional existing execution ID (for Kafka consumer)

    Returns:
        dict: Execution details with execution_id
    """

    # Use provided execution_id or generate new one
    if not execution_id:
        execution_id = str(uuid.uuid4())

    execution_start = now_jakarta()  # Use Asia/Jakarta timezone

    with get_db_session() as db:
        try:
            # STEP 1: Create or update execution record
            execution = db.query(ReportExecution).filter_by(id=execution_id).first()

            if execution:
                # Update existing execution (from Kafka queue)
                execution.status = 'running'
                execution.started_at = execution_start
            else:
                # Create new execution
                execution = ReportExecution(
                    id=execution_id,
                    config_id=config_id,
                    schedule_id=schedule_id,
                    status='running',
                    started_at=execution_start,
                    executed_by=executed_by
                )
                db.add(execution)

            db.flush()

            # STEP 2: Load configuration
            log_with_context(logger, 'info', 'Loading configuration',
                           execution_id=execution_id, config_id=config_id, stage='config_loading')

            config = db.query(ReportConfig).filter_by(id=config_id, is_active=True).first()
            if not config:
                raise ValueError(f"Config {config_id} not found or inactive")

            datasource = db.query(ReportDatasource).filter_by(id=config.datasource_id, is_active=True).first()
            if not datasource:
                raise ValueError(f"Datasource {config.datasource_id} not found or inactive")

            deliveries = db.query(ReportDelivery).filter_by(config_id=config_id, is_active=True).all()

            log_with_context(logger, 'info', 'Configuration loaded successfully',
                           execution_id=execution_id, config_id=config_id,
                           datasource=datasource.name, deliveries=len(deliveries), stage='config_loaded')

            # STEP 3: Calculate time range
            schedule = None
            if schedule_id:
                schedule = db.query(ReportSchedule).filter_by(id=schedule_id).first()

            time_range = calculate_time_range(schedule, execution_start)

            # STEP 3b: Extract filter values to use as template variables in email
            # This allows using {{merchant_id}}, {{status}}, etc. in email subject/body
            filter_variables = {}
            if config.parameters and isinstance(config.parameters, dict):
                filters_config = config.parameters.get('filters', [])
                for filter_def in filters_config:
                    field = filter_def.get('field')
                    value = filter_def.get('value')
                    if field and value is not None:
                        # Extract field name (remove table prefix if exists)
                        # e.g., "ipg_trx_master.merchant_id" -> "merchant_id"
                        field_name = field.split('.')[-1] if '.' in field else field

                        # Convert value to string (handle lists for IN operator)
                        if isinstance(value, list):
                            filter_variables[field_name] = ', '.join(str(v) for v in value)
                        else:
                            filter_variables[field_name] = str(value)

            # Merge filter variables into time_range for template replacement
            time_range.update(filter_variables)

            # STEP 4: Build query with auto date filter and static filters
            base_query = config.report_query

            # Step 4a: Apply automatic date filter (if date_field specified)
            if config.parameters and isinstance(config.parameters, dict):
                date_field = config.parameters.get('date_field')
                if date_field:
                    # Build date filter from cron expression automatically
                    cron_expr = schedule.cron_expression if schedule else None
                    date_filter = build_auto_date_filter(date_field, time_range, cron_expr)

                    if date_filter:
                        # Check if query has WHERE clause
                        query_upper = base_query.upper()
                        has_where = 'WHERE' in query_upper

                        # Find insertion point
                        insertion_keywords = ['ORDER BY', 'LIMIT', 'GROUP BY', 'HAVING']
                        insertion_pos = len(base_query)
                        for keyword in insertion_keywords:
                            pos = query_upper.find(keyword)
                            if pos != -1 and pos < insertion_pos:
                                insertion_pos = pos

                        # Insert date filter
                        if has_where:
                            base_query = base_query[:insertion_pos].rstrip() + "\nAND " + date_filter + "\n" + base_query[insertion_pos:]
                        else:
                            base_query = base_query[:insertion_pos].rstrip() + "\nWHERE " + date_filter + "\n" + base_query[insertion_pos:]

            # Step 4b: Apply static filters from parameters (merchant_id, status, etc.)
            if config.parameters and isinstance(config.parameters, dict):
                filters_config = config.parameters.get('filters', [])
                if filters_config:
                    # Apply WHERE clause using pre-configured filter values
                    base_query = apply_filters_to_query(base_query, filters_config, time_range)

            # Step 4c: Replace any remaining template variables
            final_query = replace_template_variables(base_query, time_range)

            # Log the execution context
            execution.execution_context = {
                'original_query': config.report_query,
                'final_query': final_query,
                'time_range': time_range,
                'datasource_type': datasource.db_type,
                'output_format': config.output_format
            }
            db.flush()

            # STEP 5: Execute query
            log_with_context(logger, 'info', 'Executing database query',
                           execution_id=execution_id, config_id=config_id,
                           datasource=datasource.name, db_type=datasource.db_type,
                           query=final_query, stage='query_executing')

            query_start = now_jakarta()

            if datasource.db_type == 'mysql':
                df = execute_query(datasource, final_query, config.timeout_seconds or 300)
            else:
                raise ValueError(f"Unsupported datasource type: {datasource.db_type}")

            query_time_ms = int((now_jakarta() - query_start).total_seconds() * 1000)
            rows_returned = len(df)

            log_with_context(logger, 'info', 'Query executed successfully',
                           execution_id=execution_id, config_id=config_id,
                           rows=rows_returned, duration_ms=query_time_ms, stage='query_completed')

            # STEP 6: Convert to format
            log_with_context(logger, 'info', 'Generating report file',
                           execution_id=execution_id, config_id=config_id,
                           format=config.output_format, stage='file_generating')

            # Create output directory with execution_id
            output_dir = os.path.join(REPORT_OUTPUT_PATH, execution_id)
            os.makedirs(output_dir, exist_ok=True)

            # Generate filename with timestamp
            file_extension = 'xlsx' if config.output_format == 'xlsx' else 'csv'

            # Check for custom filename template in parameters
            filename_template = None
            if config.parameters and isinstance(config.parameters, dict):
                filename_template = config.parameters.get('filename_template')

            if filename_template:
                # Use custom template
                file_name_base = replace_template_variables(filename_template, time_range)
                # Sanitize filename
                file_name_base = file_name_base.replace(' ', '_').replace('/', '_').replace(':', '-')
                file_name = f"{file_name_base}.{file_extension}"
            else:
                # Default format: ReportName_YYYYMMDD_HHMMSS.ext
                timestamp_str = execution_start.strftime('%Y%m%d_%H%M%S')
                safe_report_name = config.report_name.replace(' ', '_').replace('/', '_')
                file_name = f"{safe_report_name}_{timestamp_str}.{file_extension}"

            output_path = os.path.join(output_dir, file_name)

            # Get display columns from parameters (if specified)
            display_columns = None
            if config.parameters and isinstance(config.parameters, dict):
                display_columns = config.parameters.get('display_columns')

            # Convert to format with column filtering
            convert_to_format(df, config.output_format, output_path, display_columns)
            file_size = get_file_size(output_path)

            log_with_context(logger, 'info', 'Report file generated successfully',
                           execution_id=execution_id, config_id=config_id,
                           file_path=output_path, file_size_bytes=file_size, stage='file_generated')

            # Update execution with query results
            execution.query_execution_time_ms = query_time_ms
            execution.rows_returned = rows_returned
            execution.file_generated_path = output_path
            execution.file_size_bytes = file_size
            db.flush()

            # STEP 7: Deliver to recipients
            log_with_context(logger, 'info', 'Starting delivery to recipients',
                           execution_id=execution_id, config_id=config_id,
                           deliveries=len(deliveries), stage='delivery_starting')

            delivery_count = 0
            for delivery in deliveries:
                if delivery.method == 'email':
                    deliver_via_email(
                        db=db,  # Pass the shared database session
                        delivery=delivery,
                        file_path=output_path,
                        execution_id=execution_id,
                        config=config,
                        time_range=time_range,
                        schedule_id=schedule_id
                    )
                    delivery_count += 1
                elif delivery.method == 'sftp':
                    deliver_via_sftp(
                        db=db,  # Pass the shared database session
                        delivery=delivery,
                        file_path=output_path,
                        execution_id=execution_id,
                        config=config,
                        time_range=time_range,
                        schedule_id=schedule_id
                    )
                    delivery_count += 1
                else:
                    logger.warning(f"Unsupported delivery method: {delivery.method}")

            log_with_context(logger, 'info', 'Delivery completed',
                           execution_id=execution_id, config_id=config_id,
                           deliveries_sent=delivery_count, stage='delivery_completed')

            # STEP 8: Update execution record
            execution.status = 'completed'
            execution.completed_at = now_jakarta()
            db.flush()

            # STEP 9: Update schedule last_run_at
            if schedule:
                schedule.last_run_at = execution_start
                db.flush()

            db.commit()

            # Return execution details
            total_time_ms = int((now_jakarta() - execution_start).total_seconds() * 1000)

            log_with_context(logger, 'info', 'Report execution completed successfully',
                           execution_id=execution_id, config_id=config_id,
                           rows=rows_returned, duration_ms=total_time_ms,
                           deliveries_sent=delivery_count, stage='completed')

            return {
                'execution_id': execution_id,
                'config_id': config_id,
                'config_name': config.report_name,
                'schedule_id': schedule_id,
                'status': 'completed',
                'rows_returned': rows_returned,
                'file_path': output_path,
                'file_size_bytes': file_size,
                'deliveries_sent': delivery_count,
                'query_execution_time_ms': query_time_ms,
                'total_execution_time_ms': total_time_ms,
                'time_range': time_range
            }

        except Exception as e:
            # Update execution as failed (if execution record exists)
            try:
                if 'execution' in locals():
                    execution.status = 'failed'
                    execution.completed_at = now_jakarta()
                    execution.error_message = str(e)
                    db.commit()
            except:
                pass  # Ignore errors during error handling

            raise Exception(f"Report execution failed: {str(e)}")
