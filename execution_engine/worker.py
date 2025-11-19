"""
Kafka Consumer Worker for Report Execution

This worker consumes execution requests from Kafka and processes them using
the existing report execution engine.

Usage:
    python3 -m execution_engine.worker
"""

import asyncio
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables FIRST (before any imports that use logger)
load_dotenv()

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from execution_engine.kafka_consumer import ReportKafkaConsumer
from execution_engine.services.executor import execute_report
from shared.logger import setup_logger, log_with_context

# Setup structured logger (after load_dotenv)
logger = setup_logger('worker')


def process_execution_request(message_data: dict):
    """
    Process a report execution request from Kafka

    Args:
        message_data: Dictionary containing execution request details
            {
                "execution_id": "uuid",
                "config_id": 7,
                "schedule_id": null,
                "executed_by": "arif",
                "queued_at": "2025-10-08T14:00:00+07:00"
            }
    """
    execution_id = message_data.get('execution_id')
    config_id = message_data.get('config_id')
    schedule_id = message_data.get('schedule_id')
    executed_by = message_data.get('executed_by', 'system')

    log_with_context(logger, 'info', 'Processing execution request',
                     execution_id=execution_id, config_id=config_id,
                     schedule_id=schedule_id, executed_by=executed_by)

    # IDEMPOTENCY CHECK: Prevent duplicate processing
    from shared.database import get_db_session
    from shared.models import ReportExecution

    with get_db_session() as db:
        existing = db.query(ReportExecution).filter_by(id=execution_id).first()
        if existing and existing.status == 'completed':
            logger.warning(f"⚠️ Execution {execution_id} already completed - skipping duplicate")
            return {'status': 'skipped', 'reason': 'already_completed', 'execution_id': execution_id}

    try:
        # Execute report asynchronously
        result = asyncio.run(execute_report(
            config_id=config_id,
            schedule_id=schedule_id,
            executed_by=executed_by,
            execution_id=execution_id  # Pass execution_id to reuse the existing record
        ))

        duration_ms = result.get('total_execution_time_ms', 0)
        log_with_context(logger, 'info', '✅ Execution completed successfully',
                        execution_id=execution_id, config_id=config_id,
                        status=result.get('status'), rows=result.get('rows_returned'),
                        duration_ms=duration_ms, deliveries=result.get('deliveries_sent'))

        return result

    except Exception as e:
        logger.error(f"❌ Failed to execute report {execution_id}: {e}", exc_info=True)
        raise


def main():
    """Main worker loop with auto-restart on crash"""
    import time

    max_retries = 5
    retry_count = 0
    retry_delay = 5  # seconds

    while retry_count < max_retries:
        try:
            logger.info("=" * 80)
            logger.info(f"🚀 Starting Report Execution Worker (attempt {retry_count + 1}/{max_retries})")
            logger.info("=" * 80)

            # Initialize Kafka consumer
            consumer = ReportKafkaConsumer()

            # Start consuming messages
            consumer.consume(process_execution_request)

            # If we get here normally (KeyboardInterrupt), exit cleanly
            break

        except KeyboardInterrupt:
            logger.info("\n🛑 Worker stopped by user")
            break

        except Exception as e:
            retry_count += 1
            logger.error(f"❌ Worker crashed: {e}", exc_info=True)

            if retry_count < max_retries:
                logger.warning(f"⚠️ Restarting worker in {retry_delay}s... (attempt {retry_count + 1}/{max_retries})")
                time.sleep(retry_delay)
                # Increase retry delay exponentially
                retry_delay = min(retry_delay * 2, 60)  # Max 60s delay
            else:
                logger.error(f"❌ Max retries ({max_retries}) reached. Worker stopping.")
                sys.exit(1)


if __name__ == "__main__":
    main()
