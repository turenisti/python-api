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
    """Main worker loop"""
    logger.info("=" * 80)
    logger.info("🚀 Starting Report Execution Worker")
    logger.info("=" * 80)

    try:
        # Initialize Kafka consumer
        consumer = ReportKafkaConsumer()

        # Start consuming messages
        consumer.consume(process_execution_request)

    except KeyboardInterrupt:
        logger.info("\n🛑 Worker stopped by user")
    except Exception as e:
        logger.error(f"❌ Worker failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
