import os
import json
import ssl
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Use structured logger
from shared.logger import setup_logger
logger = setup_logger('kafka_consumer')


class ReportKafkaConsumer:
    """Kafka consumer for report execution requests"""

    def __init__(self):
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '').split(',')
        topic = os.getenv('KAFKA_TOPIC_EXECUTION_REQUESTS', 'report-scheduler-execution-request')
        group_id = os.getenv('KAFKA_CONSUMER_GROUP', 'report-workers')
        security_protocol = os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL').upper()

        logger.info(f"Initializing Kafka consumer for topic: {topic}")
        logger.info(f"Bootstrap servers: {bootstrap_servers}")
        logger.info(f"Consumer group: {group_id}")
        logger.info(f"Security protocol: {security_protocol}")

        try:
            # Common consumer configuration
            consumer_config = {
                'bootstrap_servers': bootstrap_servers,
                'group_id': group_id,
                'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
                'auto_offset_reset': 'earliest',
                'enable_auto_commit': False,  # Manual commit for reliability
                # Performance & reliability settings
                'max_poll_records': 10,  # Process max 10 messages per poll
                'max_poll_interval_ms': 900000,  # 15 minutes - max time between polls
                'session_timeout_ms': 120000,  # 2 minutes - heartbeat timeout (increased for long processing)
                'heartbeat_interval_ms': 30000,  # 30 seconds - send heartbeat every 30s
                'fetch_min_bytes': 1,  # Don't wait for minimum bytes
                'fetch_max_wait_ms': 500,  # Max wait 500ms for new messages
            }

            # Configure security based on protocol
            if security_protocol == 'SASL_SSL':
                # SASL_SSL configuration for Aiven Kafka
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

                consumer_config.update({
                    'security_protocol': 'SASL_SSL',
                    'sasl_mechanism': 'SCRAM-SHA-256',
                    'sasl_plain_username': os.getenv('KAFKA_SASL_USERNAME'),
                    'sasl_plain_password': os.getenv('KAFKA_SASL_PASSWORD'),
                    'ssl_context': ssl_context,
                })
                logger.info("🔒 Kafka consumer configured with SASL_SSL security")

            elif security_protocol == 'PLAINTEXT':
                # PLAINTEXT configuration for local Kafka
                consumer_config.update({
                    'security_protocol': 'PLAINTEXT',
                })
                logger.info("🔓 Kafka consumer configured with PLAINTEXT security")

            else:
                raise ValueError(f"Unsupported Kafka security protocol: {security_protocol}")

            self.consumer = KafkaConsumer(topic, **consumer_config)
            logger.info("✅ Kafka consumer initialized successfully")

        except KafkaError as e:
            logger.error(f"❌ Failed to initialize Kafka consumer: {e}")
            raise

    def consume(self, message_handler):
        """
        Consume messages from Kafka and process them using the provided handler

        Args:
            message_handler: Async function that processes the message
        """
        logger.info("🚀 Starting to consume messages from Kafka...")
        logger.info("⏳ Waiting for messages... (Press Ctrl+C to stop)")

        message_count = 0

        try:
            for message in self.consumer:
                try:
                    message_count += 1
                    data = message.value
                    execution_id = data.get('execution_id', 'unknown')

                    logger.info(f"📨 [{message_count}] Received message - Execution ID: {execution_id}")
                    logger.debug(f"Full message: {data}")

                    # Call the message handler
                    message_handler(data)

                    # Commit offset on success
                    self.consumer.commit()
                    logger.info(f"✅ [{message_count}] Successfully processed and committed - Execution ID: {execution_id}")

                except Exception as e:
                    logger.error(f"❌ [{message_count}] Error processing message: {e}", exc_info=True)

                    # ALWAYS commit offset to skip message and prevent infinite retries
                    # Failed executions are already logged in report_executions table
                    # We don't want consumer to crash or retry indefinitely
                    logger.warning(f"⚠️ Committing offset to skip failed message and continue processing queue")
                    try:
                        self.consumer.commit()
                        logger.info(f"✅ [{message_count}] Offset committed despite error")
                    except Exception as commit_error:
                        logger.error(f"❌ Failed to commit offset: {commit_error}")
                        # If commit fails, consumer might be disconnected - will restart

        except KeyboardInterrupt:
            logger.info("🛑 Consumer interrupted by user")
        except Exception as e:
            logger.error(f"❌ Consumer error: {e}", exc_info=True)
        finally:
            logger.info(f"📊 Total messages processed: {message_count}")
            self.close()

    def close(self):
        """Close the Kafka consumer"""
        if self.consumer:
            logger.info("Closing Kafka consumer...")
            self.consumer.close()
            logger.info("✅ Kafka consumer closed")
