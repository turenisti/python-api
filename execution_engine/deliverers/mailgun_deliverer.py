import requests
import time
import os
from datetime import datetime
from typing import Dict, List
from sqlalchemy.orm import Session
from shared.models import ReportDelivery, ReportDeliveryRecipient, ReportDeliveryLog, ReportConfig
from shared.utils import now_jakarta
from execution_engine.services.time_range_calculator import replace_template_variables
from dotenv import load_dotenv

load_dotenv()

# Mailgun configuration from .env
MAILGUN_API_KEY = os.getenv('MAILGUN_API_KEY')
MAILGUN_DOMAIN = os.getenv('MAILGUN_DOMAIN')
MAIL_FROM = os.getenv('MAIL_FROM', 'Finpay')
MAIL_FROM_ADDRESS = os.getenv('MAIL_FROM_ADDRESS', 'no-reply@finpay.id')

def send_email_via_mailgun(
    to_emails: List[str],
    subject: str,
    body: str,
    file_path: str,
    file_name: str
) -> Dict:
    """
    Send email via Mailgun API with attachment

    Args:
        to_emails: List of recipient email addresses
        subject: Email subject
        body: Email body (plain text)
        file_path: Path to file to attach
        file_name: Name of attachment file

    Returns:
        dict: Mailgun API response
    """

    url = f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages"

    with open(file_path, 'rb') as f:
        files = [("attachment", (file_name, f.read()))]

        data = {
            "from": f"{MAIL_FROM} <{MAIL_FROM_ADDRESS}>",
            "to": to_emails,
            "subject": subject,
            "text": body
        }

        response = requests.post(
            url,
            auth=("api", MAILGUN_API_KEY),
            data=data,
            files=files,
            timeout=60
        )

        response.raise_for_status()
        return response.json()

def deliver_via_email(
    db: Session,
    delivery: ReportDelivery,
    file_path: str,
    execution_id: str,
    config: ReportConfig,
    time_range: Dict,
    schedule_id: int = None
) -> int:
    """
    Send report via email with retry logic

    Args:
        db: Database session (shared with executor)
        delivery: ReportDelivery model with configuration
        file_path: Path to generated report file
        execution_id: UUID of execution record
        config: ReportConfig model
        time_range: Time range dict with template variables
        schedule_id: Optional schedule ID

    Returns:
        int: delivery_log_id
    """
    try:
        # Create delivery log record
        delivery_log = ReportDeliveryLog(
            config_id=config.id,
            delivery_id=delivery.id,
            schedule_id=schedule_id,
            execution_id=execution_id,
            status='pending',
            sent_at=now_jakarta()
        )
        db.add(delivery_log)
        db.flush()
        log_id = delivery_log.id

        # Get recipients
        recipients = db.query(ReportDeliveryRecipient).filter_by(
            delivery_id=delivery.id,
            is_active=True
        ).all()

        if not recipients:
            raise ValueError(f"No active recipients found for delivery {delivery.id}")

        email_addresses = [r.recipient_value for r in recipients]

        # Get email config from delivery_config JSON
        email_config = delivery.delivery_config or {}

        # Build email subject and body with template variables
        subject_template = email_config.get('subject', f'Report: {config.report_name}')
        body_template = email_config.get('body', f'Please find attached report: {config.report_name}')

        # Replace template variables using our template engine
        subject = replace_template_variables(subject_template, time_range)
        body = replace_template_variables(body_template, time_range)

        # Get file name
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)

        # Retry logic
        max_retry = delivery.max_retry or 3
        retry_interval = delivery.retry_interval_minutes or 5

        last_error = None
        send_start = now_jakarta()

        for attempt in range(1, max_retry + 1):
            try:
                # Send via Mailgun
                mailgun_response = send_email_via_mailgun(
                    to_emails=email_addresses,
                    subject=subject,
                    body=body,
                    file_path=file_path,
                    file_name=file_name
                )

                # Success - update log
                processing_time = (now_jakarta() - send_start).total_seconds() * 1000

                delivery_log.status = 'success'
                delivery_log.completed_at = now_jakarta()
                delivery_log.recipient_count = len(email_addresses)
                delivery_log.success_count = len(email_addresses)
                delivery_log.failure_count = 0
                delivery_log.retry_count = attempt - 1
                delivery_log.file_size_bytes = file_size
                delivery_log.processing_time_ms = int(processing_time)
                delivery_log.delivery_details = {
                    'mailgun_id': mailgun_response.get('id'),
                    'mailgun_message': mailgun_response.get('message'),
                    'recipients': email_addresses,
                    'subject': subject
                }

                db.flush()  # Don't commit here, let executor commit
                return log_id

            except Exception as e:
                last_error = str(e)

                if attempt < max_retry:
                    # Wait before retry (exponential backoff)
                    wait_seconds = retry_interval * 60 * attempt
                    time.sleep(wait_seconds)
                    continue
                else:
                    # Max retries reached - mark as failed
                    raise

    except Exception as e:
        # Failed - update log
        processing_time = (now_jakarta() - send_start).total_seconds() * 1000

        delivery_log.status = 'failed'
        delivery_log.completed_at = now_jakarta()
        delivery_log.recipient_count = len(email_addresses) if 'recipients' in locals() else 0
        delivery_log.success_count = 0
        delivery_log.failure_count = len(email_addresses) if 'recipients' in locals() else 0
        delivery_log.retry_count = max_retry if 'max_retry' in locals() else 3
        delivery_log.error_message = str(e)
        delivery_log.processing_time_ms = int(processing_time)

        db.flush()  # Don't commit here, let executor commit

        # Don't raise - just log the failure
        # This allows execution to complete even if delivery fails
        print(f"Email delivery failed: {e}")
        return log_id
