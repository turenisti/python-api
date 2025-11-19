import paramiko
import os
import time
from datetime import datetime
from typing import Dict
from sqlalchemy.orm import Session
from shared.models import ReportDelivery, ReportDeliveryLog, ReportConfig
from shared.utils import now_jakarta
from execution_engine.services.time_range_calculator import replace_template_variables


def build_remote_filename(
    filename_pattern: str,
    config: ReportConfig,
    time_range: Dict,
    execution_id: str,
    local_file_path: str
) -> str:
    """
    Build remote filename from pattern using template variables

    Filename pattern comes from config.parameters.filename_template
    (same field used for local file naming)

    Supported variables:
    - {report_name}: Config report name
    - {date}: Current date (YYYY-MM-DD)
    - {time}: Current time (HHMMSS)
    - {datetime}: Current datetime (YYYYMMDD_HHMMSS)
    - {execution_id}: Execution UUID
    - {ext}: File extension
    - {date_from}, {date_to}: From time_range

    Args:
        filename_pattern: Pattern like "{report_name}_{date}.{ext}"
        config: ReportConfig model
        time_range: Time range dictionary
        execution_id: Execution UUID
        local_file_path: Local file to get extension

    Returns:
        str: Final filename
    """
    now = datetime.now()

    # Get file extension from local file
    _, ext = os.path.splitext(local_file_path)
    ext = ext.lstrip('.')  # Remove leading dot

    # Build template variables
    template_vars = {
        'report_name': config.report_name.replace(' ', '_'),
        'date': now.strftime('%Y-%m-%d'),
        'time': now.strftime('%H%M%S'),
        'datetime': now.strftime('%Y%m%d_%H%M%S'),
        'execution_id': execution_id,
        'ext': ext,
        **time_range  # Include date_from, date_to, etc.
    }

    # Replace variables in pattern
    filename = replace_template_variables(filename_pattern, template_vars)

    # If filename doesn't have extension and pattern doesn't contain {ext}, append it
    if not filename.endswith(f'.{ext}') and '{ext}' not in filename_pattern:
        filename = f"{filename}.{ext}"

    return filename


def upload_file_via_sftp(
    host: str,
    port: int,
    username: str,
    password: str,
    remote_path: str,
    local_file_path: str,
    filename: str,
    create_directory: bool = False,
    timeout: int = 30
) -> Dict:
    """
    Upload file to SFTP server using paramiko

    Args:
        host: SFTP server hostname/IP
        port: SFTP port (usually 22)
        username: SFTP username
        password: SFTP password
        remote_path: Remote directory path
        local_file_path: Local file to upload
        filename: Remote filename
        create_directory: Create directory if doesn't exist
        timeout: Connection timeout in seconds

    Returns:
        dict: Upload result with details

    Raises:
        paramiko.AuthenticationException: Authentication failed
        paramiko.SSHException: SSH connection failed
        IOError: File upload/permission error
        socket.timeout: Connection timeout
    """
    upload_start = time.time()

    # Initialize SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # Auto-accept host keys

    try:
        # Connect to SFTP server
        ssh.connect(
            hostname=host,
            port=port,
            username=username,
            password=password,
            timeout=timeout,
            allow_agent=False,  # Don't use SSH agent
            look_for_keys=False  # Don't look for SSH keys, use password
        )

        # Open SFTP session
        sftp = ssh.open_sftp()

        try:
            # Build full remote path
            remote_path = remote_path.rstrip('/')
            full_remote_path = f"{remote_path}/{filename}"

            # Check if remote directory exists
            try:
                sftp.stat(remote_path)
            except FileNotFoundError:
                if create_directory:
                    # Create directory recursively
                    _create_remote_directory(sftp, remote_path)
                else:
                    raise IOError(f"Remote directory does not exist: {remote_path}")

            # Upload file
            sftp.put(local_file_path, full_remote_path)

            # Verify upload by getting file stats
            remote_stat = sftp.stat(full_remote_path)
            local_size = os.path.getsize(local_file_path)

            if remote_stat.st_size != local_size:
                raise IOError(f"Upload verification failed: size mismatch (local={local_size}, remote={remote_stat.st_size})")

            upload_time = (time.time() - upload_start) * 1000  # Convert to ms

            return {
                'success': True,
                'remote_path': full_remote_path,
                'file_size': remote_stat.st_size,
                'upload_time_ms': int(upload_time),
                'host': host,
                'port': port
            }

        finally:
            sftp.close()

    finally:
        ssh.close()


def _create_remote_directory(sftp, path):
    """
    Recursively create remote directory

    Args:
        sftp: Paramiko SFTP client
        path: Directory path to create
    """
    dirs = []
    current = path

    # Find which directories need to be created
    while current and current != '/':
        try:
            sftp.stat(current)
            break  # Directory exists
        except FileNotFoundError:
            dirs.append(current)
            current = os.path.dirname(current)

    # Create directories from parent to child
    for dir_path in reversed(dirs):
        sftp.mkdir(dir_path)


def deliver_via_sftp(
    db: Session,
    delivery: ReportDelivery,
    file_path: str,
    execution_id: str,
    config: ReportConfig,
    time_range: Dict,
    schedule_id: int = None
) -> int:
    """
    Deliver report via SFTP with retry logic

    Args:
        db: Database session (shared with executor)
        delivery: ReportDelivery model with SFTP configuration
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

        # Parse SFTP config from delivery_config JSON
        sftp_config = delivery.delivery_config or {}

        host = sftp_config.get('host')
        port = sftp_config.get('port', 22)
        username = sftp_config.get('username')
        password = sftp_config.get('password')
        remote_path = sftp_config.get('remote_path', '/')
        create_directory = sftp_config.get('create_directory', False)
        timeout = sftp_config.get('timeout', 30)

        # Get filename template from config parameters (not delivery_config)
        # This is where the filename is configured in the UI
        config_params = config.parameters or {}
        filename_pattern = config_params.get('filename_template', '{report_name}.{ext}')

        # Validate required fields
        if not host:
            raise ValueError("SFTP host is required in delivery_config")
        if not username:
            raise ValueError("SFTP username is required in delivery_config")
        if not password:
            raise ValueError("SFTP password is required in delivery_config")

        # Build remote filename
        remote_filename = build_remote_filename(
            filename_pattern=filename_pattern,
            config=config,
            time_range=time_range,
            execution_id=execution_id,
            local_file_path=file_path
        )

        # Get file size
        file_size = os.path.getsize(file_path)

        # Retry logic - shorter intervals for SFTP (seconds not minutes)
        max_retry = delivery.max_retry or 3
        retry_interval = delivery.retry_interval_minutes or 2  # Now treated as seconds

        # Overall timeout to prevent hanging (10 seconds max)
        overall_timeout = 10  # seconds
        last_error = None
        send_start = now_jakarta()

        for attempt in range(1, max_retry + 1):
            # Check overall timeout
            elapsed = (now_jakarta() - send_start).total_seconds()
            if elapsed >= overall_timeout:
                raise TimeoutError(f"SFTP delivery timeout after {elapsed:.1f}s (max {overall_timeout}s)")

            try:
                # Upload via SFTP with short timeout (10s)
                upload_result = upload_file_via_sftp(
                    host=host,
                    port=port,
                    username=username,
                    password=password,
                    remote_path=remote_path,
                    local_file_path=file_path,
                    filename=remote_filename,
                    create_directory=create_directory,
                    timeout=10  # 10 second connection timeout
                )

                # Success - update log
                processing_time = (now_jakarta() - send_start).total_seconds() * 1000

                delivery_log.status = 'success'
                delivery_log.completed_at = now_jakarta()
                delivery_log.recipient_count = 1  # SFTP = 1 destination
                delivery_log.success_count = 1
                delivery_log.failure_count = 0
                delivery_log.retry_count = attempt - 1
                delivery_log.file_size_bytes = file_size
                delivery_log.processing_time_ms = int(processing_time)
                delivery_log.delivery_details = {
                    'sftp_host': host,
                    'sftp_port': port,
                    'remote_path': upload_result['remote_path'],
                    'filename': remote_filename,
                    'upload_time_ms': upload_result['upload_time_ms'],
                    'file_size': upload_result['file_size'],
                    'method': 'sftp'
                }

                db.flush()  # Don't commit here, let executor commit
                return log_id

            except Exception as e:
                last_error = str(e)

                if attempt < max_retry:
                    # Wait before retry (short intervals in seconds)
                    wait_seconds = retry_interval * attempt
                    time.sleep(wait_seconds)
                    continue
                else:
                    # Max retries reached - mark as failed
                    raise

    except Exception as e:
        # Failed - update log
        processing_time = (now_jakarta() - send_start).total_seconds() * 1000

        # Build safe config for logging (mask password)
        safe_sftp_config = {k: ('***MASKED***' if k == 'password' else v)
                           for k, v in sftp_config.items()} if 'sftp_config' in locals() else {}

        delivery_log.status = 'failed'
        delivery_log.completed_at = now_jakarta()
        delivery_log.recipient_count = 1
        delivery_log.success_count = 0
        delivery_log.failure_count = 1
        delivery_log.retry_count = max_retry if 'max_retry' in locals() else 3
        delivery_log.error_message = str(e)
        delivery_log.processing_time_ms = int(processing_time)
        delivery_log.delivery_details = {
            'sftp_config': safe_sftp_config,
            'error_type': type(e).__name__,
            'last_attempt': attempt if 'attempt' in locals() else 1,
            'method': 'sftp'
        }

        db.flush()  # Don't commit here, let executor commit

        # Don't raise - just log the failure
        # This allows execution to complete even if delivery fails
        print(f"SFTP delivery failed: {e}")
        return log_id
