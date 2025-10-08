import pytz
from datetime import datetime

# Default timezone for the application
DEFAULT_TIMEZONE = pytz.timezone('Asia/Jakarta')

def now_jakarta():
    """Get current datetime in Asia/Jakarta timezone (timezone-naive for MySQL)"""
    return datetime.now(DEFAULT_TIMEZONE).replace(tzinfo=None)

def utc_to_jakarta(dt: datetime) -> datetime:
    """Convert UTC datetime to Asia/Jakarta"""
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    return dt.astimezone(DEFAULT_TIMEZONE).replace(tzinfo=None)
