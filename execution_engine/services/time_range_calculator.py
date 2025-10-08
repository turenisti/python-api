from croniter import croniter
from datetime import datetime, timedelta
from typing import Dict, Optional
from shared.models import ReportSchedule
import pytz

def calculate_time_range(schedule: Optional[ReportSchedule], execution_time: datetime) -> Dict[str, str]:
    """
    Calculate time range for query based on schedule

    Logic:
    1. If schedule.last_run_at exists → use it as start_time (most accurate)
    2. Else, calculate from cron expression (get previous scheduled time)
    3. End time is always execution_time

    Returns:
        {
            'start_datetime': '2025-10-06 12:00:00',
            'end_datetime': '2025-10-06 18:00:00',
            'start_date': '2025-10-06',
            'end_date': '2025-10-06',
            'interval_hours': 6.0,
            'interval_minutes': 360,
            'calculation_method': 'last_run_at' | 'cron_detection' | 'default_daily',
            'yesterday': '2025-10-05',
            'last_week': '2025-09-29',
            'execution_time': '2025-10-06 18:00:00'
        }
    """

    # Handle timezone - convert execution_time to schedule's timezone
    # Keep times naive (no tzinfo) since MySQL stores them without timezone
    if schedule and schedule.timezone:
        try:
            tz = pytz.timezone(schedule.timezone)
            if execution_time.tzinfo is None:
                # Localize to timezone first, then remove tzinfo
                execution_time = tz.localize(execution_time)
            else:
                execution_time = execution_time.astimezone(tz)
            # Remove tzinfo to keep it naive for MySQL
            execution_time = execution_time.replace(tzinfo=None)
        except:
            pass  # Use as-is if timezone invalid

    if schedule and schedule.last_run_at:
        # Use last run for accurate range
        start = schedule.last_run_at
        end = execution_time
        method = 'last_run_at'

    elif schedule and schedule.cron_expression:
        # Calculate from cron - get TWO previous occurrences
        # This gives us the COMPLETE interval between scheduled runs
        try:
            cron = croniter(schedule.cron_expression, execution_time)
            end = cron.get_prev(datetime)    # Most recent scheduled time BEFORE now
            start = cron.get_prev(datetime)  # The scheduled time BEFORE that
            method = 'cron_detection'
        except Exception:
            # Fallback to daily if cron parsing fails
            start = execution_time - timedelta(days=1)
            end = execution_time
            method = 'default_daily'
    else:
        # Default to daily (yesterday)
        start = execution_time - timedelta(days=1)
        end = execution_time
        method = 'default_daily'

    # Calculate intervals (both should be naive now)
    interval_seconds = (end - start).total_seconds()
    interval_hours = interval_seconds / 3600
    interval_minutes = interval_seconds / 60

    return {
        'start_datetime': start.strftime('%Y-%m-%d %H:%M:%S'),
        'end_datetime': end.strftime('%Y-%m-%d %H:%M:%S'),
        'start_date': start.strftime('%Y-%m-%d'),
        'end_date': end.strftime('%Y-%m-%d'),
        'start_time': start.strftime('%H:%M:%S'),
        'end_time': end.strftime('%H:%M:%S'),
        'interval_hours': round(interval_hours, 2),
        'interval_minutes': round(interval_minutes, 2),
        'calculation_method': method,
        'yesterday': (end - timedelta(days=1)).strftime('%Y-%m-%d'),
        'last_week': (end - timedelta(days=7)).strftime('%Y-%m-%d'),
        'last_month': (end - timedelta(days=30)).strftime('%Y-%m-%d'),
        'execution_time': end.strftime('%Y-%m-%d %H:%M:%S'),
        'execution_date': end.strftime('%Y-%m-%d'),
        'execution_hour': end.strftime('%H'),
    }

def replace_template_variables(query: str, time_range: Dict[str, str]) -> str:
    """
    Replace template variables in query with actual values

    Supported variables:
    - {{start_datetime}}, {{end_datetime}}
    - {{start_date}}, {{end_date}}
    - {{start_time}}, {{end_time}}
    - {{yesterday}}, {{last_week}}, {{last_month}}
    - {{execution_time}}, {{execution_date}}
    """

    final_query = query

    for key, value in time_range.items():
        placeholder = f"{{{{{key}}}}}"
        # Convert value to string if it's not already
        str_value = str(value) if not isinstance(value, str) else value
        final_query = final_query.replace(placeholder, str_value)

    return final_query
