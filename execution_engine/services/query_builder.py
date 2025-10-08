from typing import Dict, List, Any, Optional

def is_date_filter(filter_def: Dict) -> bool:
    """
    Check if filter is a date filter (should be skipped)

    Date filters should be in the query using template variables like {{yesterday}}
    NOT in parameters.filters
    """
    data_type = filter_def.get('type', 'string')
    field = filter_def.get('field', '')

    # Skip if explicitly marked as date type
    if data_type == 'date':
        return True

    # Skip if field contains common date patterns
    date_patterns = [
        'DATE(',
        'TIMESTAMP(',
        'created_at',
        'updated_at',
        'deleted_at',
        'date_',
        '_date',
        'datetime',
        'time_'
    ]

    field_lower = field.lower()
    for pattern in date_patterns:
        if pattern.lower() in field_lower:
            return True

    return False

def build_where_clause(filters: List[Dict], template_vars: Dict[str, Any] = None) -> str:
    """
    Build WHERE clause from pre-configured filter values

    IMPORTANT: Date filters are SKIPPED - dates should be in query using {{yesterday}}, {{start_date}}, etc.
    """
    if not filters:
        return ""

    template_vars = template_vars or {}
    where_conditions = []

    for filter_def in filters:
        # SKIP DATE FILTERS - they should be in the query, not in parameters
        if is_date_filter(filter_def):
            continue

        field = filter_def.get('field')
        operator = filter_def.get('operator', '=')
        data_type = filter_def.get('type', 'string')
        value = filter_def.get('value')

        if value is None:
            continue

        # Replace template variables
        if isinstance(value, str):
            for var_name, var_value in template_vars.items():
                value = value.replace(f'{{{{{var_name}}}}}', str(var_value))

        # Build condition
        if operator in ['=', '!=', '>', '>=', '<', '<=']:
            if data_type == 'number':
                condition = f"{field} {operator} {value}"
            else:
                condition = f"{field} {operator} '{value}'"
        elif operator == 'LIKE':
            condition = f"{field} LIKE '%{value}%'"
        elif operator == 'IN':
            if isinstance(value, list):
                quoted_values = [f"'{v}'" for v in value]
                condition = f"{field} IN ({', '.join(quoted_values)})"
            else:
                condition = f"{field} = '{value}'"
        else:
            condition = f"{field} = '{value}'"

        where_conditions.append(condition)

    if where_conditions:
        return "WHERE " + " AND ".join(where_conditions)
    return ""

def apply_filters_to_query(base_query: str, filters: List[Dict], template_vars: Dict[str, Any] = None) -> str:
    """
    Apply pre-configured filters to base query

    If query already has WHERE clause, append with AND
    If query doesn't have WHERE clause, add WHERE
    """
    # Build filter conditions (without WHERE keyword)
    where_conditions = []

    for filter_def in filters:
        # SKIP DATE FILTERS
        if is_date_filter(filter_def):
            continue

        field = filter_def.get('field')
        operator = filter_def.get('operator', '=')
        data_type = filter_def.get('type', 'string')
        value = filter_def.get('value')

        if value is None:
            continue

        # Replace template variables
        if isinstance(value, str) and template_vars:
            for var_name, var_value in template_vars.items():
                value = value.replace(f'{{{{{var_name}}}}}', str(var_value))

        # Build condition
        if operator in ['=', '!=', '>', '>=', '<', '<=']:
            if data_type == 'number':
                condition = f"{field} {operator} {value}"
            else:
                condition = f"{field} {operator} '{value}'"
        elif operator == 'LIKE':
            condition = f"{field} LIKE '%{value}%'"
        elif operator == 'IN':
            if isinstance(value, list):
                quoted_values = [f"'{v}'" for v in value]
                condition = f"{field} IN ({', '.join(quoted_values)})"
            else:
                condition = f"{field} = '{value}'"
        else:
            condition = f"{field} = '{value}'"

        where_conditions.append(condition)

    # If no conditions to add, return original query
    if not where_conditions:
        return base_query

    # Check if query already has WHERE clause
    query_upper = base_query.upper()
    has_where = 'WHERE' in query_upper

    # Find insertion point (before ORDER BY, LIMIT, GROUP BY, HAVING)
    insertion_keywords = ['ORDER BY', 'LIMIT', 'GROUP BY', 'HAVING']
    insertion_pos = len(base_query)

    for keyword in insertion_keywords:
        pos = query_upper.find(keyword)
        if pos != -1 and pos < insertion_pos:
            insertion_pos = pos

    # Build the filter clause
    if has_where:
        # Append with AND
        filter_clause = "AND " + " AND ".join(where_conditions)
    else:
        # Add new WHERE clause
        filter_clause = "WHERE " + " AND ".join(where_conditions)

    # Insert filters before ORDER BY/LIMIT/etc
    return base_query[:insertion_pos].rstrip() + "\n" + filter_clause + "\n" + base_query[insertion_pos:]


def build_auto_date_filter(date_field: str, time_range: Dict, cron_expression: Optional[str] = None) -> str:
    """
    Automatically build date filter based on cron expression

    Simple logic:
    - Detects if cron is daily/hourly/weekly from cron pattern
    - Builds appropriate WHERE clause automatically

    Args:
        date_field: Field name to filter (e.g., "created_at", "payment_date")
        time_range: Dict with start_date, end_date, start_datetime, end_datetime, yesterday
        cron_expression: Cron expression to detect granularity (optional)

    Returns:
        WHERE clause string (e.g., "DATE(created_at) = '2025-10-06'")
    """
    if not date_field:
        return ""

    # Detect granularity from cron expression
    granularity = 'daily'  # default

    if cron_expression:
        parts = cron_expression.split()
        if len(parts) >= 5:
            minute, hour, day, month, weekday = parts[:5]

            # Hourly: minute is fixed, hour is *
            if minute != '*' and hour == '*':
                granularity = 'hourly'
            # Sub-hourly: minute has */N pattern
            elif minute.startswith('*/'):
                granularity = 'sub_hourly'
            # Weekly: weekday is specific
            elif weekday != '*':
                granularity = 'weekly'
            # Monthly: day is 1
            elif day == '1' and month == '*':
                granularity = 'monthly'
            # Daily: default
            else:
                granularity = 'daily'

    # Build filter based on granularity
    if granularity == 'daily':
        # Daily reports: Use DATE() = yesterday
        date_value = time_range.get('yesterday')
        return f"DATE({date_field}) = '{date_value}'"

    elif granularity in ['hourly', 'sub_hourly']:
        # Hourly/sub-hourly: Use datetime BETWEEN
        start_datetime = time_range.get('start_datetime')
        end_datetime = time_range.get('end_datetime')
        return f"{date_field} BETWEEN '{start_datetime}' AND '{end_datetime}'"

    elif granularity in ['weekly', 'monthly']:
        # Weekly/monthly: Use DATE() BETWEEN
        start_date = time_range.get('start_date')
        end_date = time_range.get('end_date')
        return f"DATE({date_field}) BETWEEN '{start_date}' AND '{end_date}'"

    # Fallback to daily
    date_value = time_range.get('yesterday')
    return f"DATE({date_field}) = '{date_value}'"
