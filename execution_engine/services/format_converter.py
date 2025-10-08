import pandas as pd
import os
from pathlib import Path
from typing import Optional, List

def filter_display_columns(df: pd.DataFrame, display_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Filter DataFrame to only include specified display columns

    Args:
        df: pandas DataFrame with all query results
        display_columns: List of column names to include in output (None = all columns)

    Returns:
        pandas.DataFrame with only specified columns
    """
    if not display_columns:
        return df

    # Only include columns that exist in the DataFrame
    valid_columns = [col for col in display_columns if col in df.columns]

    if not valid_columns:
        # If no valid columns, return all
        return df

    return df[valid_columns]

def convert_to_format(
    df: pd.DataFrame,
    output_format: str,
    output_path: str,
    display_columns: Optional[List[str]] = None
) -> str:
    """
    Convert pandas DataFrame to specified format with optional column filtering

    Args:
        df: pandas DataFrame with query results
        output_format: 'csv' or 'xlsx'
        output_path: Full path where file should be saved
        display_columns: Optional list of columns to include in output

    Returns:
        str: Path to generated file
    """

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Filter to display columns if specified
    df_output = filter_display_columns(df, display_columns)

    if output_format == 'csv':
        df_output.to_csv(output_path, index=False, encoding='utf-8')
    elif output_format == 'xlsx':
        df_output.to_excel(output_path, index=False, engine='openpyxl')
    else:
        raise ValueError(f"Unsupported format: {output_format}. Use 'csv' or 'xlsx'")

    return output_path

def get_file_size(file_path: str) -> int:
    """Get file size in bytes"""
    return os.path.getsize(file_path)
