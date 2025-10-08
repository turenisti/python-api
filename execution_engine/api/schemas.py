from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime

class ExecuteRequest(BaseModel):
    schedule_id: Optional[int] = None
    executed_by: str = "manual"
    filter_values: Optional[Dict[str, Any]] = None  # Dynamic filter values

class ExecutionResponse(BaseModel):
    execution_id: str
    config_id: int
    config_name: str
    schedule_id: Optional[int]
    status: str
    rows_returned: int
    file_path: str
    file_size_bytes: int
    deliveries_sent: int
    query_execution_time_ms: int
    total_execution_time_ms: int
    time_range: Dict[str, Any]

class StandardResponse(BaseModel):
    status: str
    message: str
    data: Optional[Any] = None
    error: Optional[str] = None
