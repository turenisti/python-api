from fastapi import APIRouter, HTTPException, Depends, Request
from sqlalchemy.orm import Session
from typing import Optional

from execution_engine.api.schemas import StandardResponse
from execution_engine.services.executor import execute_report
from shared.database import get_db
from shared.models import ReportExecution

router = APIRouter(prefix="/api", tags=["execution"])

@router.get("/execute/{config_id}")
async def execute_report_endpoint(
    config_id: int,
    request: Request,
    schedule_id: Optional[int] = None
):
    """
    Execute report manually or from scheduler (GET endpoint)

    Uses pre-configured filters from report_configs.parameters
    Extracts executed_by from X-User-ID header (matches Go API pattern)

    Args:
        config_id: Report configuration ID
        schedule_id: Optional schedule ID (query parameter)
        request: FastAPI Request object to access headers

    Returns:
        StandardResponse with execution details
    """
    try:
        # Extract X-User-ID from header, default to "system"
        executed_by = request.headers.get("X-User-ID", "system")

        result = await execute_report(
            config_id=config_id,
            schedule_id=schedule_id,
            executed_by=executed_by
        )

        return {
            "status": "success",
            "message": "Report execution completed successfully",
            "data": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/execution/{execution_id}")
async def get_execution_status(execution_id: str, db: Session = Depends(get_db)):
    """
    Get execution status and details

    Args:
        execution_id: UUID of execution record

    Returns:
        StandardResponse with execution record
    """
    try:
        execution = db.query(ReportExecution).filter_by(id=execution_id).first()

        if not execution:
            raise HTTPException(status_code=404, detail="Execution not found")

        return {
            "status": "success",
            "message": "Execution found",
            "data": {
                "id": execution.id,
                "config_id": execution.config_id,
                "schedule_id": execution.schedule_id,
                "status": execution.status,
                "started_at": execution.started_at.isoformat() if execution.started_at else None,
                "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
                "executed_by": execution.executed_by,
                "execution_context": execution.execution_context,
                "query_execution_time_ms": execution.query_execution_time_ms,
                "rows_returned": execution.rows_returned,
                "file_generated_path": execution.file_generated_path,
                "file_size_bytes": execution.file_size_bytes,
                "error_message": execution.error_message
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
