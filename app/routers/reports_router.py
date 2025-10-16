from fastapi import APIRouter, Query
from typing import List, Optional
from app.services.reports_service import reports_3pl
from app.utils.data_fetcher import getData

router = APIRouter(prefix="/api", tags=["Reports"])

@router.get("/3pl_report")
async def generate_3pl_report(
    start_date: str,
    end_date: str,
    filter_by: Optional[List[str]] = Query(default=None),
    status: str = "all",
):
    # Fetch data asynchronously
    data = await getData(start_date, end_date, "all")

    # Filter by driver
    if filter_by and not any(f.lower() == "all" for f in filter_by):
        data = [
            order
            for order in data
            if any(
                (
                    (order.get("pickup_task", {}).get("driver_name") or "")
                    .strip()
                    .split(" ")[-1]
                    .upper()
                    == f.upper()
                )
                for f in filter_by
                if (order.get("pickup_task", {}).get("driver_name") or "").strip()
            )
        ]

    # Filter by status
    if status != "all":
        data = [
            order for order in data if str(order.get("status", "")).lower() == status
        ]

    summary = reports_3pl(data)
    return summary
