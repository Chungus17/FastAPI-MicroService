# app/routes/reports.py

from fastapi import APIRouter, Query
from typing import List, Optional
from datetime import time, datetime

from app.services.driverService import driverReport
from app.services.clientService import clientReport
from app.services.hourlyService import hourlyReport
from app.utils.data_fetcher import getData

router = APIRouter(prefix="/api", tags=["Reports"])


@router.get("/3pl_report")
async def generate_3pl_report(
    start_date: str,
    end_date: str,
    filter_by: Optional[List[str]] = Query(default=None),
    status: str = "all",
):
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

    if status != "all":
        data = [
            order for order in data if str(order.get("status", "")).lower() == status
        ]

    summary = driverReport(data)
    return summary


@router.get("/client_report")
async def generate_client_report(
    start_date: str,
    end_date: str,
    filter_by: Optional[List[str]] = Query(default=None),
    status: str = "all",
):
    data = await getData(start_date, end_date, "all")

    # Filter by client name
    if filter_by and not any(f.lower() == "all" for f in filter_by):
        data = [
            order
            for order in data
            if any(
                (
                    (order.get("client", {}).get("name") or "").strip().lower()
                    == f.lower()
                )
                for f in filter_by
                if (order.get("client", {}).get("name") or "").strip()
            )
        ]

    if status != "all":
        data = [
            order for order in data if str(order.get("status", "")).lower() == status
        ]

    summary = clientReport(data)
    return summary


@router.get("/hourly_report")
async def generate_hourly_report(
    start_date: str,
    end_date: str,
    start_time: str,
    end_time: str,
    filter_by: Optional[List[str]] = Query(default=None),
    status: str = "all",
):
    # Fetch all data for the date range
    data = await getData(start_date, end_date, "all")

    # Parse the start_time and end_time into time objects
    start_time_obj = datetime.strptime(start_time, "%H:%M").time()
    end_time_obj = datetime.strptime(end_time, "%H:%M").time()

    # Helper function to check if a datetime is in the filtered time window
    def is_in_time_window(dt: datetime) -> bool:
        t = dt.time()
        if start_time_obj <= end_time_obj:
            return start_time_obj <= t <= end_time_obj
        else:
            # Overnight range, e.g., 22:00–02:00
            return t >= start_time_obj or t <= end_time_obj

    # Filter orders by time window
    data = [
        order
        for order in data
        if order.get("created_at")
        and is_in_time_window(datetime.strptime(order["created_at"], "%Y-%m-%d %H:%M:%S"))
    ]

    # Filter by client name if provided
    if filter_by and not any(f.lower() == "all" for f in filter_by):
        data = [
            order
            for order in data
            if any(
                (order.get("client", {}).get("name") or "").strip().lower() == f.lower()
                for f in filter_by
                if (order.get("client", {}).get("name") or "").strip()
            )
        ]

    # Filter by status if provided
    if status.lower() != "all":
        data = [
            order for order in data
            if str(order.get("status", "")).lower() == status.lower()
        ]

    # Call the hourlyReport function to generate the JSON response
    summary = hourlyReport(
        data,
        start_date=start_date,
        end_date=end_date,
        top_n_clients=5
    )

    return summary