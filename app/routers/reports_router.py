# app/routes/reports.py

import json
import os
from fastapi import APIRouter, Query
from typing import List, Optional
from datetime import datetime
import uuid
import threading

import redis

from app.services.driverService import driverReport
from app.services.clientService import clientReport
from app.services.hourlyService import hourlyReport
from app.services.driverEarningsService import driverEarnings
from app.services.areaReport import areaReport, formatAreas
from app.services.taskHistoryService import task_history, task_history_table
from app.utils.data_fetcher import getData

router = APIRouter(prefix="/api", tags=["Reports"])


@router.get("/driver_report")
async def generate_driver_report(
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
        and is_in_time_window(
            datetime.strptime(order["created_at"], "%Y-%m-%d %H:%M:%S")
        )
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
            order
            for order in data
            if str(order.get("status", "")).lower() == status.lower()
        ]

    # Call the hourlyReport function to generate the JSON response
    summary = hourlyReport(
        data, start_date=start_date, end_date=end_date, top_n_clients=5
    )

    return summary


@router.get("/area_report")
async def generate_area_report(
    start_date: str,
    end_date: str,
    start_time: str,  # e.g. "00:00" or "13:30"
    end_time: str,  # e.g. "23:59" or "17:30"
    areas: Optional[List[str]] = Query(
        default=None, alias="filter_by"
    ),  # optional filter by area names
    status: str = "all",
):
    # 1) Fetch base data for the date range
    data = await getData(start_date, end_date, "all")

    # 2) Parse time window (supports overnight windows like 22:00–02:00)
    start_time_obj = datetime.strptime(start_time, "%H:%M").time()
    end_time_obj = datetime.strptime(end_time, "%H:%M").time()

    def is_in_time_window(dt: datetime) -> bool:
        t = dt.time()
        if start_time_obj <= end_time_obj:
            return start_time_obj <= t <= end_time_obj
        else:
            # Overnight range
            return (t >= start_time_obj) or (t <= end_time_obj)

    # 3) Time filter on created_at
    data = [
        o
        for o in data
        if o.get("created_at")
        and is_in_time_window(datetime.strptime(o["created_at"], "%Y-%m-%d %H:%M:%S"))
    ]

    # 4) Status filter (if not "all")
    if status.lower() != "all":
        data = [o for o in data if str(o.get("status", "")).lower() == status.lower()]

    # 5) Enrich with area, latitude, longitude using cached alias map
    data = formatAreas(data)

    # 6) Optional filter by area names (case-insensitive)
    if areas and not any(a.lower() == "all" for a in areas):
        wanted = {a.strip().lower() for a in areas}
        data = [
            o for o in data if o.get("area") and o["area"].strip().lower() in wanted
        ]

    # 7) Optionally exclude "Unknown" bucket
    # if not include_unknown:
    #     data = [o for o in data if o.get("area") and o["area"] != "Unknown"]

    # 8) Build final report (statcards + heatmap + table)
    result = areaReport(data)

    return result


@router.get("/task_history")
async def generate_task_history(
    start_date: str,
    end_date: str,
    filter_by: Optional[List[str]] = Query(default=None),
    status: str = "all",
):

    job_id = str(uuid.uuid4())

    data = await getData(start_date, end_date, filter_by)

    if status != "all":
        data = [
            order for order in data if str(order.get("status", "")).lower() == status
        ]

    # Start background thread to generate table data
    threading.Thread(target=task_history_table, args=(job_id, data)).start()

    final_data = task_history(data)
    return {"status": "processing", "job_id": job_id, "summary": final_data}


@router.get("/task_history/{job_id}")
async def get_task_history_table(job_id: str):
    # Fetch the precomputed table data from Redis
    redis_client = redis.Redis(
        host=os.environ.get("REDIS_HOST"),
        port=os.environ.get("REDIS_PORT"),
        decode_responses=True,
        username="default",
        password=os.environ.get("REDIS_PASSWORD"),
    )

    table_data = redis_client.get(job_id)
    if table_data:
        return {"status": "completed", "table": json.loads(table_data)}
    else:
        return {"status": "processing", "table": []}


@router.get("/driver_earnings")
async def generate_driver_earnings(
    start_date: str,
    end_date: str,
    filter_by: Optional[List[str]] = Query(default=None),
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

    result = driverEarnings(data)
    return result
