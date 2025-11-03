# app/routes/reports.py

import json
import os
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime
from app.services.drivers.geo import haversine, get_bounding_box
from app.utils.firebase_connection import db

router = APIRouter(prefix="/api/drivers", tags=["Drivers"])


@router.get("/auto-allocation")
def auto_allocation(pickup_lat: float, pickup_lng: float, radius: float = 2.0):
    if pickup_lat is None or pickup_lng is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="pickup_lat and pickup_lng are required",
        )

    box = get_bounding_box(pickup_lat, pickup_lng, radius)

    drivers_ref = (
        db.collection("drivers")
        .where("lat", ">=", box["min_lat"])
        .where("lat", "<=", box["max_lat"])
    )

    try:
        docs = drivers_ref.stream()
        driver_summaries = []
        for doc in docs:
            data = doc.to_dict() or {}
            driver_lat = data.get("lat")
            driver_lng = data.get("lng")
            if driver_lat is None or driver_lng is None:
                continue

            if box["min_lng"] <= float(driver_lng) <= box["max_lng"]:
                distance = haversine(
                    float(driver_lat), float(driver_lng), pickup_lat, pickup_lng
                )
                if distance <= radius:
                    driver_summaries.append(
                        {
                            "driver_id": doc.id,
                            "name": data.get("name"),
                            "lat": float(driver_lat),
                            "lng": float(driver_lng),
                            "distance_km": round(distance, 2),
                        }
                    )

        driver_summaries.sort(key=lambda x: x["distance_km"])
        return {"driver_summaries": driver_summaries}

    except Exception as e:
        # Let FastAPI set the status code and JSON body
        raise HTTPException(status_code=500, detail=str(e))
