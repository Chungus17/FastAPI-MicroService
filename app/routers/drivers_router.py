# app/routes/reports.py

import json
import os
from fastapi import APIRouter, Query
from typing import List, Optional
from datetime import datetime
from app.services.drivers.geo import haversine, get_bounding_box
from app.utils.firebase_connection import db

router = APIRouter(prefix="/api/drivers", tags=["Drivers"])


@router.get("/auto_allocation")
async def generate_driver_report(
    pickup_lat: float, pickup_lng: float, radius: float = 2.0
):
    try:
        if pickup_lat is None or pickup_lng is None:
            return jsonify({"error": "pickup_lat and pickup_lng are required"}), 400

        # Get bounding box
        box = get_bounding_box(pickup_lat, pickup_lng, radius)

        # Firestore query: range on lat only
        drivers_ref = (
            db.collection("drivers")
            .where("lat", ">=", box["min_lat"])
            .where("lat", "<=", box["max_lat"])
        )

        docs = drivers_ref.stream()

        driver_summaries = []
        for doc in docs:
            data = doc.to_dict()
            driver_lat = data.get("lat")
            driver_lng = data.get("lng")

            if driver_lat is not None and driver_lng is not None:
                # Filter longitude manually
                if box["min_lng"] <= driver_lng <= box["max_lng"]:
                    distance = haversine(driver_lat, driver_lng, pickup_lat, pickup_lng)
                    if distance <= radius:
                        driver_summaries.append(
                            {
                                "driver_id": doc.id,
                                "name": data.get("name"),
                                "lat": driver_lat,
                                "lng": driver_lng,
                                "distance_km": round(distance, 2),
                            }
                        )

        # Sort by nearest
        driver_summaries.sort(key=lambda x: x["distance_km"])

        return json({"driver_summaries": driver_summaries}), 200

    except Exception as e:
        return json({"error": str(e)}), 500
