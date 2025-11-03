from typing import Dict, Any, List
from app.services.drivers.geo import haversine, get_bounding_box
from app.utils.firebase_connection import db

def auto_allocation_one_by_one(
    pickup_lat: float,
    pickup_lng: float,
    max_radius: float,
) -> Dict[str, Any]:
    """
    Returns a flat, distance-sorted list of drivers within `max_radius` km.
    """
    box = get_bounding_box(pickup_lat, pickup_lng, max_radius)

    drivers_ref = (
        db.collection("drivers")
        .where("lat", ">=", box["min_lat"])
        .where("lat", "<=", box["max_lat"])
    )

    docs = drivers_ref.stream()
    driver_summaries: List[Dict[str, Any]] = []

    for doc in docs:
        data = doc.to_dict() or {}
        driver_lat = data.get("lat")
        driver_lng = data.get("lng")
        if driver_lat is None or driver_lng is None:
            continue

        # cheap longitude pre-filter
        if not (box["min_lng"] <= float(driver_lng) <= box["max_lng"]):
            continue

        d = haversine(float(driver_lat), float(driver_lng), pickup_lat, pickup_lng)
        if d <= max_radius:
            driver_summaries.append(
                {
                    "driver_id": doc.id,
                    "name": data.get("name"),
                    "lat": float(driver_lat),
                    "lng": float(driver_lng),
                    "distance_km": round(d, 2),
                }
            )

    driver_summaries.sort(key=lambda x: x["distance_km"])
    return {
        "pickup": {"lat": pickup_lat, "lng": pickup_lng},
        "max_radius_km": max_radius,
        "driver_summaries": driver_summaries,
    }
