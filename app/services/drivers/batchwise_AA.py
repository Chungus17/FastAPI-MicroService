from typing import Dict, Any, List, Tuple
from fastapi import HTTPException, status
from app.services.drivers.geo import haversine, get_bounding_box
from app.utils.firebase_connection import db


def _make_buckets(max_radius: float, increment: float) -> List[Tuple[float, float]]:
    buckets: List[Tuple[float, float]] = []
    start = 0.0
    while start < max_radius:
        end = min(start + increment, max_radius)
        buckets.append((start, end))
        start = end
    return buckets


def _bucket_index(distance: float, buckets: List[Tuple[float, float]]) -> int | None:
    for i, (start, end) in enumerate(buckets):
        if i < len(buckets) - 1:
            if start <= distance < end:
                return i
        else:
            if start <= distance <= end:
                return i
    return None


def _label(start: float, end: float) -> str:
    # e.g., 0-5, 5-10, 10-15 (no spaces; add spaces if you prefer "0 - 5")
    return f"{int(start) if start.is_integer() else start}-{int(end) if end.is_integer() else end}"


def auto_allocation_batchwise(
    pickup_lat: float,
    pickup_lng: float,
    max_radius: float,
    increment: float,
) -> Dict[str, Any]:
    """
    Groups drivers into distance buckets and returns a dict like:
    groups: { "0-5": [...], "5-10": [...], "10-15": [...] }
    """
    if increment <= 0 or max_radius <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="max_radius and increment must be > 0",
        )

    buckets = _make_buckets(max_radius, increment)
    # Prebuild ordered dict of labels -> list
    labels = [_label(s, e) for (s, e) in buckets]
    groups_map: Dict[str, List[Dict[str, Any]]] = {lbl: [] for lbl in labels}

    total = 0

    # Bounding-box prefilter (lat) + manual lng filter
    box = get_bounding_box(pickup_lat, pickup_lng, max_radius)
    drivers_ref = (
        db.collection("drivers")
        .where("lat", ">=", box["min_lat"])  # single range field: lat
        .where("lat", "<=", box["max_lat"])
    )

    docs = drivers_ref.stream()

    for doc in docs:
        data = doc.to_dict() or {}
        driver_lat = data.get("lat")
        driver_lng = data.get("lng")
        if driver_lat is None or driver_lng is None:
            continue

        if not (box["min_lng"] <= float(driver_lng) <= box["max_lng"]):
            continue

        d = haversine(float(driver_lat), float(driver_lng), pickup_lat, pickup_lng)
        if d > max_radius:
            continue

        idx = _bucket_index(d, buckets)
        if idx is None:
            continue

        # if data.get("duty_state") == "OFF_DUTY":
        #     continue

        # if data.get("havingtask") == True:
        #     continue

        # if data.get("isOnline") == False:
        #     continue    

        print(f"Duty state: {data.get("duty_state")} &&&& Having task is: {data.get("havingtask")} &&&& is Online is: {data.get("isOnline")}")

        lbl = labels[idx]
        groups_map[lbl].append(
            {
                "driver_id": doc.id,
                "name": data.get("name"),
                "lat": float(driver_lat),
                "lng": float(driver_lng),
                "distance_km": round(d, 2),
            }
        )
        total += 1


    # Sort each bucket by distance
    for lst in groups_map.values():
        lst.sort(key=lambda x: x["distance_km"])

    return {
        "pickup": {"lat": pickup_lat, "lng": pickup_lng},
        "max_radius_km": max_radius,
        "increment_km": increment,
        "total_drivers": total,
        "driver_summaries": groups_map,
    }
