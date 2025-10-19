from collections import defaultdict
from datetime import datetime, timedelta
import json
import os

AREAS_JSON = os.getenv("AREAS_JSON_PATH")

def formatAreas(data):
    # Load JSON data from file
    with AREAS_JSON.open("r", encoding="utf-8") as file:
        areas_data = json.load(file)

    # Build a mapping of each alias → (canonical name, lat, lon)
    area_alias_map = {}
    for item in areas_data:
        if "neighborhoodenglish" in item:
            aliases = [
                alias.strip() for alias in item["neighborhoodenglish"].split(",")
            ]
            if aliases:
                canonical = aliases[0]  # first alias as canonical
                lat = item.get("centroid_y")  # latitude
                lon = item.get("centroid_x")  # longitude
                for alias in aliases:
                    area_alias_map[alias.lower()] = (canonical, lat, lon)

    # Area extraction helper
    def extract_area_with_coords(address):
        if not address:
            return "Unknown", None, None
        address_lower = address.lower()
        for alias, (canonical, lat, lon) in area_alias_map.items():
            if alias in address_lower:
                return canonical, lat, lon
        return "Unknown", None, None

    # Loop through data and add "area", "latitude", "longitude"
    for obj in data:
        pickup_address = obj.get("pickup_task", {}).get("address", "")
        area, lat, lon = extract_area_with_coords(pickup_address)
        obj["area"] = area
        obj["latitude"] = lat
        obj["longitude"] = lon

    return data


def areaReport(data):
    # Helper to parse datetime
    def parse_dt(ts):
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") if ts else None

    # Aggregate per area
    areas = defaultdict(
        lambda: {
            "Orders": 0,
            "Revenue": 0,
            "DeliveryTimes": [],
            "AssignTimes": [],
            "PickupWaits": [],
            "TravelTimes": [],
            "DropoffWaits": [],
        }
    )

    for order in data:
        area = order.get("area", "Unknown")
        try:
            revenue = round(abs(float(order.get("amount", 0))), 2)
        except:
            revenue = 0

        created = parse_dt(order.get("created_at"))
        pickup = order.get("pickup_task", {})
        delivery = order.get("delivery_task", {})

        pickup_assigned = parse_dt(pickup.get("assigned_at"))
        pickup_arrived = parse_dt(pickup.get("arrived_at"))
        pickup_success = parse_dt(pickup.get("successful_at"))

        delivery_started = parse_dt(delivery.get("started_at"))
        delivery_arrived = parse_dt(delivery.get("arrived_at"))
        delivery_success = parse_dt(delivery.get("successful_at"))

        # Aggregate metrics
        if created and delivery_success:
            areas[area]["DeliveryTimes"].append(
                (delivery_success - created).total_seconds() / 60
            )
        if created and pickup_assigned:
            areas[area]["AssignTimes"].append(
                (pickup_assigned - created).total_seconds() / 60
            )
        if pickup_success and pickup_arrived:
            areas[area]["PickupWaits"].append(
                (pickup_success - pickup_arrived).total_seconds() / 60
            )
        if delivery_arrived and delivery_started:
            areas[area]["TravelTimes"].append(
                (delivery_arrived - delivery_started).total_seconds() / 60
            )
        if delivery_success and delivery_arrived:
            areas[area]["DropoffWaits"].append(
                (delivery_success - delivery_arrived).total_seconds() / 60
            )

        # Update counts
        areas[area]["Orders"] += 1
        areas[area]["Revenue"] += revenue

    # Helper for average
    def avg(lst):
        return round(sum(lst) / len(lst), 2) if lst else 0

    # Build statcards
    total_orders = sum(a["Orders"] for a in areas.values())
    total_revenue = round(sum(a["Revenue"] for a in areas.values()), 2)
    avg_fare = round(total_revenue / total_orders, 2) if total_orders else 0
    avg_delivery_time = (
        round(sum(sum(a["DeliveryTimes"]) for a in areas.values()) / total_orders, 2)
        if total_orders
        else 0
    )

    statcards = {
        "number_of_orders": total_orders,
        "total_revenue": total_revenue,
        "average_fare": avg_fare,
        "average_delivery_time": avg_delivery_time,
    }

    # Build heatmap data (sorted by orders desc) with lat/lon
    heatmap = sorted(
        [
            {
                "area": area,
                "orders": a["Orders"],
                "revenue": a["Revenue"],
                "latitude": next(
                    (
                        order.get("latitude")
                        for order in data
                        if order.get("area") == area
                    ),
                    None,
                ),
                "longitude": next(
                    (
                        order.get("longitude")
                        for order in data
                        if order.get("area") == area
                    ),
                    None,
                ),
            }
            for area, a in areas.items()
        ],
        key=lambda x: x["orders"],
        reverse=True,
    )

    # Build table data (sorted by orders desc)
    table = sorted(
        [
            {
                "Area": area,
                "Orders": a["Orders"],
                "Total Revenue": a["Revenue"],
                "Average Fare": (
                    round(a["Revenue"] / a["Orders"], 2) if a["Orders"] else 0
                ),
                "Average Delivery Time (min)": avg(a["DeliveryTimes"]),
                "Avg Time to Assign (min)": avg(a["AssignTimes"]),
                "Avg Pickup Waiting (min)": avg(a["PickupWaits"]),
                "Avg Travel to Customer (min)": avg(a["TravelTimes"]),
                "Avg Dropoff Waiting (min)": avg(a["DropoffWaits"]),
            }
            for area, a in areas.items()
        ],
        key=lambda x: x["Orders"],
        reverse=True,
    )

    return {"statcards": statcards, "heatmap": heatmap, "table": table}

