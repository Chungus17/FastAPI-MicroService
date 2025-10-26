from datetime import datetime
from collections import defaultdict
import heapq  # For efficient top-k calculations

def driverReport(data):
    # ----------------- Base Calculations -----------------
    def count_orders(data):
        return len(data)

    def total_fare(data):
        return round(sum(abs(float(order["amount"])) for order in data), 2)

    def average_fare(data):
        num_orders = count_orders(data)
        return round(total_fare(data) / num_orders, 2) if num_orders > 0 else 0

    def average_time_taken(data):
        total_minutes = 0
        count = 0
        for order in data:
            created_str = order.get("pickup_task", {}).get("assigned_at")
            successful_str = order.get("delivery_task", {}).get("successful_at")
            if not created_str or not successful_str:
                continue
            try:
                created = datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S")
                successful = datetime.strptime(successful_str, "%Y-%m-%d %H:%M:%S")
                total_minutes += (successful - created).total_seconds() / 60
                count += 1
            except:
                continue
        return round(total_minutes / count, 2) if count > 0 else 0

    def total_earnings(data):
        return round(total_fare(data) * 0.85, 2)

    def total_revenue(data):
        fare = total_fare(data)
        return round(fare - (fare * 0.85), 2)

    # ----------------- Charts -----------------
    def charts_per_driver_group(data):
        groups = defaultdict(list)
        for order in data:
            driver_name = order.get("pickup_task", {}).get("driver_name", "")
            if not driver_name:
                continue
            group = driver_name.split()[-1].upper()
            groups[group].append(order)

        result = {
            "number_of_orders": {},
            "total_fare": {},
            "average_fare": {},
            "total_earnings": {},
        }

        for group, orders in groups.items():
            num_orders = len(orders)
            fare = round(sum(abs(float(o["amount"])) for o in orders), 2)
            avg_fare = round(fare / num_orders, 2) if num_orders > 0 else 0
            earnings = round(fare * 0.85, 2)

            result["number_of_orders"][group] = num_orders
            result["total_fare"][group] = fare
            result["average_fare"][group] = avg_fare
            result["total_earnings"][group] = earnings

        return result

    # ----------------- Table Data + Top 10 Drivers -----------------
    drivers = defaultdict(
        lambda: {
            "Amount": 0,
            "Orders": 0,
            "DeliveryTimes": [],
            "AssignTimes": [],
            "PickupWaits": [],
            "TravelTimes": [],
            "DropoffWaits": [],
        }
    )

    def parse_dt(ts):
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") if ts else None

    for order in data:
        driver = order.get("pickup_task", {}).get("driver_name", "Unknown")
        amount_str = order.get("amount")

        try:
            amount = round(abs(float(amount_str)), 2)
        except:
            amount = 0

        created = parse_dt(order.get("created_at"))
        pickup = order.get("pickup_task", {})
        delivery = order.get("delivery_task", {})

        pickup_assigned = parse_dt(pickup.get("assigned_at"))
        pickup_arrived = parse_dt(pickup.get("arrived_at"))
        pickup_success = parse_dt(pickup.get("successful_at"))

        delivery_started = parse_dt(delivery.get("started_at"))
        delivery_arrived = parse_dt(delivery.get("arrived_at"))
        delivery_success = parse_dt(delivery.get("successful_at"))

        if delivery_success and pickup_assigned:
            drivers[driver]["DeliveryTimes"].append(
                (delivery_success - pickup_assigned).total_seconds() / 60
            )

        if created and pickup_assigned:
            drivers[driver]["AssignTimes"].append(
                (pickup_assigned - created).total_seconds() / 60
            )

        if pickup_success and pickup_arrived:
            drivers[driver]["PickupWaits"].append(
                (pickup_success - pickup_arrived).total_seconds() / 60
            )

        if delivery_arrived and delivery_started:
            drivers[driver]["TravelTimes"].append(
                (delivery_arrived - delivery_started).total_seconds() / 60
            )

        if delivery_success and delivery_arrived:
            drivers[driver]["DropoffWaits"].append(
                (delivery_success - delivery_arrived).total_seconds() / 60
            )

        drivers[driver]["Amount"] += amount
        drivers[driver]["Orders"] += 1

    # ----------------- Build Table Rows -----------------
    rows = []
    for driver, stats in drivers.items():
        def avg(lst):
            return round(sum(lst) / len(lst), 2) if lst else None

        rows.append(
            {
                "Driver": driver,
                "Orders": stats["Orders"],
                "Amount": round(stats["Amount"], 2),
                "Average Delivery Time (min)": avg(stats["DeliveryTimes"]),
                "Avg Time to Assign (min)": avg(stats["AssignTimes"]),
                "Avg Pickup Waiting (min)": avg(stats["PickupWaits"]),
                "Avg Travel to Customer (min)": avg(stats["TravelTimes"]),
                "Avg Dropoff Waiting (min)": avg(stats["DropoffWaits"]),
            }
        )

    # ----------------- Optimized Top 10 Drivers -----------------
    top_by_orders = heapq.nlargest(10, rows, key=lambda x: x["Orders"])
    top_by_fastest_delivery = heapq.nsmallest(
        10, [r for r in rows if r["Average Delivery Time (min)"] is not None],
        key=lambda x: x["Average Delivery Time (min)"]
    )

    # ----------------- Build Summary -----------------
    summary = {
        "Number of Orders": count_orders(data),
        "Total Fare": total_fare(data),
        "Average Fare": average_fare(data),
        "Average Time Taken (minutes)": average_time_taken(data),
        "Total Earnings": total_earnings(data),
        "Total Revenue": total_revenue(data),
        "Charts": charts_per_driver_group(data),
        "table_data": rows,
        "top_drivers": {
            "top_by_orders": top_by_orders,
            "top_by_fastest_delivery": top_by_fastest_delivery,
        }
    }

    return summary
