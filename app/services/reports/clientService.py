from collections import defaultdict
from datetime import datetime
import heapq

def clientReport(data):
    # ----------------- Helper Functions -----------------
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
            created_str = order.get("created_at")
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

    # ----------------- Table Data -----------------
    clients = defaultdict(
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
        client = order.get("user_name", "Unknown")
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

        # Various timings
        if created and delivery_success:
            clients[client]["DeliveryTimes"].append(
                (delivery_success - created).total_seconds() / 60
            )

        if created and pickup_assigned:
            clients[client]["AssignTimes"].append(
                (pickup_assigned - created).total_seconds() / 60
            )

        if pickup_success and pickup_arrived:
            clients[client]["PickupWaits"].append(
                (pickup_success - pickup_arrived).total_seconds() / 60
            )

        if delivery_arrived and delivery_started:
            clients[client]["TravelTimes"].append(
                (delivery_arrived - delivery_started).total_seconds() / 60
            )

        if delivery_success and delivery_arrived:
            clients[client]["DropoffWaits"].append(
                (delivery_success - delivery_arrived).total_seconds() / 60
            )

        clients[client]["Amount"] += amount
        clients[client]["Orders"] += 1

    # ----------------- Build Table Rows -----------------
    rows = []
    for client, stats in clients.items():
        def avg(lst):
            return round(sum(lst) / len(lst), 2) if lst else None

        avg_fare = (
            round(stats["Amount"] / stats["Orders"], 2)
            if stats["Orders"] > 0
            else 0
        )

        rows.append(
            {
                "Client": client,
                "Orders": stats["Orders"],
                "Total Fare": round(stats["Amount"], 2),
                "Average Fare": avg_fare,
                "Average Delivery Time (min)": avg(stats["DeliveryTimes"]),
                "Avg Time to Assign (min)": avg(stats["AssignTimes"]),
                "Avg Pickup Waiting (min)": avg(stats["PickupWaits"]),
                "Avg Travel to Customer (min)": avg(stats["TravelTimes"]),
                "Avg Dropoff Waiting (min)": avg(stats["DropoffWaits"]),
            }
        )

    # ----------------- Optimized Top 15 Clients -----------------
    top_by_orders = heapq.nlargest(15, rows, key=lambda x: x["Orders"])
    top_by_fare = heapq.nlargest(15, rows, key=lambda x: x["Total Fare"])

    # ----------------- Recharts-Friendly Chart Data -----------------
    charts = {
        "top_clients_by_orders": [
            {"name": x["Client"], "value": x["Orders"]} for x in top_by_orders
        ],
        "top_clients_by_fare": [
            {"name": x["Client"], "value": x["Total Fare"]} for x in top_by_fare
        ],
        "scatter_clients": [
            {"client": x["Client"], "orders": x["Orders"], "amount": x["Total Fare"]}
            for x in rows
        ],
    }

    # ----------------- Build Summary -----------------
    summary = {
        "number_of_orders": count_orders(data),
        "total_fare": total_fare(data),
        "average_fare": average_fare(data),
        "average_delivery_time": average_time_taken(data),
        "charts": charts,
        "table": rows,
    }

    return summary
