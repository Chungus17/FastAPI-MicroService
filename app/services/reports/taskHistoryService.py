import os
from datetime import datetime, timedelta
import json
import redis

redis_client = redis.Redis(
    host=os.environ.get("REDIS_HOST"),
    port=os.environ.get("REDIS_PORT"),
    decode_responses=True,
    username="default",
    password=os.environ.get("REDIS_PASSWORD"),
)


def task_history(data):
    def parse_dt(ts):
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") if ts else None

    # --- Summary helpers ---
    def count_orders(data):
        return len(data)

    def total_fare(data):
        return round(sum(abs(float(order["amount"])) for order in data), 2)

    def average_fare(data):
        num_orders = count_orders(data)
        return round(total_fare(data) / num_orders, 2) if num_orders > 0 else 0

    def average_delivery_time(data):
        total_minutes = 0
        count = 0
        for order in data:
            created = parse_dt(order.get("created_at"))
            successful = parse_dt(order.get("delivery_task", {}).get("successful_at"))
            if created and successful:
                total_minutes += (successful - created).total_seconds() / 60
                count += 1
        return round(total_minutes / count, 2) if count > 0 else 0

    # --- Final return ---
    return {
        "number_of_orders": count_orders(data),
        "total_fare": total_fare(data),
        "average_fare": average_fare(data),
        "average_delivery_time": average_delivery_time(data),
        # "table": rows,  # ✅ each row = one order
    }


def task_history_table(job_id, data):
    def parse_dt(ts):
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") if ts else None

    def minutes_diff(start, end):
        return round((end - start).total_seconds() / 60, 2) if start and end else None

    rows = []
    for order in data:
        created = parse_dt(order.get("created_at"))
        pickup = order.get("pickup_task", {})
        delivery = order.get("delivery_task", {})

        pickup_assigned = parse_dt(pickup.get("assigned_at"))
        pickup_arrived = parse_dt(pickup.get("arrived_at"))
        pickup_success = parse_dt(pickup.get("successful_at"))

        delivery_started = parse_dt(delivery.get("started_at"))
        delivery_arrived = parse_dt(delivery.get("arrived_at"))
        delivery_success = parse_dt(delivery.get("successful_at"))

        rows.append(
            {
                "Order ID": order.get("reference"),
                "Client": order.get("user_name"),
                "Amount": round(abs(float(order.get("amount", 0))), 2),
                "Status": order.get("status"),
                "Created At": order.get("created_at"),
                "Time to Assign (min)": minutes_diff(created, pickup_assigned),
                "Time to Pickup (min)": minutes_diff(pickup_assigned, pickup_success),
                "Pickup Waiting (min)": minutes_diff(pickup_arrived, pickup_success),
                "Travel to Customer (min)": minutes_diff(
                    delivery_started, delivery_arrived
                ),
                "Dropoff Waiting (min)": minutes_diff(
                    delivery_arrived, delivery_success
                ),
                "Total Delivery Time (min)": minutes_diff(created, delivery_success),
            }
        )

    # Store in Redis
    redis_client.set(job_id, json.dumps(rows), ex=30)
