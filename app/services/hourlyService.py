from collections import defaultdict
from datetime import datetime
import calendar
from statistics import mean

def hourlyReport(data, start_date, end_date, top_n_clients=5):

    # ---------- Initialize structures ----------
    hour_orders = defaultdict(list)                        # hour -> list of orders
    hour_client_orders = defaultdict(lambda: defaultdict(int))  # hour -> client -> count
    weekday_hour_orders = defaultdict(lambda: defaultdict(list)) # weekday -> hour -> list of orders
    weekday_dates = defaultdict(set)                       # weekday -> set of unique dates
    delivery_times = []

    total_orders = 0
    total_amount = 0.0

    # ---------- Process each order ----------
    for order in data:
        created_str = order.get("created_at")
        if not created_str:
            continue
        dt = datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S")
        hour = dt.hour
        weekday = dt.weekday()  # Monday=0
        client = order.get("user_name", "Unknown")
        amount = abs(float(order.get("amount", 0) or 0))

        # Track totals
        total_orders += 1
        total_amount += amount
        weekday_dates[weekday].add(dt.date())

        # Hourly and weekday-hourly aggregation
        hour_orders[hour].append(order)
        hour_client_orders[hour][client] += 1
        weekday_hour_orders[weekday][hour].append(order)

        # Delivery time (optional)
        pickup_started_str = order.get("pickup_task", {}).get("started_at")
        delivery_success_str = order.get("delivery_task", {}).get("successful_at")
        if pickup_started_str and delivery_success_str:
            try:
                pickup_started = datetime.strptime(pickup_started_str, "%Y-%m-%d %H:%M:%S")
                delivery_success = datetime.strptime(delivery_success_str, "%Y-%m-%d %H:%M:%S")
                delivery_times.append((delivery_success - pickup_started).total_seconds() / 60)
            except:
                pass

    # ---------- Hourly chart ----------
    hourly_chart = []
    for h in range(24):
        num_days = max(len(set(dt.date() for dt in [datetime.strptime(o["created_at"], "%Y-%m-%d %H:%M:%S") for o in hour_orders[h]])), 1)
        avg_orders = len(hour_orders[h]) / num_days
        top_clients = sorted(hour_client_orders[h].items(), key=lambda x: x[1], reverse=True)[:top_n_clients]
        hourly_chart.append({
            "hour": h,
            "average_orders": round(avg_orders, 2),
            "top_clients": [{"name": c, "orders": n} for c, n in top_clients]
        })

    # ---------- Heatmap ----------
    heatmap = []
    for w in range(7):
        hours_list = []
        num_days_for_weekday = max(len(weekday_dates[w]), 1)
        for h in range(24):
            avg_orders = len(weekday_hour_orders[w][h]) / num_days_for_weekday
            hours_list.append({"hour": h, "average_orders": round(avg_orders, 2)})
        heatmap.append({
            "weekday": calendar.day_name[w],
            "hours": hours_list
        })

    # ---------- Summary ----------
    # Hottest / coolest hour
    hottest_hour = max(hourly_chart, key=lambda x: x["average_orders"])
    coolest_hour = min(hourly_chart, key=lambda x: x["average_orders"])

    # Hottest / coolest day and average orders per weekday
    day_totals = []
    average_orders_per_weekday = {}
    for w in range(7):
        num_days_for_weekday = max(len(weekday_dates[w]), 1)
        total_orders_for_weekday = sum(len(weekday_hour_orders[w][h]) for h in range(24))
        avg_orders_for_weekday = total_orders_for_weekday / num_days_for_weekday
        day_totals.append(avg_orders_for_weekday)
        average_orders_per_weekday[calendar.day_name[w]] = round(avg_orders_for_weekday, 2)

    hottest_day_index = day_totals.index(max(day_totals))
    coolest_day_index = day_totals.index(min(day_totals))

    summary = {
        "total_orders": total_orders,
        "date_range": {"start": start_date, "end": end_date},
        "hottest_hour": hottest_hour,
        "coolest_hour": coolest_hour,
        "hottest_day": {
            "weekday": calendar.day_name[hottest_day_index],
            "average_orders": round(day_totals[hottest_day_index], 2)
        },
        "coolest_day": {
            "weekday": calendar.day_name[coolest_day_index],
            "average_orders": round(day_totals[coolest_day_index], 2)
        },
        "avg_delivery_time": round(mean(delivery_times), 2) if delivery_times else None,
        "total_amount_collected": round(total_amount, 2),
        "avg_fare_per_order": round(total_amount / total_orders, 2) if total_orders > 0 else 0,
        "average_orders_per_weekday": average_orders_per_weekday
    }

    return {
        "summary": summary,
        "hourly_chart": hourly_chart,
        "heatmap": heatmap
    }
