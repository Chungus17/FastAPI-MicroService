from collections import defaultdict
from datetime import datetime
import calendar
from statistics import mean


def hourlyReport(
    data, start_date, end_date, start_time="00:00", end_time="23:59", top_n_clients=5
):
    start_hour = int(start_time.split(":")[0])
    end_hour = int(end_time.split(":")[0])

    def hours_in_range(start_h, end_h):
        if start_h <= end_h:
            return list(range(start_h, end_h + 1))
        return list(range(start_h, 24)) + list(range(0, end_h + 1))

    hours_iter = hours_in_range(start_hour, end_hour)

    hour_orders = defaultdict(list)
    hour_client_orders = defaultdict(lambda: defaultdict(int))
    hour_dates_set = defaultdict(set)  # hour -> unique dates (for averaging)

    weekday_hour_orders = defaultdict(lambda: defaultdict(list))
    weekday_dates = defaultdict(set)

    delivery_times = []
    total_orders = 0
    total_amount = 0.0

    for order in data:
        created_str = order.get("created_at")
        if not created_str:
            continue
        dt = datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S")
        hour = dt.hour
        weekday = dt.weekday()
        client = order.get("user_name", "Unknown")
        amount = abs(float(order.get("amount", 0) or 0))

        # filter by time (supports overnight)
        if start_hour <= end_hour:
            if not (start_hour <= hour <= end_hour):
                continue
        else:
            if not (hour >= start_hour or hour <= end_hour):
                continue

        total_orders += 1
        total_amount += amount

        # aggregates
        hour_orders[hour].append(order)
        hour_client_orders[hour][client] += 1
        hour_dates_set[hour].add(dt.date())

        weekday_hour_orders[weekday][hour].append(order)
        weekday_dates[weekday].add(dt.date())

        # delivery time (optional)
        ps = order.get("pickup_task", {}).get("started_at")
        ds = order.get("delivery_task", {}).get("successful_at")
        if ps and ds:
            try:
                pdt = datetime.strptime(ps, "%Y-%m-%d %H:%M:%S")
                ddt = datetime.strptime(ds, "%Y-%m-%d %H:%M:%S")
                delivery_times.append((ddt - pdt).total_seconds() / 60)
            except:
                pass

    # ---- Hourly totals (unchanged shape) ----
    total_orders_per_hour = {}
    for h in hours_iter:
        top_clients_total = sorted(
            hour_client_orders[h].items(), key=lambda x: x[1], reverse=True
        )[:top_n_clients]
        total_orders_per_hour[h] = {
            "orders": len(hour_orders[h]),
            "top_clients": [{"name": c, "orders": n} for c, n in top_clients_total],
        }

    # ---- Hourly averages (now keyed by hour, same style) ----
    average_orders_per_hour = {}
    for h in hours_iter:
        num_days = max(len(hour_dates_set[h]), 1)
        avg_orders = len(hour_orders[h]) / num_days

        # top clients by average per day for that hour
        top_clients_avg = [
            (client, cnt / num_days) for client, cnt in hour_client_orders[h].items()
        ]
        top_clients_avg_sorted = sorted(
            top_clients_avg, key=lambda x: x[1], reverse=True
        )[:top_n_clients]

        average_orders_per_hour[h] = {
            "orders": round(avg_orders, 2),
            "top_clients": [
                {"name": c, "orders": round(a, 2)}
                for c, a in top_clients_avg_sorted
            ],
        }

    # ---- Heatmap (unchanged) ----
    heatmap = []
    total_orders_per_weekday = {}
    for w in range(7):
        hours_list = []
        num_days_for_weekday = max(len(weekday_dates[w]), 1)
        total_orders_for_day = 0
        for h in hours_iter:
            avg_h = len(weekday_hour_orders[w][h]) / num_days_for_weekday
            hours_list.append({"hour": h, "orders": round(avg_h, 2)})
            total_orders_for_day += len(weekday_hour_orders[w][h])
        heatmap.append({"weekday": calendar.day_name[w], "hours": hours_list})
        total_orders_per_weekday[calendar.day_name[w]] = total_orders_for_day

    # ---- Summary (adjusted to read dicts) ----
    filtered_hours = [
        h for h in hours_iter if total_orders_per_hour[h]["orders"] > 0
    ]
    if filtered_hours:
        hottest_hour_key = max(
            filtered_hours, key=lambda h: average_orders_per_hour[h]["orders"]
        )
        coolest_hour_key = min(
            filtered_hours, key=lambda h: average_orders_per_hour[h]["orders"]
        )
        hottest_hour = {"hour": hottest_hour_key, **average_orders_per_hour[hottest_hour_key]}
        coolest_hour = {"hour": coolest_hour_key, **average_orders_per_hour[coolest_hour_key]}
    else:
        hottest_hour = coolest_hour = None

    average_orders_per_weekday_avg = {}
    filtered_days_avg = []
    for w in range(7):
        num_days_for_weekday = max(len(weekday_dates[w]), 1)
        total_orders_for_weekday = sum(
            len(weekday_hour_orders[w][h]) for h in hours_iter
        )
        avg_orders_for_weekday = total_orders_for_weekday / num_days_for_weekday
        average_orders_per_weekday_avg[calendar.day_name[w]] = round(
            avg_orders_for_weekday, 2
        )
        if total_orders_for_weekday > 0:
            filtered_days_avg.append((calendar.day_name[w], avg_orders_for_weekday))

    if filtered_days_avg:
        hottest_day_name, hottest_day_avg = max(filtered_days_avg, key=lambda x: x[1])
        coolest_day_name, coolest_day_avg = min(filtered_days_avg, key=lambda x: x[1])
    else:
        hottest_day_name = coolest_day_name = None
        hottest_day_avg = coolest_day_avg = None

    summary = {
        "total_orders": total_orders,
        "total_orders_per_hour": total_orders_per_hour,  # hours as keys
        "total_orders_per_weekday": total_orders_per_weekday,
        "date_range": {"start": start_date, "end": end_date},
        "hottest_hour": hottest_hour,
        "coolest_hour": coolest_hour,
        "hottest_day": {
            "weekday": hottest_day_name,
            "average_orders": (
                round(hottest_day_avg, 2) if hottest_day_avg is not None else None
            ),
        },
        "coolest_day": {
            "weekday": coolest_day_name,
            "average_orders": (
                round(coolest_day_avg, 2) if coolest_day_avg is not None else None
            ),
        },
        "avg_delivery_time": round(mean(delivery_times), 2) if delivery_times else None,
        "total_amount_collected": round(total_amount, 2),
        "avg_fare_per_order": (
            round(total_amount / total_orders, 2) if total_orders > 0 else 0
        ),
        "average_orders_per_weekday": average_orders_per_weekday_avg,
    }

    return {
        "summary": summary,
        "average_orders_per_hour": average_orders_per_hour, 
        "heatmap": heatmap,
    }
