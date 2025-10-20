from collections import defaultdict


def driverEarnings(data, sort_by: str = "amount", descending: bool = True):

    totals = defaultdict(float)

    for order in data:
        driver = (order.get("pickup_task", {}) or {}).get("driver_name") or "Unknown"
        amt_raw = order.get("amount", 0)

        try:
            amt = abs(float(amt_raw))
        except (TypeError, ValueError):
            amt = 0.0

        totals[driver] += amt

    results = [{"driver": d, "amount": round(a, 2)} for d, a in totals.items()]

    if sort_by == "amount":
        results.sort(key=lambda x: x["amount"], reverse=descending)
    elif sort_by == "driver":
        results.sort(
            key=lambda x: (x["driver"] is None, x["driver"]), reverse=descending
        )

    return results
