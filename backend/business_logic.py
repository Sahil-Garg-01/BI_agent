from datetime import datetime


def map_sector(sector):
    if not sector:
        return None
    sector = sector.lower()

    if sector == "energy":
        return ["powerline", "renewables"]

    return [sector]


def filter_by_quarter(date_str, quarter_filter):
    if not date_str or not quarter_filter:
        return True

    dt = datetime.fromisoformat(date_str)

    current_q = (datetime.now().month - 1) // 3 + 1
    dt_q = (dt.month - 1) // 3 + 1

    if quarter_filter == "this_quarter":
        return dt_q == current_q and dt.year == datetime.now().year

    if quarter_filter == "last_quarter":
        last_q = current_q - 1 if current_q > 1 else 4
        year = datetime.now().year if current_q > 1 else datetime.now().year - 1
        return dt_q == last_q and dt.year == year

    return True


def analyze_pipeline_logic(deals, filters):

    sector_list = map_sector(filters.get("sector"))

    filtered = []

    for d in deals:

        if sector_list and d["sector"] not in sector_list:
            continue

        if not filter_by_quarter(d["tentative_close_date"], filters.get("quarter")):
            continue

        if filters.get("stage") and d["stage"] != filters["stage"]:
            continue

        filtered.append(d)

    total_value = sum(d["value"] for d in filtered)

    stage_dist = {}
    for d in filtered:
        stage = d["stage"] or "Unknown"
        stage_dist[stage] = stage_dist.get(stage, 0) + 1

    return {
        "deal_count": len(filtered),
        "total_value": total_value,
        "stage_distribution": stage_dist
    }


def analyze_revenue_logic(work_orders, filters):
    sector_list = map_sector(filters.get("sector"))
    filtered = []

    for w in work_orders:
        if sector_list and w["sector"] not in sector_list:
            continue
        filtered.append(w)

    total_billed = sum(w["billed_excl"] for w in filtered)
    total_collected = sum(w["collected"] for w in filtered)
    total_receivable = sum(w["receivable"] for w in filtered)

    return {
        "total_billed": total_billed,
        "total_collected": total_collected,
        "total_receivable": total_receivable
    }