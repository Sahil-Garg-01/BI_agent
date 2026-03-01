from datetime import datetime


def map_sector(sector):
    sector = sector.lower()

    if sector == "energy":
        return ["powerline", "renewables"]

    return [sector]


def filter_this_quarter(deals):
    now = datetime.now()
    current_q = (now.month - 1) // 3 + 1
    year = now.year

    filtered = []

    for d in deals:
        if d["tentative_close_date"]:
            dt = datetime.fromisoformat(d["tentative_close_date"])
            dq = (dt.month - 1) // 3 + 1

            if dq == current_q and dt.year == year:
                filtered.append(d)

    return filtered


def analyze_pipeline_logic(deals, sector, quarter):
    sectors = map_sector(sector)
    deals = [d for d in deals if d["sector"] in sectors]

    if quarter == "this_quarter":
        deals = filter_this_quarter(deals)

    total_value = sum(d["value"] for d in deals)

    stage_dist = {}
    for d in deals:
        stage = d["stage"] or "Unknown"
        stage_dist[stage] = stage_dist.get(stage, 0) + 1

    return {
        "sector": sector,
        "deal_count": len(deals),
        "total_value": total_value,
        "stage_distribution": stage_dist
    }


def analyze_revenue_logic(work_orders, sector):
    sectors = map_sector(sector)
    work_orders = [w for w in work_orders if w["sector"] in sectors]

    total_billed = sum(w["billed_excl"] for w in work_orders)
    total_collected = sum(w["collected"] for w in work_orders)
    total_receivable = sum(w["receivable"] for w in work_orders)

    return {
        "sector": sector,
        "total_billed": total_billed,
        "total_collected": total_collected,
        "total_receivable": total_receivable
    }