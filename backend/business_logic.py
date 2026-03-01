from datetime import datetime

def calculate_pipeline_health(deals, sector=None):
    now = datetime.now()
    current_quarter = (now.month - 1) // 3 + 1

    filtered = []

    for deal in deals:
        if sector and deal.get("sector") != sector.lower():
            continue
        filtered.append(deal)

    total_value = sum(d.get("value", 0) for d in filtered)

    stage_distribution = {}
    for d in filtered:
        stage = d.get("stage", "Unknown")
        stage_distribution[stage] = stage_distribution.get(stage, 0) + 1

    return {
        "total_pipeline_value": total_value,
        "stage_distribution": stage_distribution,
        "deal_count": len(filtered)
    }