from datetime import datetime


# ===== DOMAIN-SPECIFIC LOGIC =====

def map_sector(sector):
    """Map composite sectors to their components."""
    if not sector:
        return None
    sector = sector.lower()

    if sector == "energy":
        return ["powerline", "renewables"]

    return [sector]


def filter_by_quarter(date_str: str, quarter_filter: str) -> bool:
    """Filter records by quarter (this_quarter, last_quarter)."""
    if not date_str or not quarter_filter:
        return True

    try:
        dt = datetime.fromisoformat(date_str)
    except (ValueError, TypeError):
        return True

    current_q = (datetime.now().month - 1) // 3 + 1
    dt_q = (dt.month - 1) // 3 + 1

    if quarter_filter == "this_quarter":
        return dt_q == current_q and dt.year == datetime.now().year

    if quarter_filter == "last_quarter":
        last_q = current_q - 1 if current_q > 1 else 4
        year = datetime.now().year if current_q > 1 else datetime.now().year - 1
        return dt_q == last_q and dt.year == year

    return True


# ===== DYNAMIC QUERY ENGINE =====

def apply_filters(data: list, filters: dict, board_type: str = None) -> list:
    """
    Apply flexible filters to data with domain-aware logic.
    
    Supports generic field matching plus domain intelligence:
    - Sector: composite sector mapping (energy → [powerline, renewables])
    - Quarter: relative date filtering (this_quarter, last_quarter)
    
    Args:
        data: Normalized records list
        filters: Filter criteria {sector, stage, quarter, ...}
        board_type: \"deals\" or \"work_orders\" for domain logic
        
    Returns:
        Filtered records list
    """
    
    if not filters:
        return data

    filtered = []

    for row in data:
        match = True

        for key, value in filters.items():
            
            # Skip empty filters
            if not value:
                continue
            
            # Domain-specific: composite sector mapping
            if key == "sector":
                sector_list = map_sector(value)
                if sector_list and row.get("sector") not in sector_list:
                    match = False
                    break
            
            # Domain-specific: quarter filtering
            elif key == "quarter" and board_type == "deals":
                if not filter_by_quarter(row.get("tentative_close_date"), value):
                    match = False
                    break
            
            # Generic field matching
            elif key in row:
                if row[key] is None:
                    match = False
                    break

                if str(row[key]).lower() != str(value).lower():
                    match = False
                    break

        if match:
            filtered.append(row)

    return filtered


def calculate_metric(data: list, metric: str) -> float:
    """
    Calculate metric from data (sum, avg, min, max, count).
    
    Parses metric string and performs aggregation:
    - \"count()\" → record count
    - \"sum(field)\" → sum of field values
    - \"avg(field)\" → average of field values
    - \"max(field)\" → maximum value
    - \"min(field)\" → minimum value
    
    Args:
        data: Records list
        metric: Metric string specification
        
    Returns:
        Calculated numeric value or None
    """
    
    if metric == "count" or metric.lower().startswith("count"):
        return len(data)
    
    if metric.startswith("sum(") and metric.endswith(")"):
        field = metric[4:-1]
        return sum(
            r.get(field) or 0
            for r in data
            if r.get(field) is not None
        )
    
    if metric.startswith("avg(") and metric.endswith(")"):
        field = metric[4:-1]
        values = [r.get(field) for r in data if r.get(field) is not None]
        return sum(values) / len(values) if values else 0
    
    if metric.startswith("max(") and metric.endswith(")"):
        field = metric[4:-1]
        values = [r.get(field) for r in data if r.get(field) is not None]
        return max(values) if values else None
    
    if metric.startswith("min(") and metric.endswith(")"):
        field = metric[4:-1]
        values = [r.get(field) for r in data if r.get(field) is not None]
        return min(values) if values else None
    
    return None


def run_dynamic_query(data: list, filters: dict, group_by: str = None, metrics: list = None, board_type: str = None) -> dict:
    """
    Execute dynamic query with filtering, grouping, and aggregation.
    
    Core query engine supporting:
    - Filtering by any field with domain-aware logic
    - Grouping by any field (sector, stage, owner, etc.)
    - Dynamic metric calculation (sum, avg, min, max)
    
    Args:
        data: Normalized records
        filters: Filter criteria dict
        group_by: Field to group by (optional)
        metrics: List of metrics to calculate (optional)
        board_type: \"deals\" or \"work_orders\"
        
    Returns:
        Query result with count, filters_applied, and grouped metrics
    """
    
    # Apply filters
    filtered_data = apply_filters(data, filters, board_type)
    
    result = {
        "count": len(filtered_data),
        "from_total": len(data),
        "filters_applied": filters
    }
    
    # If no metrics specified, return basic count
    if not metrics:
        return result
    
    # Ungrouped aggregation
    if not group_by:
        for metric in metrics:
            metric_value = calculate_metric(filtered_data, metric)
            result[f"metric_{metric}"] = metric_value
        
        return result
    
    # Grouped aggregation
    grouped = {}
    for row in filtered_data:
        key = row.get(group_by, "Unknown")
        grouped.setdefault(key, []).append(row)
    
    result["grouped_by"] = group_by
    result["groups"] = {}
    
    for key, rows in grouped.items():
        group_result = {"count": len(rows)}
        
        for metric in metrics:
            metric_value = calculate_metric(rows, metric)
            group_result[f"metric_{metric}"] = metric_value
        
        result["groups"][key] = group_result
    
    return result

