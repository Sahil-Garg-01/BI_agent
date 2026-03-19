from datetime import datetime
from typing import List, Dict, Optional, Any, Union
import statistics
import logging

logger = logging.getLogger(__name__)


# =====================================================
# DOMAIN-SPECIFIC LOGIC
# =====================================================

def map_sector(sector: Optional[str]) -> Optional[List[str]]:
    """
    Map composite/alias sectors to their components.
    
    Supports queries like "energy sector" by mapping to [powerline, renewables].
    
    Args:
        sector: Sector name
        
    Returns:
        List of mapped sector names, or [sector] if no mapping
    """
    if not sector:
        return None
    
    sector = sector.lower().strip()
    
    # Composite sector mappings
    composite_mappings = {
        "energy": ["powerline", "renewables"],
        "utilities": ["powerline", "renewables"],
    }
    
    if sector in composite_mappings:
        return composite_mappings[sector]
    
    return [sector]


def filter_by_quarter(date_str: Optional[str], quarter_filter: Optional[str]) -> bool:
    """
    Filter records by quarter (this_quarter, last_quarter, or relative expressions).
    
    Supports:
    - "this_quarter": current Q
    - "last_quarter": Q-1
    - "Q1", "Q2", etc: specific quarters
    
    Args:
        date_str: ISO format date string
        quarter_filter: Quarter filter expression
        
    Returns:
        True if record matches filter
    """
    if not date_str or not quarter_filter:
        return True
    
    try:
        dt = datetime.fromisoformat(date_str)
    except (ValueError, TypeError):
        return True
    
    current_q = (datetime.now().month - 1) // 3 + 1
    dt_q = (dt.month - 1) // 3 + 1
    current_year = datetime.now().year
    
    if quarter_filter == "this_quarter":
        return dt_q == current_q and dt.year == current_year
    
    if quarter_filter == "last_quarter":
        last_q = current_q - 1 if current_q > 1 else 4
        year = current_year if current_q > 1 else current_year - 1
        return dt_q == last_q and dt.year == year
    
    # Support Q1, Q2, Q3, Q4
    if quarter_filter.startswith("Q"):
        try:
            q = int(quarter_filter[1])
            return dt_q == q and dt.year == current_year
        except (ValueError, IndexError):
            pass
    
    return True


# =====================================================
# DYNAMIC QUERY ENGINE
# =====================================================

def apply_filters(
    data: List[Dict],
    filters: Dict[str, Any],
    board_type: Optional[str] = None
) -> List[Dict]:
    """
    Apply flexible filters to data with domain-aware logic.
    
    Supports:
    - Generic field matching (exact match, case-insensitive)
    - Composite sector mapping (energy → [powerline, renewables])
    - Relative quarter filtering (this_quarter, last_quarter)
    - Multiple values: filters[key] can be string or list of strings
    
    Args:
        data: Normalized records list
        filters: Filter criteria {field: value, ...}
        board_type: "deals" or "work_orders" for domain logic
        
    Returns:
        Filtered records list
    """
    
    if not filters:
        return data
    
    filtered = []
    
    for row in data:
        match = True
        
        for key, value in filters.items():
            
            # Skip None/empty filters
            if value is None or value == "":
                continue
            
            # Domain-specific: composite sector mapping
            if key == "sector":
                sector_list = map_sector(value)
                if sector_list and row.get("sector") not in sector_list:
                    match = False
                    break
            
            # Domain-specific: quarter filtering (deals)
            elif key == "quarter" and board_type == "deals":
                if not filter_by_quarter(row.get("tentative_close_date"), value):
                    match = False
                    break
            
            # Generic field matching (case-insensitive)
            elif key in row:
                row_value = row[key]
                
                # None values don't match filters
                if row_value is None:
                    match = False
                    break
                
                # String comparison
                if isinstance(value, (list, tuple)):
                    # Multiple acceptable values
                    if str(row_value).lower() not in [str(v).lower() for v in value]:
                        match = False
                        break
                else:
                    if str(row_value).lower() != str(value).lower():
                        match = False
                        break
        
        if match:
            filtered.append(row)
    
    return filtered


def calculate_metric(data: List[Dict], metric: str) -> Optional[Union[int, float]]:
    """
    Calculate metric from data (count, sum, avg, median, max, min).
    
    Supported metrics:
    - "count" or "count()": record count
    - "sum(field)": sum of numeric values
    - "avg(field)": arithmetic mean
    - "median(field)": median value
    - "max(field)": maximum value
    - "min(field)": minimum value
    - "sum_if(field, condition)": conditional sum
    
    Args:
        data: Records list
        metric: Metric specification string
        
    Returns:
        Calculated value or None if calculation fails
    """
    
    if not data:
        return None
    
    # Count
    if metric == "count" or metric.lower() == "count()":
        return len(data)
    
    # Sum
    if metric.startswith("sum(") and metric.endswith(")"):
        field = metric[4:-1]
        total = sum(
            r.get(field) or 0
            for r in data
            if r.get(field) is not None
        )
        return total if total != 0 else None
    
    # Average
    if metric.startswith("avg(") and metric.endswith(")"):
        field = metric[4:-1]
        values = [r.get(field) for r in data if r.get(field) is not None]
        if not values:
            return None
        return sum(values) / len(values)
    
    # Median
    if metric.startswith("median(") and metric.endswith(")"):
        field = metric[7:-1]
        values = sorted([r.get(field) for r in data if r.get(field) is not None])
        if not values:
            return None
        return statistics.median(values)
    
    # Max
    if metric.startswith("max(") and metric.endswith(")"):
        field = metric[4:-1]
        values = [r.get(field) for r in data if r.get(field) is not None]
        return max(values) if values else None
    
    # Min
    if metric.startswith("min(") and metric.endswith(")"):
        field = metric[4:-1]
        values = [r.get(field) for r in data if r.get(field) is not None]
        return min(values) if values else None
    
    logger.warning(f"Unknown metric format: {metric}")
    return None


def run_dynamic_query(
    data: List[Dict],
    filters: Dict[str, Any],
    group_by: Optional[str] = None,
    metrics: Optional[List[str]] = None,
    board_type: Optional[str] = None
) -> Dict:
    """
    Execute dynamic query with filtering, grouping, and aggregation.
    
    Core query engine supporting:
    - Flexible filtering with domain-aware logic
    - Arbitrary grouping by any field
    - Multiple metric calculations simultaneously
    - Comprehensive result metadata
    
    Query result schema:
    {
        "count": int,  # Records matching filters
        "from_total": int,  # Total records in dataset
        "filters_applied": dict,  # Echo of filters used
        "grouped_by": str (optional),  # Grouping field if applicable
        "groups": dict (optional),  # {group_key: {metrics...}}
        "metric_*": value (optional),  # Ungrouped metrics
        "execution_time_ms": float  # For observability
    }
    
    Args:
        data: Normalized records from normalization layer
        filters: Filter criteria {field: value}  
        group_by: Field to group results by (optional)
        metrics: List of metric strings (optional)
        board_type: "deals" or "work_orders" for domain logic
        
    Returns:
        Query result dict with aggregated data and metadata
    """
    
    import time
    start_time = time.time()
    
    # Apply filters
    filtered_data = apply_filters(data, filters, board_type)
    
    result = {
        "count": len(filtered_data),
        "from_total": len(data),
        "filters_applied": filters,
        "execution_time_ms": 0.0
    }
    
    # If no metrics specified, return basic count
    if not metrics:
        result["execution_time_ms"] = round((time.time() - start_time) * 1000, 2)
        return result
    
    # Ungrouped aggregation
    if not group_by:
        for metric in metrics:
            metric_key = f"metric_{metric.replace('(', '_').replace(')', '')}"
            metric_value = calculate_metric(filtered_data, metric)
            result[metric_key] = metric_value
        
        result["execution_time_ms"] = round((time.time() - start_time) * 1000, 2)
        return result
    
    # Grouped aggregation
    grouped: Dict[str, List[Dict]] = {}
    
    for row in filtered_data:
        key = str(row.get(group_by, "Unknown"))
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(row)
    
    result["grouped_by"] = group_by
    result["groups"] = {}
    result["group_count"] = len(grouped)
    
    for key in sorted(grouped.keys()):
        rows = grouped[key]
        group_result = {"count": len(rows)}
        
        for metric in metrics:
            metric_key = f"metric_{metric.replace('(', '_').replace(')', '')}"
            metric_value = calculate_metric(rows, metric)
            group_result[metric_key] = metric_value
        
        result["groups"][key] = group_result
    
    result["execution_time_ms"] = round((time.time() - start_time) * 1000, 2)
    return result


def query_cross_board(
    deals_data: List[Dict],
    work_orders_data: List[Dict],
    filters: Dict[str, Any],
    metrics: Optional[List[str]] = None
) -> Dict:
    """
    Execute query across both deals and work_orders boards.
    
    Joins data on sector and displays combined insights.
    
    Args:
        deals_data: Normalized deals records
        work_orders_data: Normalized work_orders records
        filters: Filter criteria (applied to both boards)
        metrics: Metrics to calculate
        
    Returns:
        Combined result dict with insights from both boards
    """
    
    deals_filtered = apply_filters(deals_data, filters, "deals")
    wo_filtered = apply_filters(work_orders_data, filters, "work_orders")
    
    # Sector-wise join
    sector_insights = {}
    
    for deal in deals_filtered:
        sector = deal.get("sector", "Unknown")
        if sector not in sector_insights:
            sector_insights[sector] = {
                "deals_count": 0,
                "deals_value": 0,
                "work_orders_count": 0,
                "work_orders_billed": 0
            }
        sector_insights[sector]["deals_count"] += 1
        sector_insights[sector]["deals_value"] += deal.get("value") or 0
    
    for wo in wo_filtered:
        sector = wo.get("sector", "Unknown")
        if sector not in sector_insights:
            sector_insights[sector] = {
                "deals_count": 0,
                "deals_value": 0,
                "work_orders_count": 0,
                "work_orders_billed": 0
            }
        sector_insights[sector]["work_orders_count"] += 1
        sector_insights[sector]["work_orders_billed"] += wo.get("billed_excl") or 0
    
    return {
        "deals_count": len(deals_filtered),
        "work_orders_count": len(wo_filtered),
        "sector_insights": sector_insights,
        "filters_applied": filters
    }

