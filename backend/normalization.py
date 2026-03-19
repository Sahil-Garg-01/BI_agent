# backend/normalization.py

from datetime import datetime
from typing import List, Dict, Optional, Any


# =====================================================
# COLUMN ID MAPPINGS
# =====================================================

# DEAL BOARD COLUMN IDS
DEAL_VALUE_COL = "numeric_mm115qzd"
SECTOR_COL = "color_mm1114hs"
STAGE_COL = "color_mm1168cs"
PROBABILITY_COL = "color_mm11q7n9"
TENTATIVE_CLOSE_COL = "date_mm11kbb7"
CREATED_DATE_COL = "date_mm116tvt"
OWNER_COL = "color_mm111241"
CLIENT_CODE_COL = "dropdown_mm11cr96"

# WORK ORDER COLUMN IDS
WO_SECTOR_COL = "color_mm118xhw"
WO_EXEC_STATUS_COL = "color_mm11yr4g"
WO_DOC_TYPE_COL = "color_mm119fjx"
WO_START_DATE_COL = "date_mm11n50a"
WO_END_DATE_COL = "date_mm11p4wy"
WO_AMOUNT_EXCL = "numeric_mm11pjr5"
WO_BILLED_EXCL = "numeric_mm11dtvf"
WO_COLLECTED = "numeric_mm119t4e"
WO_RECEIVABLE = "numeric_mm118cve"


# =====================================================
# SAFE PARSERS WITH ERROR HANDLING
# =====================================================

def safe_float(value: Any) -> Optional[float]:
    """
    Safely convert value to float, returning None on error.
    
    Handles:
    - None/empty strings
    - Non-numeric strings
    - Already numeric values
    
    Args:
        value: Value to convert
        
    Returns:
        Float value or None if conversion fails
    """
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_date(value: Any) -> Optional[datetime]:
    """
    Safely parse ISO format date strings.
    
    Args:
        value: ISO format date string
        
    Returns:
        datetime object or None if parsing fails
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None


def normalize_text(value: Any) -> Optional[str]:
    """
    Normalize text: strip whitespace and convert to lowercase.
    
    Args:
        value: Text value
        
    Returns:
        Normalized string or None
    """
    if not value:
        return None
    try:
        return str(value).strip().lower()
    except:
        return None


def calculate_data_quality_score(record: Dict) -> float:
    """
    Calculate data quality score (0.0 to 1.0) based on completeness.
    
    Considers mandatory vs optional fields.
    
    Args:
        record: Normalized record
        
    Returns:
        Quality score between 0 and 1
    """
    mandatory_fields = ["value", "stage", "tentative_close_date"]
    present = sum(1 for f in mandatory_fields if record.get(f) is not None)
    return present / len(mandatory_fields)


# =====================================================
# DEALS NORMALIZATION
# =====================================================

def normalize_deals(items: List[Dict]) -> List[Dict]:
    """
    Normalize raw deals from Monday.com.
    
    Transforms Monday.com board items into clean records with:
    - Extracted and typed column values
    - Computed weighted values based on probability
    - Data quality flags for tracking inconsistencies
    - Filtering recommendations based on data state
    
    Output record schema:
    {
        deal_name: str,
        owner: Optional[str],
        client_code: Optional[str],
        sector: Optional[str],
        stage: Optional[str],
        probability: Optional[str],  # high, medium, low
        value: Optional[float],
        tentative_close_date: Optional[str],
        created_date: Optional[str],
        weighted_value: Optional[float],  # value * probability_weight
        
        # Data quality flags
        missing_value: bool,
        missing_stage: bool,
        missing_close_date: bool,
        quality_score: float,  # 0-1: mandatory field presence
        data_caveat: Optional[str]  # Human-readable warning
    }
    
    Args:
        items: Raw items from Monday.com board
        
    Returns:
        List of normalized deal records
    """
    
    normalized: List[Dict] = []
    
    for item in items:
        
        record: Dict[str, Any] = {
            "deal_name": item.get("name", "Unnamed"),
            "owner": None,
            "client_code": None,
            "sector": None,
            "stage": None,
            "probability": None,
            "value": None,
            "tentative_close_date": None,
            "created_date": None,
            
            # Derived
            "weighted_value": None,
            
            # Data quality flags
            "missing_value": False,
            "missing_stage": False,
            "missing_close_date": False,
            "quality_score": 0.0,
            "data_caveat": None
        }
        
        # Extract column values
        for col in item.get("column_values", []):
            
            col_id = col.get("id")
            text = col.get("text")
            
            if col_id == DEAL_VALUE_COL:
                record["value"] = safe_float(text)
            elif col_id == SECTOR_COL:
                record["sector"] = normalize_text(text)
            elif col_id == STAGE_COL:
                record["stage"] = normalize_text(text)
            elif col_id == PROBABILITY_COL:
                record["probability"] = normalize_text(text)
            elif col_id == TENTATIVE_CLOSE_COL:
                record["tentative_close_date"] = text if text else None
            elif col_id == CREATED_DATE_COL:
                record["created_date"] = text if text else None
            elif col_id == OWNER_COL:
                record["owner"] = normalize_text(text)
            elif col_id == CLIENT_CODE_COL:
                record["client_code"] = normalize_text(text)
        
        # DERIVED: Weighted value
        probability_weights = {
            "high": 0.9,
            "medium": 0.6,
            "low": 0.3
        }
        
        if record["value"] is not None and record["probability"] in probability_weights:
            record["weighted_value"] = (
                record["value"] * probability_weights[record["probability"]]
            )
        
        # DATA QUALITY FLAGS
        if record["value"] is None:
            record["missing_value"] = True
        
        if record["stage"] is None:
            record["missing_stage"] = True
        
        if record["tentative_close_date"] is None:
            record["missing_close_date"] = True
        
        # Quality score
        mandatory = ["value", "stage", "tentative_close_date"]
        present = sum(1 for f in mandatory if record.get(f) is not None)
        record["quality_score"] = present / len(mandatory)
        
        # Caveat for low quality
        caveats = []
        if record["missing_value"]:
            caveats.append("no value")
        if record["missing_stage"]:
            caveats.append("no stage")
        if record["missing_close_date"]:
            caveats.append("no close date")
        
        if caveats:
            record["data_caveat"] = f"Warning: {', '.join(caveats)}"
        
        normalized.append(record)
    
    return normalized


# =====================================================
# WORK ORDER NORMALIZATION
# =====================================================

def normalize_work_orders(items: List[Dict]) -> List[Dict]:
    """
    Normalize raw work orders from Monday.com.
    
    Transforms Monday.com board items into clean records with:
    - Extracted and typed column values
    - Computed billing and collection ratios
    - Data quality flags for financial anomalies
    
    Output record schema:
    {
        deal_name: str,
        sector: Optional[str],
        execution_status: Optional[str],
        document_type: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
        
        # Financial fields (all Optional[float])
        amount_excl: Optional[float],
        billed_excl: Optional[float],
        collected: Optional[float],
        receivable: Optional[float],
        
        # Derived metrics
        billing_ratio: Optional[float],  # billed / amount
        collection_ratio: Optional[float],  # collected / billed
        
        # Data quality flags
        negative_billed: bool,
        receivable_high: bool,
        missing_amount: bool,
        quality_score: float,  # 0-1: field presence
        data_caveat: Optional[str]  # Human-readable warning
    }
    
    Args:
        items: Raw items from Monday.com work_orders board
        
    Returns:
        List of normalized work order records
    """
    
    normalized: List[Dict] = []
    
    for item in items:
        
        record: Dict[str, Any] = {
            "deal_name": item.get("name", "Unnamed"),
            "sector": None,
            "execution_status": None,
            "document_type": None,
            "start_date": None,
            "end_date": None,
            
            "amount_excl": None,
            "billed_excl": None,
            "collected": None,
            "receivable": None,
            
            # Derived
            "billing_ratio": None,
            "collection_ratio": None,
            
            # Data quality flags
            "negative_billed": False,
            "receivable_high": False,
            "missing_amount": False,
            "quality_score": 0.0,
            "data_caveat": None
        }
        
        # Extract column values
        for col in item.get("column_values", []):
            
            col_id = col.get("id")
            text = col.get("text")
            
            if col_id == WO_SECTOR_COL:
                record["sector"] = normalize_text(text)
            elif col_id == WO_EXEC_STATUS_COL:
                record["execution_status"] = normalize_text(text)
            elif col_id == WO_DOC_TYPE_COL:
                record["document_type"] = normalize_text(text)
            elif col_id == WO_START_DATE_COL:
                record["start_date"] = text if text else None
            elif col_id == WO_END_DATE_COL:
                record["end_date"] = text if text else None
            elif col_id == WO_AMOUNT_EXCL:
                record["amount_excl"] = safe_float(text)
            elif col_id == WO_BILLED_EXCL:
                record["billed_excl"] = safe_float(text)
            elif col_id == WO_COLLECTED:
                record["collected"] = safe_float(text)
            elif col_id == WO_RECEIVABLE:
                record["receivable"] = safe_float(text)
        
        # DERIVED: Billing ratio
        if record["amount_excl"] and record["amount_excl"] != 0 and record["billed_excl"] is not None:
            record["billing_ratio"] = record["billed_excl"] / record["amount_excl"]
        
        # DERIVED: Collection ratio
        if record["billed_excl"] and record["billed_excl"] != 0 and record["collected"] is not None:
            record["collection_ratio"] = record["collected"] / record["billed_excl"]
        
        # DATA QUALITY FLAGS
        if record["billed_excl"] is not None and record["billed_excl"] < 0:
            record["negative_billed"] = True
        
        if record["receivable"] and record["receivable"] > 0:
            record["receivable_high"] = True
        
        if record["amount_excl"] is None:
            record["missing_amount"] = True
        
        # Quality score
        mandatory = ["amount_excl", "billed_excl", "collected"]
        present = sum(1 for f in mandatory if record.get(f) is not None)
        record["quality_score"] = present / len(mandatory)
        
        # Caveat for issues
        caveats = []
        if record["missing_amount"]:
            caveats.append("no amount")
        if record["negative_billed"]:
            caveats.append("negative billed amount")
        if record["receivable_high"]:
            caveats.append("high outstanding receivables")
        
        if caveats:
            record["data_caveat"] = f"Warning: {', '.join(caveats)}"
        
        normalized.append(record)
    
    return normalized


def summarize_data_quality(records: List[Dict], board_type: str = "deals") -> Dict:
    """
    Generate data quality summary for all records.
    
    Args:
        records: List of normalized records
        board_type: "deals" or "work_orders"
        
    Returns:
        Quality summary dict with statistics
    """
    
    if not records:
        return {
            "total_records": 0,
            "avg_quality_score": 0.0,
            "records_with_caveats": 0,
            "caveat_types": {}
        }
    
    total = len(records)
    avg_quality = sum(r.get("quality_score", 0) for r in records) / total
    caveats_count = sum(1 for r in records if r.get("data_caveat"))
    
    caveat_types = {}
    for r in records:
        if r.get("data_caveat"):
            warnings = r["data_caveat"].replace("Warning: ", "").split(", ")
            for w in warnings:
                caveat_types[w] = caveat_types.get(w, 0) + 1
    
    return {
        "total_records": total,
        "avg_quality_score": round(avg_quality, 2),
        "records_with_caveats": caveats_count,
        "caveat_types": caveat_types,
        "board_type": board_type
    }