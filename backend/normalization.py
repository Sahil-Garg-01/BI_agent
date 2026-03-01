# backend/normalization.py

from datetime import datetime


# -------- REAL DEAL FUNNEL COLUMN IDS --------
DEAL_VALUE_COL = "numeric_mm115qzd"
SECTOR_COL = "color_mm1114hs"
STAGE_COL = "color_mm1168cs"
TENTATIVE_CLOSE_COL = "date_mm11kbb7"


def safe_float(value):
    try:
        return float(value)
    except:
        return 0.0


def parse_date(value):
    try:
        return datetime.fromisoformat(value)
    except:
        return None


def normalize_deals(items):
    normalized = []

    for item in items:
        record = {
            "deal_name": item["name"],
            "sector": None,
            "value": 0.0,
            "stage": None,
            "tentative_close_date": None
        }

        for col in item["column_values"]:
            col_id = col["id"]
            text = col["text"]

            if col_id == DEAL_VALUE_COL:
                record["value"] = safe_float(text)

            elif col_id == SECTOR_COL:
                record["sector"] = text.strip().lower() if text else None

            elif col_id == STAGE_COL:
                record["stage"] = text

            elif col_id == TENTATIVE_CLOSE_COL:
                parsed_date = parse_date(text)
                record["tentative_close_date"] = (
                    parsed_date.isoformat() if parsed_date else None
                )

        normalized.append(record)

    return normalized