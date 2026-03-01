from datetime import datetime


# DEAL BOARD IDS
DEAL_VALUE_COL = "numeric_mm115qzd"
SECTOR_COL = "color_mm1114hs"
STAGE_COL = "color_mm1168cs"
TENTATIVE_CLOSE_COL = "date_mm11kbb7"

# WORK ORDER IDS
WO_SECTOR_COL = "color_mm118xhw"
WO_AMOUNT_EXCL = "numeric_mm11pjr5"
WO_BILLED_EXCL = "numeric_mm11dtvf"
WO_COLLECTED = "numeric_mm119t4e"
WO_RECEIVABLE = "numeric_mm118cve"


def safe_float(value):
    try:
        return float(value)
    except:
        return 0.0


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
            if col["id"] == DEAL_VALUE_COL:
                record["value"] = safe_float(col["text"])

            elif col["id"] == SECTOR_COL:
                record["sector"] = col["text"].lower() if col["text"] else None

            elif col["id"] == STAGE_COL:
                record["stage"] = col["text"]

            elif col["id"] == TENTATIVE_CLOSE_COL:
                record["tentative_close_date"] = col["text"]

        normalized.append(record)

    return normalized


def normalize_work_orders(items):
    normalized = []

    for item in items:
        record = {
            "deal_name": item["name"],
            "sector": None,
            "amount_excl": 0.0,
            "billed_excl": 0.0,
            "collected": 0.0,
            "receivable": 0.0
        }

        for col in item["column_values"]:
            if col["id"] == WO_SECTOR_COL:
                record["sector"] = col["text"].lower() if col["text"] else None

            elif col["id"] == WO_AMOUNT_EXCL:
                record["amount_excl"] = safe_float(col["text"])

            elif col["id"] == WO_BILLED_EXCL:
                record["billed_excl"] = safe_float(col["text"])

            elif col["id"] == WO_COLLECTED:
                record["collected"] = safe_float(col["text"])

            elif col["id"] == WO_RECEIVABLE:
                record["receivable"] = safe_float(col["text"])

        normalized.append(record)

    return normalized