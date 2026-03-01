# backend/normalization.py

from datetime import datetime


# -----------------------------
# DEAL BOARD COLUMN IDS
# -----------------------------
DEAL_VALUE_COL = "numeric_mm115qzd"
SECTOR_COL = "color_mm1114hs"
STAGE_COL = "color_mm1168cs"
PROBABILITY_COL = "color_mm11q7n9"
TENTATIVE_CLOSE_COL = "date_mm11kbb7"
CREATED_DATE_COL = "date_mm116tvt"
OWNER_COL = "color_mm111241"
CLIENT_CODE_COL = "dropdown_mm11cr96"


# -----------------------------
# WORK ORDER COLUMN IDS
# -----------------------------
WO_SECTOR_COL = "color_mm118xhw"
WO_EXEC_STATUS_COL = "color_mm11yr4g"
WO_DOC_TYPE_COL = "color_mm119fjx"
WO_START_DATE_COL = "date_mm11n50a"
WO_END_DATE_COL = "date_mm11p4wy"

WO_AMOUNT_EXCL = "numeric_mm11pjr5"
WO_BILLED_EXCL = "numeric_mm11dtvf"
WO_COLLECTED = "numeric_mm119t4e"
WO_RECEIVABLE = "numeric_mm118cve"


# -----------------------------
# SAFE PARSERS
# -----------------------------

def safe_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except:
        return None


def parse_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except:
        return None


def normalize_text(value):
    if not value:
        return None
    return value.strip().lower()


# -----------------------------
# DEALS NORMALIZATION
# -----------------------------

def normalize_deals(items):

    normalized = []

    for item in items:

        record = {
            "deal_name": item["name"],
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
            "missing_close_date": False
        }

        for col in item["column_values"]:

            col_id = col["id"]
            text = col["text"]

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

        # ---- Derived metrics ----
        probability_weights = {
            "high": 0.9,
            "medium": 0.6,
            "low": 0.3
        }

        if record["value"] is not None and record["probability"] in probability_weights:
            record["weighted_value"] = (
                record["value"] * probability_weights[record["probability"]]
            )

        # ---- Data quality flags ----
        if record["value"] is None:
            record["missing_value"] = True

        if record["stage"] is None:
            record["missing_stage"] = True

        if record["tentative_close_date"] is None:
            record["missing_close_date"] = True

        normalized.append(record)

    return normalized


# -----------------------------
# WORK ORDER NORMALIZATION
# -----------------------------

def normalize_work_orders(items):

    normalized = []

    for item in items:

        record = {
            "deal_name": item["name"],
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
            "missing_amount": False
        }

        for col in item["column_values"]:

            col_id = col["id"]
            text = col["text"]

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

        # ---- Derived metrics ----
        if record["amount_excl"] and record["billed_excl"] is not None:
            if record["amount_excl"] != 0:
                record["billing_ratio"] = (
                    record["billed_excl"] / record["amount_excl"]
                )

        if record["billed_excl"] and record["collected"] is not None:
            if record["billed_excl"] != 0:
                record["collection_ratio"] = (
                    record["collected"] / record["billed_excl"]
                )

        # ---- Data quality flags ----
        if record["billed_excl"] is not None and record["billed_excl"] < 0:
            record["negative_billed"] = True

        if record["receivable"] and record["receivable"] > 0:
            record["receivable_high"] = True

        if record["amount_excl"] is None:
            record["missing_amount"] = True

        normalized.append(record)

    return normalized