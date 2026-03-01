import os
import logging
import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

MONDAY_API_KEY = os.getenv("MONDAY_API_KEY", "").strip()
MONDAY_URL = "https://api.monday.com/v2"


def run_query(query: str, timeout: int = 10) -> dict:
    """Execute GraphQL query against Monday.com API with error handling."""
    headers = {
        "Authorization": MONDAY_API_KEY,
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(
            MONDAY_URL,
            json={"query": query},
            headers=headers,
            timeout=timeout
        )
    except requests.Timeout:
        logger.error(f"Monday API timeout after {timeout}s")
        raise Exception("Monday API timeout")

    if response.status_code != 200:
        logger.error(f"Monday API error {response.status_code}: {response.text[:100]}")
        raise Exception(f"Monday API error: {response.text}")

    return response.json()


def get_board_id_by_name(board_name: str) -> str:
    query = """
    query {
      boards {
        id
        name
      }
    }
    """

    data = run_query(query)

    for board in data["data"]["boards"]:
        if board["name"] == board_name:
            logger.debug(f"Found board: {board_name}")
            return board["id"]

    logger.error(f"Board not found: {board_name}")
    raise Exception(f"Board '{board_name}' not found")


def fetch_board_items(board_id: str, limit: int = 500) -> list:
    """Fetch items from board with column values."""
    query = f"""
    query {{
      boards(ids: {board_id}) {{
        items_page(limit: {limit}) {{
          items {{
            id
            name
            column_values {{
              id
              text
            }}
          }}
        }}
      }}
    }}
    """

    data = run_query(query)
    return data["data"]["boards"][0]["items_page"]["items"]