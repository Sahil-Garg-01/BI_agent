import os
import requests
from dotenv import load_dotenv

load_dotenv()

MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
MONDAY_URL = "https://api.monday.com/v2"


def run_query(query: str):
    headers = {
        "Authorization": MONDAY_API_KEY,
        "Content-Type": "application/json"
    }

    response = requests.post(
        MONDAY_URL,
        json={"query": query},
        headers=headers
    )

    if response.status_code != 200:
        raise Exception(f"Monday API error: {response.text}")

    return response.json()


def get_board_id_by_name(board_name: str):
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
            return board["id"]

    raise Exception(f"Board '{board_name}' not found")


def fetch_board_items(board_id: int):
    query = f"""
    query {{
      boards(ids: {board_id}) {{
        items_page(limit: 500) {{
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