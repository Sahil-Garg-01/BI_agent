# backend/agent.py

import os
import json
import logging
from dotenv import load_dotenv
from google import genai
from google.genai import types

from .monday_client import get_board_id_by_name, fetch_board_items
from .normalization import normalize_deals, normalize_work_orders
from .business_logic import run_dynamic_query

logger = logging.getLogger(__name__)

load_dotenv()

client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

SYSTEM_PROMPT = """
You are a Business Intelligence AI Agent for Skylark Drones.

You answer executive-level business questions using LIVE monday.com data.

Rules:
- Always use the query_data tool to fetch and analyze data.
- Maintain conversational filters across turns (context-aware).
- If required information is missing (e.g., sector), ask for clarification.
- Support filtering by sector, stage, quarter, and other fields.
- Support grouping (group_by) and dynamic metrics calculation.
- Do not output raw JSON; provide concise executive summaries.

Response Format:
1. Brief Summary
2. Key Metrics (with numbers)
3. Observations
4. Risks or Opportunities
"""


# -------- TOOL DEFINITIONS --------

tools = [
    types.Tool(
        function_declarations=[
            types.FunctionDeclaration(
                name="query_data",
                description="Run dynamic queries on deals or work_orders with filtering, grouping, and metrics",
                parameters={
                    "type": "object",
                    "properties": {
                        "board": {
                            "type": "string",
                            "enum": ["deals", "work_orders"],
                            "description": "Which board to query"
                        },
                        "filters": {
                            "type": "object",
                            "description": "Filter criteria (e.g., {\"sector\": \"renewables\", \"stage\": \"proposal\"})"
                        },
                        "group_by": {
                            "type": "string",
                            "description": "Field to group results by (e.g., 'sector', 'stage', 'quarter')"
                        },
                        "metrics": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Metrics to calculate (e.g., ['sum(value)', 'sum(billed_excl)'])"
                        }
                    },
                    "required": ["board"]
                }
            )
        ]
    )
]


def execute_tool(args: dict, context: dict, trace: list) -> dict:
    """Execute dynamic query tool with filter context management."""
    
    # Merge new filters with persistent context
    context.setdefault("filters", {})
    context["filters"].update({k: v for k, v in args.get("filters", {}).items() if v})

    board = args.get("board")
    filters = context["filters"]
    group_by = args.get("group_by")
    metrics = args.get("metrics", [])

    logger.info(f"Executing query: board={board}, filters={filters}, group_by={group_by}, metrics={metrics}")
    trace.append(f"📊 Querying {board} with filters: {filters}")

    try:
        if board == "deals":
            board_id = get_board_id_by_name("Deal funnel Data")
            items = fetch_board_items(board_id)
            data = normalize_deals(items)
            logger.debug(f"Fetched {len(data)} deals")

        elif board == "work_orders":
            board_id = get_board_id_by_name("Work_Order_Tracker Data")
            items = fetch_board_items(board_id)
            data = normalize_work_orders(items)
            logger.debug(f"Fetched {len(data)} work orders")

        else:
            logger.error(f"Invalid board: {board}")
            return {"error": f"Invalid board: {board}. Must be 'deals' or 'work_orders'."}

        result = run_dynamic_query(data, filters, group_by, metrics, board)
        logger.info(f"Query result: {result}")
        trace.append("✅ Data processed successfully")
        
        return result

    except Exception as e:
        logger.error(f"Tool execution error: {str(e)}", exc_info=True)
        return {"error": str(e)}


def run_agent(user_query: str, context: dict) -> tuple:
    """Run multi-turn agent loop with dynamic queries."""
    
    logger.info(f"Starting agent with query: {user_query[:60]}...")
    trace = []

    history = [
        {"role": "user", "parts": [{"text": user_query}]}
    ]

    iteration = 0
    max_iterations = 5

    while iteration < max_iterations:
        iteration += 1

        logger.debug(f"Agent iteration {iteration}")
        
        response = client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=history,
            config=types.GenerateContentConfig(
                tools=tools,
                system_instruction=SYSTEM_PROMPT
            )
        )

        if response.function_calls:

            for fc in response.function_calls:
                logger.info(f"Tool called: {fc.name}")
                
                history.append({
                    "role": "model",
                    "parts": [{
                        "function_call": {
                            "name": fc.name,
                            "args": fc.args
                        }
                    }]
                })

                result = execute_tool(fc.args, context, trace)

                history.append({
                    "role": "user",
                    "parts": [{
                        "text": json.dumps(result)
                    }]
                })

            continue

        break

    final_text = response.text or "No meaningful response generated."
    logger.info(f"Agent response generated ({len(final_text)} chars)")

    return final_text, trace, context
