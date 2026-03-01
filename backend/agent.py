# backend/agent.py

import os
import json
import logging
from dotenv import load_dotenv
from google import genai
from google.genai import types

from .monday_client import get_board_id_by_name, fetch_board_items
from .normalization import normalize_deals, normalize_work_orders
from .business_logic import analyze_pipeline_logic, analyze_revenue_logic

logger = logging.getLogger(__name__)

load_dotenv()

client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

SYSTEM_PROMPT = """
You are a Business Intelligence AI agent for Skylark Drones.

Guidelines:
- Always use tools for business data.
- Provide executive-level insights.
- If sector like "energy" is asked, it may include powerline and renewables.
- Highlight risks, revenue gaps, and stage maturity.
- Do not dump raw data.
"""


# -------- TOOL DEFINITIONS --------

tools = [
    types.Tool(
        function_declarations=[
            types.FunctionDeclaration(
                name="analyze_pipeline",
                description="Analyze pipeline health",
                parameters={
                    "type": "object",
                    "properties": {
                        "sector": {"type": "string"},
                        "quarter": {"type": "string"},
                        "stage": {"type": "string"}
                    }
                }
            ),
            types.FunctionDeclaration(
                name="analyze_revenue",
                description="Analyze revenue health",
                parameters={
                    "type": "object",
                    "properties": {
                        "sector": {"type": "string"},
                        "quarter": {"type": "string"}
                    }
                }
            )
        ]
    )
]


def execute_tool(tool_name, args, context, trace):

    # Merge with previous filters
    context.setdefault("filters", {})
    context["filters"].update({k: v for k, v in args.items() if v})

    filters = context["filters"]

    if tool_name == "analyze_pipeline":

        logger.info(f"filters: {filters}")
        trace.append(f"Fetching Deals board with filters: {filters}")

        board_id = get_board_id_by_name("Deal funnel Data")
        items = fetch_board_items(board_id)
        deals = normalize_deals(items)
        logger.debug(f"Fetched {len(deals)} deals")

        result = analyze_pipeline_logic(deals, filters)
        logger.info(f"Pipeline result: {result}")

        trace.append("Pipeline metrics calculated")
        return result

    elif tool_name == "analyze_revenue":

        trace.append(f"Fetching Work Orders board with filters: {filters}")
        logger.info(f"filters: {filters}")

        board_id = get_board_id_by_name("Work_Order_Tracker Data")
        items = fetch_board_items(board_id)
        work_orders = normalize_work_orders(items)

        result = analyze_revenue_logic(work_orders, filters)
        logger.info(f"Revenue result: {result}")

        trace.append("Revenue metrics calculated")
        return result

    return {}


def run_agent(user_query: str, context: dict):

    logger.info(f"Running agent with query: {user_query[:50]}...")
    trace = []

    history = [
        {"role": "user", "parts": [{"text": user_query}]}
    ]

    while True:

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

                history.append({
                    "role": "model",
                    "parts": [{
                        "function_call": {
                            "name": fc.name,
                            "args": fc.args
                        }
                    }]
                })

                result = execute_tool(fc.name, fc.args, context, trace)

                history.append({
                    "role": "user",
                    "parts": [{
                        "text": json.dumps(result)
                    }]
                })

            continue

        break

    final_text = response.text or "No meaningful response generated."

    return final_text, trace, context