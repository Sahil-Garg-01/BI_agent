# backend/agent.py

import os
import json
from dotenv import load_dotenv
from google import genai
from google.genai import types

from .monday_client import get_board_id_by_name, fetch_board_items
from .normalization import normalize_deals, normalize_work_orders
from .business_logic import analyze_pipeline_logic, analyze_revenue_logic

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
                description="Analyze pipeline health for a given sector and time period",
                parameters={
                    "type": "object",
                    "properties": {
                        "sector": {"type": "string"},
                        "quarter": {"type": "string"}
                    },
                    "required": ["sector"]
                }
            ),
            types.FunctionDeclaration(
                name="analyze_revenue",
                description="Analyze revenue health for a given sector",
                parameters={
                    "type": "object",
                    "properties": {
                        "sector": {"type": "string"}
                    },
                    "required": ["sector"]
                }
            )
        ]
    )
]


# -------- TOOL EXECUTION --------

def execute_tool(tool_name, args, trace):

    if tool_name == "analyze_pipeline":
        sector = args.get("sector")
        quarter = args.get("quarter", "all")

        trace.append(f"Fetching Deals board for sector: {sector}")

        board_id = get_board_id_by_name("Deal funnel Data")
        items = fetch_board_items(board_id)
        deals = normalize_deals(items)

        result = analyze_pipeline_logic(deals, sector, quarter)

        trace.append("Pipeline metrics calculated")
        return result


    elif tool_name == "analyze_revenue":
        sector = args.get("sector")

        trace.append(f"Fetching Work Orders board for sector: {sector}")

        board_id = get_board_id_by_name("Work_Order_Tracker Data")
        items = fetch_board_items(board_id)
        work_orders = normalize_work_orders(items)

        result = analyze_revenue_logic(work_orders, sector)

        trace.append("Revenue metrics calculated")
        return result


    return {}


# -------- AGENT LOOP --------

def run_agent(user_query: str):

    trace = []

    history = [
        {
            "role": "user",
            "parts": [{"text": user_query}]
        }
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

        # If tool calls exist
        if response.function_calls:

            for fc in response.function_calls:

                tool_name = fc.name
                args = fc.args

                # Append model tool call
                history.append({
                    "role": "model",
                    "parts": [{
                        "function_call": {
                            "name": tool_name,
                            "args": args
                        }
                    }]
                })

                result = execute_tool(tool_name, args, trace)

                # Append tool response as user message (valid for google.genai SDK)
                history.append({
                    "role": "user",
                    "parts": [{
                        "function_response": {
                            "name": tool_name,
                            "response": result
                        }
                    }]
                })

            continue

        break

    # Extract final text
    if response.candidates and response.candidates[0].content.parts:
        text_parts = [
            p.text for p in response.candidates[0].content.parts
            if hasattr(p, "text") and p.text
        ]
        final_text = "\n".join(text_parts)
    else:
        final_text = "No meaningful response generated."

    return final_text, trace