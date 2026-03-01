# backend/agent.py

import os
import json
import google.genai as genai
from google.genai import types
from dotenv import load_dotenv
from .monday_client import get_board_id_by_name, fetch_board_items
from .normalization import normalize_deals

load_dotenv()

client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

system_prompt = """
You are a business intelligence AI agent for Skylark Drones.
Answer founder-level business questions.
Use tools when necessary.
If multiple sectors relate to a question (e.g., energy),
analyze each and then combine insights before answering.
Provide executive-level insights, not raw data.
"""

def safe_extract_text(response):
    if response.candidates and response.candidates[0].content.parts:
        text_parts = [
            p.text for p in response.candidates[0].content.parts
            if hasattr(p, "text") and p.text
        ]
        return "\n".join(text_parts) if text_parts else "No meaningful response generated."
    else:
        return "No meaningful response generated."

# ---- TOOL DEFINITIONS ----

tools = [
    types.Tool(
        function_declarations=[
            types.FunctionDeclaration(
                name="analyze_pipeline",
                description="Analyze pipeline health for a given sector and time period",
                parameters=types.Schema(
                    type="object",
                    properties={
                        "sector": types.Schema(type="string", description="Sector name like mining, powerline, renewables"),
                        "quarter": types.Schema(type="string", description="Time period like this_quarter, next_quarter, or all")
                    },
                    required=["sector"]
                )
            )
        ]
    )
]


# ---- TOOL EXECUTION ----

from datetime import datetime

def get_current_quarter():
    now = datetime.now()
    return (now.month - 1) // 3 + 1, now.year


def execute_tool(tool_name, arguments, trace):
    if tool_name == "analyze_pipeline":
        sector = arguments.get("sector", "").lower()
        quarter_filter = arguments.get("quarter", "all")

        trace.append(f"Calling Monday API for sector: {sector}")

        board_id = get_board_id_by_name("Deal funnel Data")
        items = fetch_board_items(board_id)
        deals = normalize_deals(items)

        trace.append(f"Fetched {len(deals)} deal records")
        available_sectors = set(d.get("sector") for d in deals if d.get("sector"))
        trace.append(f"Available sectors in data: {available_sectors}")

        # FILTER BY SECTOR
        deals = [d for d in deals if d.get("sector") == sector]

        # FILTER BY QUARTER
        if quarter_filter == "this_quarter":
            q, year = get_current_quarter()

            filtered = []
            for d in deals:
                date_str = d.get("tentative_close_date")
                if date_str:
                    date_obj = datetime.fromisoformat(date_str)
                    deal_q = (date_obj.month - 1) // 3 + 1
                    if deal_q == q and date_obj.year == year:
                        filtered.append(d)

            deals = filtered

        total_value = sum(d.get("value", 0) for d in deals)

        stage_distribution = {}
        for d in deals:
            stage = d.get("stage") or "Unknown"
            stage_distribution[stage] = stage_distribution.get(stage, 0) + 1

        result = {
            "sector": sector,
            "deal_count": len(deals),
            "total_pipeline_value": total_value,
            "stage_distribution": stage_distribution
        }

        trace.append("Pipeline metrics calculated")

        return result

    return None


# ---- AGENT CORE ----

def run_agent(user_query: str):
    trace = []

    history = [
        types.Content(role="user", parts=[types.Part(text=user_query)])
    ]

    while True:
        response = client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=history,
            config=types.GenerateContentConfig(
                tools=tools,
                system_instruction=system_prompt
            )
        )

        if response.function_calls:
            # Append model response
            history.append(response.candidates[0].content)

            for function_call in response.function_calls:
                tool_name = function_call.name
                arguments = function_call.args

                tool_result = execute_tool(tool_name, arguments, trace)

                function_response_part = types.Part.from_function_response(
                    name=tool_name,
                    response=tool_result
                )
                history.append(types.Content(role="tool", parts=[function_response_part]))
        else:
            # No tool call, return the text response
            return safe_extract_text(response), trace