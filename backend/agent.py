# backend/agent.py

import os
import json
import logging
import time
from typing import Tuple, Dict, List, Any, Optional
from dotenv import load_dotenv
from google import genai
from google.genai import types

from .monday_client import (
    get_board_id_by_name,
    fetch_board_items,
    MondayAPIError,
)
from .normalization import (
    normalize_deals,
    normalize_work_orders,
    summarize_data_quality,
)
from .business_logic import run_dynamic_query, query_cross_board

logger = logging.getLogger(__name__)

load_dotenv()

client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY", "").strip())

# =====================================================
# SYSTEM PROMPTS
# =====================================================

SYSTEM_PROMPT = """
You are an executive-level BI Agent for Skylark Drones.

GOAL: Provide founder-level insights with complete transparency.

Core Rules:
1. ALWAYS use query_data tool to fetch live data. No assumptions.
2. Maintain conversational filters (sector, stage, quarter persist).
3. If critical info missing, explicitly ask for clarification.
4. Support filtering by: sector, stage, quarter, owner, status.
5. Provide metrics: sum, average, count, grouping.
6. Never output raw JSON; provide executive summaries.

Response Format (Always):
1. **Brief Summary** - What the data shows
2. **Key Metrics** - Numbers with context (e.g., "Total: $2.5M (+15% vs Q1")
3. **Observations** - Patterns, risks, opportunities
4. **Data Notes** - Any quality caveats or missing information

Data Quality Transparency:
- If <50% of records have critical fields, flag it
- Mention missing values that affect reliability
- Use "based on available data" when partial dataset used
"""

CLARIFICATION_PROMPT = """
I need clarification to find the right data:

The query was ambiguous. Could you provide:
- Sector filter? (e.g., renewables, powerline, energy)
- Stage filter? (e.g., proposal, negotiation, closed)
- Time period? (e.g., this quarter, last quarter)
- Metrics focus? (e.g., deal count, total value, probability-weighted)

Give me any of these details to refine the analysis.
"""


# =====================================================
# TOOL DEFINITIONS
# =====================================================

tools = [
    types.Tool(
        function_declarations=[
            types.FunctionDeclaration(
                name="query_data",
                description="Query deals or work_orders board with flexible filtering, grouping, and metric aggregation. Supports dynamic field selection and cross-board joins.",
                parameters={
                    "type": "object",
                    "properties": {
                        "board": {
                            "type": "string",
                            "enum": ["deals", "work_orders", "cross_board"],
                            "description": "Data source: 'deals' (pipeline), 'work_orders' (execution), or 'cross_board' (sector-wise analysis)"
                        },
                        "filters": {
                            "type": "object",
                            "description": "Filter records: {sector, stage, quarter, owner, status, ...}. Empty object = no filters."
                        },
                        "group_by": {
                            "type": "string",
                            "description": "Group results by field (e.g., 'sector', 'stage', 'owner', 'probability'). Omit for ungrouped."
                        },
                        "metrics": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Aggregations: count, sum(field), avg(field), median(field), max(field), min(field)"
                        }
                    },
                    "required": ["board"]
                }
            )
        ]
    )
]


# =====================================================
# TOOL EXECUTION
# =====================================================

def execute_tool(
    args: Dict[str, Any],
    context: Dict[str, Any],
    trace: List[str]
) -> Dict[str, Any]:
    """
    Execute query_data tool with comprehensive error handling and tracing.
    
    Architecture:
    1. Validate tool arguments
    2. Merge new filters with persistent context
    3. Fetch live data from Monday.com
    4. Normalize data with quality tracking
    5. Execute dynamic query
    6. Attach data quality caveats to result
    
    Args:
        args: Tool call arguments {board, filters?, group_by?, metrics?}
        context: Conversation state {filters: {}, ...}
        trace: Execution trace list for UI/debugging
        
    Returns:
        Query result with metrics OR error dict
    """
    
    start_time = time.time()
    
    # --------- ARGUMENT VALIDATION ---------
    board = args.get("board", "").lower()
    
    if board not in ["deals", "work_orders", "cross_board"]:
        error_msg = f"Invalid board: {board}. Must be 'deals', 'work_orders', or 'cross_board'."
        logger.error(error_msg)
        trace.append(f"❌ {error_msg}")
        return {"error": error_msg}
    
    # --------- CONTEXT MERGING ---------
    # Merge new filters with persistent context (new values override old)
    context.setdefault("filters", {})
    new_filters = args.get("filters", {})
    
    for key, value in new_filters.items():
        if value is not None and value != "":
            context["filters"][key] = value
    
    filters = context["filters"]
    group_by = args.get("group_by")
    metrics = args.get("metrics", [])
    
    logger.info(
        f"Tool execution: board={board}, filters={filters}, "
        f"group_by={group_by}, metrics={metrics}"
    )
    trace.append(
        f"🔍 Querying {board.upper()}\n"
        f"  Filters: {json.dumps(filters) if filters else 'None'}\n"
        f"  Group by: {group_by or 'None'}\n"
        f"  Metrics: {', '.join(metrics) if metrics else 'count'}"
    )
    
    # --------- DATA FETCHING & NORMALIZATION ---------
    try:
        if board == "deals":
            board_id = get_board_id_by_name("Deal funnel Data")
            trace.append(f"📡 Fetching from board ID: {board_id}")
            
            items = fetch_board_items(board_id)
            data = normalize_deals(items)
            
            logger.debug(f"Fetched {len(data)} deals, normalizing...")
            trace.append(f"✓ Fetched {len(data)} deals")
            
            # Quality summary
            quality = summarize_data_quality(data, "deals")
            trace.append(
                f"📊 Data Quality: {quality['avg_quality_score']} avg, "
                f"{quality['records_with_caveats']} with caveats"
            )
        
        elif board == "work_orders":
            board_id = get_board_id_by_name("Work_Order_Tracker Data")
            trace.append(f"📡 Fetching from board ID: {board_id}")
            
            items = fetch_board_items(board_id)
            data = normalize_work_orders(items)
            
            logger.debug(f"Fetched {len(data)} work orders, normalizing...")
            trace.append(f"✓ Fetched {len(data)} work orders")
            
            # Quality summary
            quality = summarize_data_quality(data, "work_orders")
            trace.append(
                f"📊 Data Quality: {quality['avg_quality_score']} avg, "
                f"{quality['records_with_caveats']} with caveats"
            )
        
        elif board == "cross_board":
            # Fetch from both boards for sector-wise analysis
            trace.append(f"🔀 Cross-board query: fetching from both deals and work_orders")
            
            # Fetch deals
            deals_board_id = get_board_id_by_name("Deal funnel Data")
            deals_items = fetch_board_items(deals_board_id)
            deals_data = normalize_deals(deals_items)
            trace.append(f"✓ Fetched {len(deals_data)} deals")
            
            # Fetch work orders
            wo_board_id = get_board_id_by_name("Work_Order_Tracker Data")
            wo_items = fetch_board_items(wo_board_id)
            wo_data = normalize_work_orders(wo_items)
            trace.append(f"✓ Fetched {len(wo_data)} work orders")
            
            # Skip normal query execution, use cross_board logic below
            data = None
            quality = None
    
    except MondayAPIError as e:
        error_msg = f"Data fetch failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        trace.append(f"❌ {error_msg}")
        return {"error": error_msg}
    
    except Exception as e:
        error_msg = f"Normalization error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        trace.append(f"❌ {error_msg}")
        return {"error": error_msg}
    
    # --------- QUERY EXECUTION ---------
    try:
        if board == "cross_board":
            # Cross-board sector-wise join
            logger.debug(f"Executing cross-board query...")
            result = query_cross_board(deals_data, wo_data, filters, metrics)
            trace.append(f"🔗 Sector-wise join completed")
        else:
            # Standard single-board query
            logger.debug(f"Executing query with {len(data)} records...")
            result = run_dynamic_query(data, filters, group_by, metrics, board)
            
            # Add quality context to result
            if quality:
                result["data_quality"] = quality
        
        execution_ms = (time.time() - start_time) * 1000
        result["execution_time_ms"] = execution_ms
        
        logger.info(
            f"Query complete: {result['count'] if 'count' in result else 'cross-board'} "
            f"({execution_ms:.1f}ms)"
        )
        trace.append(f"✅ Query executed in {execution_ms:.1f}ms")
        if "count" in result:
            trace.append(f"📈 Result: {result['count']} records matched")
        elif "sector_insights" in result:
            trace.append(f"📈 Result: Sector insights for {len(result['sector_insights'])} sectors")
        
        return result
    
    except Exception as e:
        error_msg = f"Query execution error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        trace.append(f"❌ {error_msg}")
        return {"error": error_msg}


# =====================================================
# AGENTIC LOOP
# =====================================================

def run_agent(
    user_query: str,
    context: Dict[str, Any]
) -> Tuple[str, List[str], Dict[str, Any]]:
    """
    Execute agentic loop with Google Generative AI tool-calling.
    
    Multi-turn conversation with:
    - Tool-calling decision making
    - Context persistence across turns
    - Error recovery and clarification
    - Maximum 5 iterations to prevent loops
    
    Args:
        user_query: Natural language business question
        context: Conversation state {filters: {key: value, ...}}
        
    Returns:
        (final_response: str, trace: [str], updated_context: dict)
    """
    
    if not user_query or not user_query.strip():
        return "Please provide a business question.", [], context
    
    logger.info(f"Agent starting | Query: {user_query[:80]}...")
    trace = []
    
    # Initialize conversation history
    history = [
        {"role": "user", "parts": [{"text": user_query}]}
    ]
    
    iteration = 0
    max_iterations = 5
    
    while iteration < max_iterations:
        iteration += 1
        
        logger.debug(f"Agent iteration {iteration}/{max_iterations}")
        trace.append(f"---\n**Iteration {iteration}**")
        
        # --------- MODEL GENERATE ---------
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash-lite",
                contents=history,
                config=types.GenerateContentConfig(
                    tools=tools,
                    system_instruction=SYSTEM_PROMPT,
                    temperature=0.7
                )
            )
        except Exception as e:
            error_msg = f"LLM error: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return (
                f"I encountered an error calling the AI model: {error_msg}",
                trace,
                context
            )
        
        # --------- TOOL CALLS ---------
        if response.function_calls:
            
            for fc in response.function_calls:
                logger.info(f"Tool call: {fc.name} with args: {fc.args}")
                trace.append(f"🔧 Calling tool: **{fc.name}**")
                
                # Add model's call to history
                history.append({
                    "role": "model",
                    "parts": [{
                        "function_call": {
                            "name": fc.name,
                            "args": fc.args
                        }
                    }]
                })
                
                # Execute tool
                result = execute_tool(fc.args, context, trace)
                
                # Add result to history as text JSON
                result_text = json.dumps(result) if isinstance(result, dict) else str(result)
                history.append({
                    "role": "user",
                    "parts": [{
                        "text": result_text
                    }]
                })
                
                logger.debug(f"Tool result: {len(result_text)} chars")
            
            # Continue loop for next iteration
            continue
        
        # --------- FINAL RESPONSE ---------
        break
    
    # Extract final text response
    final_text = response.text or CLARIFICATION_PROMPT
    
    if iteration >= max_iterations:
        logger.warning("Max iterations reached, returning response")
        trace.append(f"⚠️ Max iterations ({max_iterations}) reached")
    
    logger.info(f"Agent complete | Response: {len(final_text)} chars")
    
    return final_text, trace, context
