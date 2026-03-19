# backend/response_formatter.py

"""
Response Formatting Layer

Transforms agent outputs and query results into structured, frontend-ready responses
with proper error handling, metadata, and data quality information.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class ResponseFormatter:
    """Format and structure all API responses."""
    
    @staticmethod
    def success_response(
        response_text: str,
        trace: List[str],
        context: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Format successful agent response.
        
        Args:
            response_text: Agent's natural language response
            trace: List of execution trace entries
            context: Conversation context dict
            metadata: Optional additional metadata
            
        Returns:
            Structured response dict
        """
        
        return {
            "status": "success",
            "response": response_text,
            "trace": trace,
            "context": context,
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": metadata or {}
        }
    
    @staticmethod
    def error_response(
        error_message: str,
        error_type: str = "agent_error",
        trace: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Format error response.
        
        Args:
            error_message: Human-readable error description
            error_type: Category (validation_error, agent_error, api_error, etc.)
            trace: Optional execution trace at failure point
            
        Returns:
            Structured error response
        """
        
        return {
            "status": "error",
            "error": {
                "message": error_message,
                "type": error_type
            },
            "trace": trace or [],
            "timestamp": datetime.utcnow().isoformat()
        }
    
    @staticmethod
    def format_query_result(
        query_result: Dict[str, Any],
        board_type: str = "deals"
    ) -> Dict[str, Any]:
        """
        Format raw query result for frontend display.
        
        Adds:
        - Human-readable summary
        - Data quality warnings
        - Recommendations
        
        Args:
            query_result: Raw result from run_dynamic_query
            board_type: "deals" or "work_orders"
            
        Returns:
            Formatted result dict with context
        """
        
        formatted = {
            "raw_result": query_result,
            "summary": {
                "records_matched": query_result.get("count", 0),
                "total_available": query_result.get("from_total", 0),
                "filters": query_result.get("filters_applied", {}),
                "execution_ms": query_result.get("execution_time_ms", 0)
            }
        }
        
        # Data quality annotations
        quality = query_result.get("data_quality", {})
        if quality:
            formatted["data_quality"] = {
                "avg_score": quality.get("avg_quality_score", 0),
                "records_with_issues": quality.get("records_with_caveats", 0),
                "issue_types": quality.get("caveat_types", {})
            }
            
            # Add caveat if quality is low
            if quality.get("avg_quality_score", 1) < 0.5:
                formatted["caveat"] = (
                    "⚠️ Data quality is low. Many records have missing critical fields. "
                    "Results should be considered preliminary."
                )
        
        # Grouping results
        if query_result.get("grouped_by"):
            formatted["grouped_by"] = query_result["grouped_by"]
            formatted["groups"] = query_result.get("groups", {})
            formatted["group_count"] = query_result.get("group_count", 0)
        
        return formatted
    
    @staticmethod
    def validate_request(
        message: Optional[str],
        context: Optional[Dict[str, Any]]
    ) -> tuple:
        """
        Validate incoming request.
        
        Args:
            message: User query/message
            context: Conversation context
            
        Returns:
            (is_valid: bool, error_if_invalid: Dict or None)
        """
        
        errors = []
        
        # Message validation
        if not message:
            errors.append("Message cannot be empty")
        elif not isinstance(message, str):
            errors.append("Message must be a string")
        elif len(message.strip()) == 0:
            errors.append("Message cannot be whitespace only")
        elif len(message) > 2000:
            errors.append("Message too long (max 2000 chars)")
        
        # Context validation
        if context is None:
            context = {"filters": {}}
        elif not isinstance(context, dict):
            errors.append("Context must be a dict")
        
        if errors:
            return False, {
                "status": "error",
                "error": {
                    "message": "Request validation failed",
                    "type": "validation_error",
                    "details": errors
                }
            }
        
        return True, None


def format_api_response(
    response_text: str,
    trace: List[str],
    context: Dict[str, Any],
    error: Optional[str] = None
) -> Dict[str, Any]:
    """
    Format final API response.
    
    Main entry point for formatting all responses from run_agent().
    
    Args:
        response_text: Agent response (or error message if error occurred)
        trace: Execution trace
        context: Updated conversation context
        error: Optional error message (None if success)
        
    Returns:
        Structured API response
    """
    
    if error:
        return ResponseFormatter.error_response(error, trace=trace)
    
    # Success response with visible trace for transparency
    formatted_trace = []
    for item in trace:
        formatted_trace.append(item)
    
    return ResponseFormatter.success_response(
        response_text=response_text,
        trace=formatted_trace,
        context=context,
        metadata={
            "trace_entries": len(formatted_trace),
            "has_context_filters": len(context.get("filters", {})) > 0
        }
    )
