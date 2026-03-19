# backend/main.py

"""
FastAPI Backend for Skylark BI Agent

Provides REST API for business intelligence queries with live Monday.com integration.
"""

import logging
from typing import Optional, Dict, Any
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
import logging.config

from .agent import run_agent
from .response_formatter import ResponseFormatter, format_api_response

# =====================================================
# LOGGING CONFIGURATION
# =====================================================

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        },
        "detailed": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "standard",
            "stream": "ext://sys.stdout"
        }
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": "DEBUG",
            "propagate": True
        }
    }
}

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

# =====================================================
# FASTAPI APP INITIALIZATION
# =====================================================

app = FastAPI(
    title="Skylark BI Agent API",
    description="Executive-level BI agent with live Monday.com integration. Supports complex queries with tool-based retrieval.",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# =====================================================
# REQUEST/RESPONSE MODELS
# =====================================================

class ChatRequest(BaseModel):
    """Chat/query request model."""
    
    message: str
    context: Optional[Dict[str, Any]] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "message": "How many renewables deals are in proposal stage?",
                "context": {
                    "filters": {
                        "sector": "renewables",
                        "stage": "proposal"
                    }
                }
            }
        }


class ChatResponse(BaseModel):
    """Chat response model."""
    
    status: str  # "success" or "error"
    response: Optional[str] = None  # Agent's response
    error: Optional[Dict[str, Any]] = None  # Error details if status="error"
    trace: list = []  # Execution trace
    context: Dict[str, Any] = {}  # Updated conversation context
    timestamp: str = ""  # ISO timestamp
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "response": "Based on the current data, there are 5 renewables deals in proposal stage with a combined value of $2.3M.",
                "trace": [
                    "Iteration 1",
                    "🔧 Calling tool: query_data",
                    "✓ Fetched 12 deals",
                    "✅ Query executed in 234.5ms"
                ],
                "context": {"filters": {"sector": "renewables", "stage": "proposal"}},
                "timestamp": "2026-03-19T10:30:45.123456"
            }
        }


# =====================================================
# ERROR HANDLERS
# =====================================================

@app.exception_handler(ValueError)
async def value_error_handler(request, exc):
    """Handle ValueError exceptions."""
    logger.error(f"Value error: {str(exc)}")
    return {
        "status": "error",
        "error": {
            "message": str(exc),
            "type": "validation_error"
        }
    }


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle uncaught exceptions."""
    logger.error(f"Unexpected error: {str(exc)}", exc_info=True)
    return {
        "status": "error",
        "error": {
            "message": "Internal server error",
            "type": "server_error"
        }
    }


# =====================================================
# HEALTH CHECK
# =====================================================

@app.get("/health", summary="Health Check")
def health_check():
    """
    Health check endpoint.
    
    Returns:
        Status dict
    """
    return {
        "status": "healthy",
        "service": "Skylark BI Agent API",
        "version": "2.0.0"
    }


# =====================================================
# MAIN CHAT ENDPOINT
# =====================================================

@app.post(
    "/chat",
    response_model=ChatResponse,
    summary="Execute BI Query",
    tags=["Chat"]
)
def chat(request: ChatRequest):
    """
    Process business intelligence query with persistent context.
    
    This endpoint:
    1. Validates incoming request
    2. Maintains filters across turns for multi-step analysis
    3. Triggers live API calls to Monday.com boards
    4. Returns formatted response with execution trace for transparency
    
    The trace shows:
    - Iteration count
    - Tool calls made
    - API fetch statistics
    - Query execution time
    - Data quality metrics
    
    Example flow:
    - User: "How's our pipeline looking for energy sector?"
    - Agent: Calls query_data tool for deals board
    - Monday.com: Returns all deal records
    - Agent: Normalizes and filters by sector
    - Agent: Calculates metrics and provides insights
    
    Args:
        request: ChatRequest with message and optional context
        
    Returns:
        ChatResponse with status, response, trace, and updated context
        
    Raises:
        HTTPException: If validation fails or agent encounters error
    """
    
    # --------- INPUT VALIDATION ---------
    is_valid, validation_error = ResponseFormatter.validate_request(
        request.message,
        request.context
    )
    
    if not is_valid:
        logger.warning(f"Request validation failed: {validation_error}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=validation_error
        )
    
    # Normalize context
    context = request.context or {"filters": {}}
    if "filters" not in context:
        context["filters"] = {}
    
    logger.info(f"Processing query: {request.message[:80]}...")
    logger.debug(f"Current filters: {context.get('filters', {})}")
    
    # --------- AGENT EXECUTION ---------
    try:
        response_text, trace, updated_context = run_agent(
            request.message,
            context
        )
        
        logger.info(f"Agent completed successfully ({len(trace)} trace steps)")
        
        # Format response
        return format_api_response(
            response_text=response_text,
            trace=trace,
            context=updated_context,
            error=None
        )
    
    except ValueError as e:
        error_msg = f"Validation error: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_msg
        )
    
    except Exception as e:
        error_msg = f"Agent execution failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_msg
        )


# =====================================================
# QUERY EXAMPLES ENDPOINT
# =====================================================

@app.get(
    "/examples",
    summary="Example Queries",
    tags=["Help"]
)
def get_examples():
    """
    Get example queries for founder-level business intelligence.
    
    Returns:
        List of example queries with descriptions
    """
    
    return {
        "examples": [
            {
                "query": "How many deals do we have in the renewables sector?",
                "use_case": "Total pipeline in specific sector"
            },
            {
                "query": "Show me deals grouped by stage with total values",
                "use_case": "Pipeline health snapshot"
            },
            {
                "query": "What's our total billed amount this quarter?",
                "use_case": "Revenue/execution tracking"
            },
            {
                "query": "Group work orders by sector and sum the collected amounts",
                "use_case": "Collections by sector"
            },
            {
                "query": "Show high-probability deals from last quarter",
                "use_case": "Weighted pipeline view"
            },
            {
                "query": "What's the average deal value by sector?",
                "use_case": "Deal size analysis"
            }
        ]
    }


# =====================================================
# ROOT ENDPOINT
# =====================================================

@app.get("/", tags=["Info"])
def root():
    """Root endpoint with API info."""
    
    return {
        "service": "Skylark BI Agent",
        "version": "2.0.0",
        "endpoints": {
            "health": "/health",
            "chat": "/chat (POST)",
            "examples": "/examples",
            "docs": "/docs (Swagger UI)"
        }
    }