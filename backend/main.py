# backend/main.py

import logging
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional, Dict
from .agent import run_agent

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
app = FastAPI(
    title="Skylark BI Agent API",
    description="Executive-level BI agent with live Monday.com data integration",
    version="1.0.0"
)

class ChatRequest(BaseModel):
    message: str
    context: Optional[Dict] = {}
    
    class Config:
        example = {
            "message": "Show me renewables deals grouped by stage",
            "context": {"filters": {"sector": "renewables"}}
        }

@app.post("/chat", summary="Execute BI Query")
def chat(request: ChatRequest):
    """
    Process business intelligence query with persistent context.
    
    Maintains filters across turns for multi-step analysis.
    Returns formatted response with execution trace.
    """
    logger.info(f"Query: {request.message[:50]}...")
    try:
        if not request.message or not request.message.strip():
            raise ValueError("Message cannot be empty")
        
        response, trace, updated_context = run_agent(
            request.message,
            request.context or {"filters": {}}
        )

        return {
            "response": response,
            "trace": trace,
            "updated_context": updated_context
        }
    except Exception as e:
        logger.error(f"Agent error: {str(e)}", exc_info=True)
        raise