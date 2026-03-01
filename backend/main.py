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
app = FastAPI(title="Skylark BI Agent")

class ChatRequest(BaseModel):
    message: str
    context: Optional[Dict] = {}

@app.post("/chat")
def chat(request: ChatRequest):
    logger.info(f"Query: {request.message[:50]}...")
    try:
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