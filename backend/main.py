# backend/main.py

import logging
from fastapi import FastAPI
from pydantic import BaseModel
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

@app.post("/chat")
def chat(request: ChatRequest):
    logger.info(f"Query: {request.message[:50]}...")
    try:
        response, trace = run_agent(request.message)
        return {
            "response": response,
            "trace": trace
        }
    except Exception as e:
        logger.error(f"Agent error: {str(e)}", exc_info=True)
        raise