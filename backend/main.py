# backend/main.py

from fastapi import FastAPI
from pydantic import BaseModel
from .agent import run_agent

app = FastAPI(title="Skylark BI Agent")

class ChatRequest(BaseModel):
    message: str

@app.post("/chat")
def chat(request: ChatRequest):
    response, trace = run_agent(request.message)

    return {
        "response": response,
        "trace": trace
    }