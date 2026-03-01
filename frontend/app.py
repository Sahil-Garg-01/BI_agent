# frontend/app.py

import logging
import streamlit as st
import requests

logger = logging.getLogger(__name__)

BACKEND_URL = "http://localhost:8000/chat"

st.title("Skylark Drones BI Agent")

if "conversation" not in st.session_state:
    st.session_state.conversation = []

if "context" not in st.session_state:
    st.session_state.context = {"filters": {}}

user_input = st.chat_input("Ask a business question...")

if user_input:
    st.session_state.conversation.append(
        {"role": "user", "content": user_input}
    )

    payload = {
        "message": user_input,
        "context": st.session_state.context
    }

    with st.spinner("Analyzing..."):
        res = requests.post(BACKEND_URL, json=payload)

    if res.status_code == 200:
        data = res.json()

        st.session_state.context = data.get("updated_context", {"filters": {}})

        st.session_state.conversation.append(
            {"role": "assistant", "content": data["response"]}
        )

for msg in st.session_state.conversation:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])