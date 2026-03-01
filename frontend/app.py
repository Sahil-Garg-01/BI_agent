# frontend/app.py

import logging
import streamlit as st
import requests

logger = logging.getLogger(__name__)

BACKEND_URL = "https://biagent-production.up.railway.app/chat"

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
        try:
            res = requests.post(BACKEND_URL, json=payload, timeout=30)
            res.raise_for_status()
        except requests.Timeout:
            st.error("Request timeout - backend may be slow")
            st.stop()
        except requests.RequestException as e:
            st.error(f"Backend error: {str(e)}")
            st.stop()

    if res.status_code == 200:
        try:
            data = res.json()
        except ValueError:
            st.error(" Invalid response format")
            st.stop()

        st.session_state.context = data.get("updated_context", {"filters": {}})

        st.session_state.conversation.append(
            {"role": "assistant", "content": data["response"]}
        )
        
        # Show trace in expander
        with st.expander("Execution Trace"):
            for trace_item in data.get("trace", []):
                st.text(trace_item)

for msg in st.session_state.conversation:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])