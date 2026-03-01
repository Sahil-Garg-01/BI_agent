# frontend/app.py

import logging
import streamlit as st
import requests

logger = logging.getLogger(__name__)

BACKEND_URL = "http://localhost:8000/chat"

st.title("Skylark Drones BI Agent")

user_input = st.chat_input("Ask a business question...")

if user_input:
    with st.spinner("Analyzing..."):
        res = requests.post(
            BACKEND_URL,
            json={"message": user_input}
        )

        if res.status_code == 200:
            data = res.json()

            st.subheader("Response")
            st.write(data["response"])

            st.subheader("Tool Trace")
            for t in data["trace"]:
                st.write("-", t)
        else:
            logger.error(f"Backend error {res.status_code}")
            st.error("Backend error")