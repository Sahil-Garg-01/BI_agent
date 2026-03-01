# frontend/app.py

import streamlit as st
import requests

BACKEND_URL = "http://localhost:8000/chat"

st.title("Skylark Drones BI Agent")

user_input = st.chat_input("Ask a business question...")

if user_input:
    with st.spinner("Thinking..."):
        response = requests.post(
            BACKEND_URL,
            json={"message": user_input}
        )
        if response.status_code == 200:
            data = response.json()
            st.write("### Response")
            st.write(data["response"])

            st.write("### Tool Trace")
            for t in data["trace"]:
                st.write("-", t)
        else:
            st.error(f"Backend error: {response.status_code} - {response.text}")