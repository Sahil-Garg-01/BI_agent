# frontend/app.py

"""
Streamlit Frontend for Skylark BI Agent

Executive-level UI for querying business data with transparent tool execution visibility.
"""

import logging
import streamlit as st
import requests
import json
from datetime import datetime

logger = logging.getLogger(__name__)

# =====================================================
# PAGE CONFIGURATION
# =====================================================

st.set_page_config(
    page_title="Skylark BI Agent 🚁",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🚁 Skylark BI Agent")
st.markdown(
    "Executive-level business intelligence with **live** Monday.com data integration. "
    "Every query shows complete tool-call transparency."
)

# =====================================================
# CONFIGURATION
# =====================================================

# Handle missing secrets gracefully (for development)
try:
    API_BASE_URL = st.secrets["api_url"]
except (KeyError, AttributeError, FileNotFoundError):
    API_BASE_URL = "http://localhost:8000"

BACKEND_URL = f"{API_BASE_URL}/chat"
HEALTH_URL = f"{API_BASE_URL}/health"

# =====================================================
# SIDEBAR
# =====================================================

with st.sidebar:
    st.header("⚙️ Configuration")
    
    # API Status
    try:
        health_resp = requests.get(HEALTH_URL, timeout=5)
        if health_resp.status_code == 200:
            st.success("✅ Backend Connected")
        else:
            st.error("❌ Backend Error")
    except requests.RequestException:
        st.error("❌ Backend Offline")
    
    st.divider()
    
    # Clear conversation
    if st.button("🔄 Clear Conversation", use_container_width=True):
        st.session_state.conversation = []
        st.session_state.context = {"filters": {}}
        st.rerun()
    
    # Show current filters
    st.subheader("📋 Active Filters")
    if st.session_state.get("context", {}).get("filters"):
        filters = st.session_state.context["filters"]
        for key, value in filters.items():
            st.caption(f"`{key}`: {value}")
    else:
        st.caption("No filters set yet")
    
    # Help section
    st.divider()
    st.subheader("💡 Example Queries")
    examples = [
        "How many renewables deals are in proposal stage?",
        "Show deals grouped by owner with total values",
        "What's our pipeline by sector this quarter?",
        "Group work orders by status and sum billed amounts"
    ]
    
    selected_example = st.selectbox(
        "Quick start:",
        [""] + examples,
        key="example_selector"
    )
    
    if selected_example:
        st.session_state.next_query = selected_example

# =====================================================
# SESSION STATE INITIALIZATION
# =====================================================

if "conversation" not in st.session_state:
    st.session_state.conversation = []

if "context" not in st.session_state:
    st.session_state.context = {"filters": {}}

if "next_query" not in st.session_state:
    st.session_state.next_query = None

# =====================================================
# MAIN CHAT INTERFACE
# =====================================================

# Display conversation history
for msg in st.session_state.conversation:
    with st.chat_message(msg["role"]):
        # Response text
        st.markdown(msg["content"])
        
        # Show trace in expander (for assistant messages)
        if msg["role"] == "assistant" and msg.get("trace"):
            with st.expander("📊 Execution Trace", expanded=False):
                trace_html = ""
                for trace_item in msg["trace"]:
                    # Escape HTML and preserve formatting
                    trace_text = trace_item.replace("<", "&lt;").replace(">", "&gt;")
                    
                    # Syntax highlighting for trace items
                    if trace_text.startswith("🔍"):
                        trace_html += f"<div style='color: #0066cc; font-weight: bold;'>{trace_text}</div>"
                    elif trace_text.startswith("✓"):
                        trace_html += f"<div style='color: green;'>{trace_text}</div>"
                    elif trace_text.startswith("❌"):
                        trace_html += f"<div style='color: red; font-weight: bold;'>{trace_text}</div>"
                    elif trace_text.startswith("📊"):
                        trace_html += f"<div style='color: #9933ff;'>{trace_text}</div>"
                    elif trace_text.startswith("✅"):
                        trace_html += f"<div style='color: green;'>{trace_text}</div>"
                    elif trace_text.startswith("🔧"):
                        trace_html += f"<div style='color: #ff6600;'>{trace_text}</div>"
                    elif trace_text.startswith("⚠️"):
                        trace_html += f"<div style='color: orange;'>{trace_text}</div>"
                    elif trace_text.startswith("📡"):
                        trace_html += f"<div style='color: #0099cc;'>{trace_text}</div>"
                    else:
                        trace_html += f"<div style='color: #666;'><code>{trace_text}</code></div>"
                
                st.markdown(trace_html, unsafe_allow_html=True)
        
        # Show timestamp
        if msg.get("timestamp"):
            st.caption(f"🕐 {msg['timestamp']}")

# =====================================================
# INPUT HANDLING
# =====================================================

# Check if example was selected
user_input = None
if st.session_state.next_query:
    user_input = st.session_state.next_query
    st.session_state.next_query = None

# Chat input
if not user_input:
    user_input = st.chat_input(
        "Ask a business question...",
        key="chat_input"
    )

if user_input:
    # Add user message to conversation
    st.session_state.conversation.append({
        "role": "user",
        "content": user_input,
        "timestamp": datetime.now().strftime("%H:%M:%S")
    })
    
    # Display user message
    with st.chat_message("user"):
        st.markdown(user_input)
    
    # Prepare payload
    payload = {
        "message": user_input,
        "context": st.session_state.context
    }
    
    # API call with loading state
    with st.spinner("🔄 Agent analyzing..."):
        try:
            response = requests.post(
                BACKEND_URL,
                json=payload,
                timeout=60
            )
            
            if response.status_code != 200:
                error_msg = f"API Error {response.status_code}"
                try:
                    error_data = response.json()
                    if "detail" in error_data:
                        error_msg = error_data["detail"]
                except:
                    error_msg = response.text[:200]
                
                st.error(f"❌ {error_msg}")
                logger.error(f"API error: {error_msg}")
            
            else:
                data = response.json()
                
                # Update context
                if "context" in data:
                    st.session_state.context = data["context"]
                
                # Add assistant message
                assistant_msg = {
                    "role": "assistant",
                    "content": data.get("response", "No response"),
                    "trace": data.get("trace", []),
                    "timestamp": data.get("timestamp", datetime.now().isoformat())
                }
                
                st.session_state.conversation.append(assistant_msg)
                
                # Display assistant response
                with st.chat_message("assistant"):
                    st.markdown(assistant_msg["content"])
                    
                    # Show trace
                    if assistant_msg["trace"]:
                        with st.expander("📊 Execution Trace", expanded=False):
                            trace_html = ""
                            for trace_item in assistant_msg["trace"]:
                                trace_text = trace_item.replace("<", "&lt;").replace(">", "&gt;")
                                
                                # Color-coded trace
                                if trace_text.startswith("🔍"):
                                    trace_html += f"<div style='color: #0066cc; font-weight: bold;'>{trace_text}</div>"
                                elif trace_text.startswith("✓"):
                                    trace_html += f"<div style='color: green;'>{trace_text}</div>"
                                elif trace_text.startswith("❌"):
                                    trace_html += f"<div style='color: red; font-weight: bold;'>{trace_text}</div>"
                                elif trace_text.startswith("📊"):
                                    trace_html += f"<div style='color: #9933ff;'>{trace_text}</div>"
                                elif trace_text.startswith("✅"):
                                    trace_html += f"<div style='color: #28a745;'>{trace_text}</div>"
                                elif trace_text.startswith("🔧"):
                                    trace_html += f"<div style='color: #ff6600;'>{trace_text}</div>"
                                elif trace_text.startswith("⚠️"):
                                    trace_html += f"<div style='color: #ff9900;'>{trace_text}</div>"
                                elif trace_text.startswith("📡"):
                                    trace_html += f"<div style='color: #0099cc;'>{trace_text}</div>"
                                else:
                                    trace_html += f"<div style='color: #666;'><code>{trace_text}</code></div>"
                            
                            st.markdown(trace_html, unsafe_allow_html=True)
                    
                    # Show timestamp
                    st.caption(f"🕐 {assistant_msg['timestamp']}")
        
        except requests.Timeout:
            st.error("⏱️ Request timeout - backend may be slow or offline")
            logger.error("Request timeout")
        
        except requests.RequestException as e:
            st.error(f"❌ Connection error: {str(e)}")
            logger.error(f"Request error: {str(e)}")
        
        except json.JSONDecodeError:
            st.error("❌ Invalid response format from backend")
            logger.error("JSON decode error")
        
        except Exception as e:
            st.error(f"❌ Unexpected error: {str(e)}")
            logger.error(f"Unexpected error: {str(e)}", exc_info=True)

# =====================================================
# FOOTER
# =====================================================

st.divider()
st.markdown(
    """
    <div style='text-align: center; color: #888; font-size: 0.9em;'>
    powered by <strong>Skylark BI Agent v2.0</strong> | 
    <a href='http://localhost:8000/docs'>API Docs</a>
    </div>
    """,
    unsafe_allow_html=True
)