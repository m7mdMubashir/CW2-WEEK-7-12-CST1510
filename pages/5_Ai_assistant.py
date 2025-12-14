import streamlit as st
import sys
import os
from app.services.Ai_assistant import AIAssistant
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
sys.path.append(root_dir)

from app.services.Ai_assistant import AIAssistant

st.set_page_config(page_title="AI Assistant", page_icon="🤖")

st.markdown(
    """
    <div style="background:linear-gradient(90deg,#4f46e5,#818cf8);padding:15px;border-radius:10px;color:white;margin-bottom:20px;">
        <h2 style="margin:0;">🤖 In-Depth AI Assistant</h2>
        <div style="opacity:0.8;">Expert help for Cybersecurity, Data Science, and IT Operations</div>
    </div>
    """, 
    unsafe_allow_html=True
)

ai_service = AIAssistant()

with st.sidebar:
    st.header("🧠 AI Brain Settings")
    domain = st.selectbox(
        "Select Expertise Area",
        ["General Helper", "Cybersecurity Expert", "Data Scientist", "IT Support Lead"]
    )
    
    if st.button("Clear Chat History"):
        st.session_state.global_messages = []
        st.rerun()

roles = {
    "General Helper": "You are a helpful assistant for the Multi-Domain Platform.",
    "Cybersecurity Expert": "You are a Senior Security Analyst. Explain threats using MITRE ATT&CK terms.",
    "Data Scientist": "You are a Data Governance Officer. Focus on data quality and privacy.",
    "IT Support Lead": "You are an IT Support Manager. Be empathetic and professional."
}
current_role = roles[domain]

if "global_messages" not in st.session_state:
    st.session_state.global_messages = []

for msg in st.session_state.global_messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if prompt := st.chat_input("How can I help you today?"):
    with st.chat_message("user"):
        st.markdown(prompt)
    st.session_state.global_messages.append({"role": "user", "content": prompt})

    with st.chat_message("assistant"):
        response_stream = ai_service.get_response(
            system_role=current_role,
            user_prompt=prompt,
            chat_history=st.session_state.global_messages
        )
        response = st.write_stream(response_stream)
    
    st.session_state.global_messages.append({"role": "assistant", "content": response})