from openai import OpenAI
import streamlit as st
import os

class AIAssistant:
    def __init__(self):
        try:
            self.client = OpenAI(
                base_url="https://api.groq.com/openai/v1",
                api_key="YOUR_API_KEY_HERE"
            )
        except Exception:
            self.client = None

    def get_response(self, system_role, user_prompt, chat_history):
        if not self.client:
            yield "Error: API Client failed to initialize."
            return

        messages = [{"role": "system", "content": system_role}] + chat_history + [{"role": "user", "content": user_prompt}]

        try:
            stream = self.client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=messages,
                stream=True
            )
            for chunk in stream:
                if chunk.choices[0].delta.content:
                    yield chunk.choices[0].delta.content
        except Exception as e:
            yield f"Connection Error: {e}"