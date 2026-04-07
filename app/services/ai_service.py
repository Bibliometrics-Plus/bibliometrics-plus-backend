"""
Grounded AI helpers for the Bibliometrics+ dashboard.

The AI layer is designed to summarize real dashboard results, not invent new
facts. The app first queries the database, then passes those results and the
user's question to the model.
"""

from __future__ import annotations

import json
import os
from textwrap import dedent

import pandas as pd
import requests
import streamlit as st


class AIServiceError(RuntimeError):
    """Raised when the AI service cannot complete a request."""


def has_openai_api_key() -> bool:
    """Return whether an OpenAI API key is configured for the app."""
    return bool(os.getenv("OPENAI_API_KEY") or st.secrets.get("OPENAI_API_KEY", ""))


def _get_api_key() -> str:
    """
    Resolve the OpenAI API key from the environment or Streamlit secrets.

    Supporting both options makes local development and Streamlit deployment
    easier, because some setups prefer `.env` while others rely on secrets.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        api_key = st.secrets.get("OPENAI_API_KEY", "")
    if not api_key:
        raise AIServiceError("OPENAI_API_KEY is not configured.")
    return api_key


def dataframe_context(name: str, df: pd.DataFrame, max_rows: int = 15) -> str:
    """
    Convert a dataframe into compact JSON for grounding.

    I limit the number of rows because the model does not need the entire raw
    table to produce useful executive summaries.
    """
    records = df.head(max_rows).to_dict(orient="records")
    return f"{name}: {json.dumps(records, default=str)}"


def build_grounded_prompt(question: str, contexts: list[str]) -> str:
    """Create a prompt that explicitly restricts the model to grounded data."""
    joined_context = "\n".join(contexts)
    return dedent(
        f"""
        You are helping with the Bibliometrics+ dashboard.
        This project is an AI & EDI-Driven Library Usage Analytics Dashboard.

        Use only the supplied dashboard data.
        Do not invent metrics, rows, or trends that are not present.
        If the evidence is incomplete, say that clearly.
        Keep the answer concise, professional, and easy to present.

        User question:
        {question}

        Grounding data:
        {joined_context}
        """
    ).strip()


def ask_openai(question: str, contexts: list[str], model: str = "gpt-4.1-mini") -> str:
    """
    Call the OpenAI Responses API with grounded dashboard context.

    This uses `requests` so the dashboard does not depend on an additional SDK.
    """
    api_key = _get_api_key()
    prompt = build_grounded_prompt(question, contexts)

    response = requests.post(
        "https://api.openai.com/v1/responses",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "input": prompt,
        },
        timeout=60,
    )

    if response.status_code >= 400:
        try:
            payload = response.json()
        except ValueError:
            payload = {}

        error_payload = payload.get("error", {})
        error_code = error_payload.get("code")
        error_message = error_payload.get("message", "").strip()

        if error_code == "insufficient_quota":
            raise AIServiceError(
                "The OpenAI API key is configured, but the API project does not currently have enough quota or billing enabled."
            )
        if response.status_code in {401, 403}:
            raise AIServiceError("The OpenAI API key was rejected. Check that the key is valid for this project.")
        if error_message:
            raise AIServiceError(f"OpenAI request failed: {error_message}")
        raise AIServiceError("OpenAI request failed. Check the API key, model access, and billing configuration.")

    payload = response.json()
    output = payload.get("output", [])
    text_parts: list[str] = []
    for item in output:
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                text_parts.append(content.get("text", ""))

    answer = "\n".join(part for part in text_parts if part).strip()
    if not answer:
        raise AIServiceError("The AI service returned an empty response.")
    return answer
