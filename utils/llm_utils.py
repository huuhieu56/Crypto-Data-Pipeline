"""Utilities for calling LLM providers (Gemini / OpenAI) in chat mode."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import aiohttp

from config.llm_config import (
    LLM_API_KEY,
    LLM_MODEL,
    LLM_PROVIDER,
    MAX_RETRIES,
    MAX_TOKENS,
    RETRY_DELAY,
    TEMPERATURE,
    TIMEOUT_SECONDS,
)
from utils.exceptions import LLMAPIError, LLMQuotaExceededError
from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Provider-specific callers
# ---------------------------------------------------------------------------

async def _call_gemini(
    session: aiohttp.ClientSession,
    system_prompt: str,
    messages: list[dict[str, str]],
) -> str:
    """Call Google Gemini with system instruction + multi-turn contents."""
    if not LLM_API_KEY:
        raise LLMAPIError("Missing LLM_API_KEY")

    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{LLM_MODEL}:generateContent?key={LLM_API_KEY}"
    )

    # Build Gemini contents array from conversation history
    contents: list[dict[str, Any]] = []
    for msg in messages:
        role = "user" if msg["role"] == "user" else "model"
        contents.append({"role": role, "parts": [{"text": msg["content"]}]})

    payload: dict[str, Any] = {
        "contents": contents,
        "generationConfig": {
            "temperature": TEMPERATURE,
            "maxOutputTokens": MAX_TOKENS,
        },
    }
    if system_prompt:
        payload["systemInstruction"] = {"parts": [{"text": system_prompt}]}

    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
    async with session.post(url, json=payload, timeout=timeout) as resp:
        body = await resp.text()
        if resp.status >= 400:
            raise LLMAPIError(f"Gemini HTTP {resp.status}: {body[:300]}")
        data = json.loads(body)
        try:
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()
        except Exception as exc:
            raise LLMAPIError(f"Unexpected Gemini response: {body[:300]}") from exc


async def _call_openai(
    session: aiohttp.ClientSession,
    system_prompt: str,
    messages: list[dict[str, str]],
) -> str:
    """Call OpenAI Chat Completions with system + multi-turn messages."""
    if not LLM_API_KEY:
        raise LLMAPIError("Missing LLM_API_KEY")

    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {LLM_API_KEY}",
        "Content-Type": "application/json",
    }

    oai_messages: list[dict[str, str]] = []
    if system_prompt:
        oai_messages.append({"role": "system", "content": system_prompt})
    for msg in messages:
        oai_messages.append({"role": msg["role"], "content": msg["content"]})

    payload = {
        "model": LLM_MODEL,
        "messages": oai_messages,
        "temperature": TEMPERATURE,
        "max_tokens": MAX_TOKENS,
    }

    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
    async with session.post(url, json=payload, headers=headers, timeout=timeout) as resp:
        body = await resp.text()
        if resp.status >= 400:
            raise LLMAPIError(f"OpenAI HTTP {resp.status}: {body[:300]}")
        data = json.loads(body)
        try:
            return data["choices"][0]["message"]["content"].strip()
        except Exception as exc:
            raise LLMAPIError(f"Unexpected OpenAI response: {body[:300]}") from exc


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def get_chat_response(
    session: aiohttp.ClientSession,
    system_prompt: str,
    messages: list[dict[str, str]],
) -> str | None:
    """Send a chat request to the configured LLM provider with retry/backoff.

    Args:
        session: aiohttp client session.
        system_prompt: System-level instruction including market context.
        messages: Conversation history as list of {"role": "user"|"assistant", "content": "..."}.

    Returns:
        The assistant reply text, or None if all retries fail.

    Raises:
        LLMQuotaExceededError: When the provider quota is exhausted (fail-fast).
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if LLM_PROVIDER == "gemini":
                reply = await _call_gemini(session, system_prompt, messages)
            elif LLM_PROVIDER == "openai":
                reply = await _call_openai(session, system_prompt, messages)
            else:
                raise LLMAPIError(f"Unsupported LLM_PROVIDER: {LLM_PROVIDER}")

            logger.debug("LLM reply (%d chars)", len(reply))
            return reply
        except LLMQuotaExceededError:
            raise
        except LLMAPIError as exc:
            msg = str(exc).lower()
            if "http 429" in msg and "quota" in msg:
                raise LLMQuotaExceededError(str(exc)) from exc
            logger.warning("Attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY * attempt)

    return None
