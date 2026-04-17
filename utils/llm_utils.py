"""Utilities for generating LLM-based advisory trading signals."""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any

import aiohttp

from config.llm_config import (
    GEMINI_API_KEY,
    GEMINI_MODEL,
    LLM_PROVIDER,
    MAX_RETRIES,
    MAX_TOKENS,
    OPENAI_API_KEY,
    OPENAI_MODEL,
    RETRY_DELAY,
    TEMPERATURE,
    TIMEOUT_SECONDS,
)
from utils.exceptions import LLMAPIError, LLMParseError, LLMQuotaExceededError
from utils.logger import get_logger

logger = get_logger(__name__)


async def _call_gemini(session: aiohttp.ClientSession, prompt: str) -> str:
    if not GEMINI_API_KEY:
        raise LLMAPIError("Missing GEMINI_API_KEY")

    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": TEMPERATURE,
            "maxOutputTokens": MAX_TOKENS,
        },
    }

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


async def _call_openai(session: aiohttp.ClientSession, prompt: str) -> str:
    if not OPENAI_API_KEY:
        raise LLMAPIError("Missing OPENAI_API_KEY")

    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": OPENAI_MODEL,
        "messages": [{"role": "user", "content": prompt}],
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


def _extract_json_object(raw: str) -> str:
    text = raw.strip()
    text = re.sub(r"^```(?:json)?", "", text).strip()
    text = re.sub(r"```$", "", text).strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise LLMParseError(f"No JSON object found in response: {raw!r}")
    return text[start : end + 1]


def parse_llm_response(raw: str) -> dict[str, Any]:
    try:
        result = json.loads(_extract_json_object(raw))
        signal = result.get("signal")
        confidence = result.get("confidence")
        reason = result.get("reason")
        key_risk = result.get("key_risk", "")

        if signal not in {"BUY", "SELL", "HOLD"}:
            raise ValueError("signal must be BUY|SELL|HOLD")
        if not isinstance(confidence, int) or not (1 <= confidence <= 5):
            raise ValueError("confidence must be integer in [1,5]")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be non-empty string")
        if key_risk is None:
            key_risk = ""
        if not isinstance(key_risk, str):
            raise ValueError("key_risk must be string")

        return {
            "signal": signal,
            "confidence": confidence,
            "reason": reason.strip()[:240],
            "key_risk": key_risk.strip()[:120],
        }
    except LLMParseError:
        raise
    except Exception as exc:
        raise LLMParseError(f"Failed to parse LLM response: {raw!r}") from exc


async def get_llm_signal(
    session: aiohttp.ClientSession,
    symbol: str,
    prompt: str,
) -> dict[str, Any] | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if LLM_PROVIDER == "gemini":
                raw = await _call_gemini(session, prompt)
            elif LLM_PROVIDER == "openai":
                raw = await _call_openai(session, prompt)
            else:
                raise LLMAPIError(f"Unsupported LLM_PROVIDER: {LLM_PROVIDER}")

            parsed = parse_llm_response(raw)
            logger.debug("[%s] %s (conf=%s)", symbol, parsed["signal"], parsed["confidence"])
            return parsed
        except LLMQuotaExceededError:
            # Quota exhausted should fail fast for this symbol (and caller can stop whole run).
            raise
        except (LLMAPIError, LLMParseError) as exc:
            msg = str(exc).lower()
            if "http 429" in msg and "quota" in msg:
                raise LLMQuotaExceededError(str(exc)) from exc
            logger.warning("[%s] Attempt %d/%d failed: %s", symbol, attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY * attempt)

    return None
