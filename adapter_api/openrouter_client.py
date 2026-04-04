from __future__ import annotations

import os
from typing import Any

import httpx


class OpenRouterError(RuntimeError):
    pass


def _env(name: str, default: str | None = None) -> str | None:
    v = os.environ.get(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def chat_completion(*, messages: list[dict[str, str]], model: str | None = None) -> dict[str, Any]:
    """
    Minimal OpenRouter Chat Completions client.
    Expects `OPENROUTER_API_KEY` in environment (loaded via .env by adapter_api/app.py).
    """
    api_key = _env("OPENROUTER_API_KEY")
    if not api_key:
        raise OpenRouterError("Missing OPENROUTER_API_KEY")

    chosen_model = model or _env("OPENROUTER_MODEL", "openai/gpt-4o-mini") or "openai/gpt-4o-mini"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    # Optional (recommended by OpenRouter for attribution/analytics)
    http_referer = _env("OPENROUTER_HTTP_REFERER")
    app_title = _env("OPENROUTER_APP_TITLE")
    if http_referer:
        headers["HTTP-Referer"] = http_referer
    if app_title:
        headers["X-Title"] = app_title

    payload = {
        "model": chosen_model,
        "messages": messages,
        "temperature": 0.2,
    }

    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload)
    except Exception as e:
        raise OpenRouterError(f"Network error calling OpenRouter: {e}") from e

    if resp.status_code >= 400:
        raise OpenRouterError(f"OpenRouter error {resp.status_code}: {resp.text[:400]}")

    try:
        return resp.json()
    except Exception as e:
        raise OpenRouterError(f"Invalid JSON from OpenRouter: {e}") from e

