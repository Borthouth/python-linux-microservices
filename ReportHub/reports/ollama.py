import json
import logging
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from django.conf import settings


logger = logging.getLogger(__name__)


class OllamaUnavailable(Exception):
    pass


def generate_text(prompt, response_format=None, *, options=None):
    body = {
        "model": getattr(settings, "OLLAMA_MODEL", "qwen2.5:3b"),
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.1, **(options or {})},
    }
    if response_format:
        body["format"] = response_format

    base_url = getattr(settings, "OLLAMA_BASE_URL", "http://ollama:11434").rstrip("/")
    request = Request(
        f"{base_url}/api/generate",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(request, timeout=getattr(settings, "OLLAMA_TIMEOUT", 120)) as response:
            ollama_response = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        logger.warning("Ollama request failed: %s", exc)
        raise OllamaUnavailable from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        logger.warning("Ollama returned an invalid response: %s", exc)
        raise OllamaUnavailable from exc

    if not isinstance(ollama_response, dict):
        raise OllamaUnavailable("Ollama returned an invalid response.")
    generated_text = ollama_response.get("response")
    if not isinstance(generated_text, str):
        raise OllamaUnavailable("Ollama response did not contain generated text.")
    if ollama_response.get("done_reason") == "length":
        logger.info("Ollama response reached its configured output limit.")
    return generated_text
