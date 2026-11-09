import json
import httpx
from .config import Settings


class LLMError(RuntimeError):
    pass


def extract_json_object(content: str) -> dict:
    """Parse a JSON object from model output, tolerating code fences and leading or trailing chatter."""
    text = content.strip()
    if text.startswith('```'):
        text = text.strip('`').removeprefix('json').strip()
    try:
        result = json.loads(text)
    except ValueError:
        start, end = text.find('{'), text.rfind('}')
        if start == -1 or end <= start:
            raise
        result = json.loads(text[start:end + 1])
    if not isinstance(result, dict):
        raise ValueError('Expected JSON object')
    return result


class NvidiaClient:
    attempts = 2

    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = httpx.AsyncClient(base_url=settings.nvidia_base_url.rstrip('/'), timeout=60)

    async def close(self):
        await self.client.aclose()

    async def json_completion(self, system: str, user: str) -> dict:
        if not self.settings.nvidia_api_key or not self.settings.nvidia_model:
            raise LLMError('NVIDIA_API_KEY and NVIDIA_MODEL are required in real mode')
        payload = {'model': self.settings.nvidia_model, 'messages': [{'role': 'system', 'content': system}, {'role': 'user', 'content': user}], 'temperature': 0, 'max_tokens': 3000, 'response_format': {'type': 'json_object'}}
        last: Exception | None = None
        for _ in range(self.attempts):
            try:
                response = await self.client.post('/chat/completions', json=payload, headers={'Authorization': f'Bearer {self.settings.nvidia_api_key}'})
                response.raise_for_status()
                choice = response.json()['choices'][0]
                content = choice['message']['content']
                if not isinstance(content, str):
                    raise ValueError('NVIDIA returned no text content')
                if choice.get('finish_reason') == 'length':
                    raise ValueError('reply was cut off at the token limit')
                return extract_json_object(content)
            except (httpx.HTTPError, KeyError, IndexError, ValueError, TypeError) as exc:
                last = exc
                if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in (400, 401, 403, 404, 410):
                    break  # retrying cannot fix a bad key, model or request
        raise LLMError(f'NVIDIA inference failed: {last}') from last
