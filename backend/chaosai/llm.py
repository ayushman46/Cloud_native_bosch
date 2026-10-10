import json
import httpx
from .config import Settings


class LLMError(RuntimeError):
    pass


class NvidiaClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = httpx.AsyncClient(base_url=settings.nvidia_base_url.rstrip('/'), timeout=25)

    async def close(self):
        await self.client.aclose()

    async def json_completion(self, system: str, user: str) -> dict:
        if not self.settings.nvidia_api_key or not self.settings.nvidia_model:
            raise LLMError('NVIDIA_API_KEY and NVIDIA_MODEL are required in real mode')
        payload = {'model': self.settings.nvidia_model, 'messages': [{'role': 'system', 'content': system}, {'role': 'user', 'content': user}], 'temperature': 0, 'max_tokens': 1000, 'response_format': {'type': 'json_object'}}
        try:
            response = await self.client.post('/chat/completions', json=payload, headers={'Authorization': f'Bearer {self.settings.nvidia_api_key}'})
            response.raise_for_status()
            content = response.json()['choices'][0]['message']['content']
            if not isinstance(content, str):
                raise ValueError('NVIDIA returned no text content')
            if content.startswith('```'):
                content = content.strip('`').removeprefix('json').strip()
            result = json.loads(content)
            if not isinstance(result, dict):
                raise ValueError('Expected JSON object')
            return result
        except (httpx.HTTPError, KeyError, IndexError, ValueError, TypeError) as exc:
            raise LLMError(f'NVIDIA inference failed: {exc}') from exc
