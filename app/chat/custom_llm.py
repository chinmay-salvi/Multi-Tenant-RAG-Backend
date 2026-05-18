import urllib.request
import json
import os
import ssl
from typing import Any, Dict, Optional, Sequence
from llama_index.core.base.llms.types import (
    ChatMessage,
    ChatResponse,
    ChatResponseAsyncGen,
    CompletionResponse,
    CompletionResponseAsyncGen,
    LLMMetadata,
)
from llama_index.core.bridge.pydantic import Field
from llama_index.core.callbacks import CallbackManager
from llama_index.core.llms.callbacks import (
    llm_chat_callback,
    llm_completion_callback,
)
from llama_index.core.base.llms.generic_utils import (
    completion_response_to_chat_response,
)
from llama_index.core.llms.llm import LLM


def allowSelfSignedHttps(allowed):
    if (
        allowed
        and not os.environ.get("PYTHONHTTPSVERIFY", "")
        and getattr(ssl, "_create_unverified_context", None)
    ):
        ssl._create_default_https_context = ssl._create_unverified_context


class CustomLLM(LLM):
    url: str = Field(description="API endpoint URL")
    api_key: str = Field(description="API key for authentication")
    model_kwargs: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional kwargs for the model.",
    )

    def __init__(
        self,
        url: str,
        api_key: str,
        model_kwargs: Optional[Dict[str, Any]] = None,
        callback_manager: Optional[CallbackManager] = None,
        **kwargs: Any,
    ) -> None:
        model_kwargs = model_kwargs or {}
        callback_manager = callback_manager or CallbackManager([])
        allowSelfSignedHttps(True)

        super().__init__(
            url=url,
            api_key=api_key,
            model_kwargs=model_kwargs,
            callback_manager=callback_manager,
            **kwargs,
        )

    @llm_completion_callback()
    def complete(self, prompt: str, **kwargs: Any) -> CompletionResponse:
        messages = [{"role": "user", "content": prompt}]

        data = {
            "messages": messages,
            "max_tokens": kwargs.get("max_tokens", 1024),
            "temperature": kwargs.get("temperature", 0.5),
            "top_p": kwargs.get("top_p", 1),
            **self.model_kwargs,
            **kwargs,
        }

        body = str.encode(json.dumps(data))
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

        req = urllib.request.Request(self.url, body, headers)

        try:
            response = urllib.request.urlopen(req)
            result = json.loads(response.read().decode("utf-8"))
            completion = result["choices"][0]["message"]["content"]
            return CompletionResponse(text=completion, raw=result)
        except urllib.error.HTTPError as error:
            print(f"The request failed with status code: {error.code}")
            print(error.info())
            print(error.read().decode("utf8", "ignore"))
            raise

    @llm_chat_callback()
    def chat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        prompt = " ".join([f"{m.role}: {m.content}" for m in messages])
        completion_response = self.complete(prompt, **kwargs)
        return completion_response_to_chat_response(completion_response)

    @classmethod
    def class_name(cls) -> str:
        return "CustomLLM"

    @property
    def metadata(self) -> LLMMetadata:
        """LLM metadata."""
        return LLMMetadata(model_name="Custom LLM")

    def stream_complete(self, prompt: str, **kwargs: Any):
        raise NotImplementedError("Streaming is not implemented for this custom LLM.")

    def stream_chat(self, messages: Sequence[ChatMessage], **kwargs: Any):
        raise NotImplementedError("Streaming is not implemented for this custom LLM.")

    @llm_completion_callback()
    async def acomplete(self, prompt: str, **kwargs: Any) -> CompletionResponse:
        return self.complete(prompt, **kwargs)

    @llm_chat_callback()
    async def achat(
        self, messages: Sequence[ChatMessage], **kwargs: Any
    ) -> ChatResponse:
        return self.chat(messages, **kwargs)

    async def astream_chat(
        self, messages: Sequence[ChatMessage], **kwargs: Any
    ) -> ChatResponseAsyncGen:
        raise NotImplementedError(
            "Async streaming chat is not implemented for this custom LLM."
        )

    async def astream_complete(
        self, prompt: str, **kwargs: Any
    ) -> CompletionResponseAsyncGen:
        raise NotImplementedError(
            "Async streaming completion is not implemented for this custom LLM."
        )
