import asyncio
import datetime
import logging
from typing import Dict, Any, List

from anyio import ClosedResourceError
from anyio.streams.memory import MemoryObjectSendStream
from llama_index.core.callbacks import CBEventType, EventPayload
from llama_index.core.callbacks.base import BaseCallbackHandler
from llama_index.core.query_engine import SubQuestionAnswerPair

from app.message import schemas as schema


logger = logging.getLogger(__name__)


class ChatCallbackHandler(BaseCallbackHandler):
    """
    A custom LlamaIndex callback handler that intercepts execution traces to stream them via SSE.

    Inherits from LlamaIndex's BaseCallbackHandler to intercept internal pipeline traces
    (like query engine initiation, sub-question splits, and LLM steps).
    Converts these traces into serializable `MessageSubProcess` definitions and pushes them to
    the active token memory send channel (`_send_chan`).
    """

    def __init__(
        self,
        send_chan: MemoryObjectSendStream,
    ):
        """
        Initializes the event callback listener.

        Args:
            send_chan (MemoryObjectSendStream): Target memory channel to push execution traces.
        """
        ignored_events = [CBEventType.CHUNKING, CBEventType.NODE_PARSING]
        super().__init__(ignored_events, ignored_events)
        self._send_chan = send_chan

    def on_event_start(
        self,
        event_type: CBEventType,
        payload: Dict[str, Any] | None = None,
        event_id: str = "",
        **kwargs: Any,
    ) -> None:
        """
        Triggered when a LlamaIndex execution event begins.

        Spawns an asynchronous background task to stream the initiation event.
        """
        print("started", event_type, event_id, datetime.datetime.now())
        asyncio.create_task(
            self.async_on_event(
                event_type, payload, event_id, is_start_event=True, **kwargs
            )
        )

    def on_event_end(
        self,
        event_type: CBEventType,
        payload: Dict[str, Any] | None = None,
        event_id: str = "",
        **kwargs: Any,
    ) -> None:
        """
        Triggered when a LlamaIndex execution event finishes.

        Spawns an asynchronous background task to stream the completion event.
        """
        print("ended", event_type, event_id, datetime.datetime.now())
        asyncio.create_task(
            self.async_on_event(
                event_type, payload, event_id, is_start_event=False, **kwargs
            )
        )

    @staticmethod
    def get_metadata_from_event(
        event_type: CBEventType,
        payload: Dict[str, Any] | None = None,
        is_start_event: bool = False,
    ) -> schema.SubProcessMetadataMap:
        """
        Extracts execution payloads (such as SubQuestion splits) to stream them in metadata maps.

        Args:
            event_type (CBEventType): Event description classification.
            payload (Dict[str, Any], optional): The raw execution payload mapping.
            is_start_event (bool): True if start event; False if end.

        Returns:
            schema.SubProcessMetadataMap: Mapped Pydantic-serializable sub-process details.
        """
        metadata_map = {}

        if (
            event_type == CBEventType.SUB_QUESTION
            and EventPayload.SUB_QUESTION in payload
        ):
            sub_q: SubQuestionAnswerPair = payload[EventPayload.SUB_QUESTION]
            metadata_map[schema.SubProcessMetadataKeysEnum.SUB_QUESTION.value] = (
                schema.QuestionAnswerPair.from_sub_question_answer_pair(sub_q).dict()
            )
        return metadata_map

    async def async_on_event(
        self,
        event_type: CBEventType,
        payload: Dict[str, Any] | None = None,
        event_id: str = "",
        is_start_event: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        Serializes the LlamaIndex trace event and sends it to the active AnyIO channel.

        Args:
            event_type (CBEventType): The intercepted trace event classification.
            payload (Dict[str, Any], optional): The raw payload detail.
            event_id (str): Unique trace ID.
            is_start_event (bool): Identifies if the process is starting or finishing.
        """
        metadata_map = (
            self.get_metadata_from_event(
                event_type, payload=payload, is_start_event=is_start_event
            )
            or None
        )

        source = schema.MessageSubProcessSourceEnum[event_type.name]

        # Avoid pushing tokens if the client has already disconnected
        if self._send_chan._closed:
            logger.debug("Received event after send channel closed. Ignoring.")
            return
        try:
            # Stream the serialized subprocess metadata
            await self._send_chan.send(
                schema.StreamedMessageSubProcess(
                    source=source,
                    metadata_map=metadata_map,
                    event_id=event_id,
                    has_ended=not is_start_event,
                )
            )
        except ClosedResourceError:
            logger.exception(
                "Tried sending SubProcess event %s after channel was closed",
                f"(source={source})",
            )

    def start_trace(self, trace_id: str | None = None) -> None:
        """No-op tracing hook."""

    def end_trace(
        self,
        trace_id: str | None = None,
        trace_map: Dict[str, List[str]] | None = None,
    ) -> None:
        """No-op tracing hook."""
