from fastapi import APIRouter
from app.api.endpoints.chatbot import chatbot_router
from app.api.endpoints.conversation import conversation_router
from app.api.endpoints.datafeed import datafeed_router
from app.api.endpoints.message import message_router
from app.api.endpoints.tags import tags_router
from app.api.endpoints.ticket import ticket_router
from app.api.endpoints.lead import lead_router
from app.api.endpoints.payments import payments_router

api_router = APIRouter(prefix="/v1")


api_router.include_router(chatbot_router, tags=["chatbots"])
api_router.include_router(conversation_router, tags=["conversation"])
api_router.include_router(datafeed_router, tags=["datafeed"])
api_router.include_router(lead_router, tags=["lead"])
api_router.include_router(message_router, tags=["message"])
api_router.include_router(tags_router, tags=["tags"])
api_router.include_router(ticket_router, tags=["ticket"])
api_router.include_router(payments_router, tags=["payments"])