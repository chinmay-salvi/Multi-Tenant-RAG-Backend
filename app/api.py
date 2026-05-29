from fastapi import APIRouter
from app.chatbot.router import chatbot_router
from app.datafeed.router import datafeed_router
from app.message.router import message_router
from app.user_org.router import user_org_router
from app.ticket.router import ticket_router
from app.lead.router import lead_router
from app.payments.router import payments_router

api_router = APIRouter(prefix="/v1")


api_router.include_router(chatbot_router, tags=["chatbots"])
api_router.include_router(datafeed_router, tags=["datafeed"])
api_router.include_router(lead_router, tags=["lead"])
api_router.include_router(message_router, tags=["message"])
api_router.include_router(user_org_router, tags=["tags"])
api_router.include_router(ticket_router, tags=["ticket"])
api_router.include_router(payments_router, tags=["payments"])
