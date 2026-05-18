import logging
from uuid import UUID

import aiofiles
from fastapi import (
    APIRouter,
    Depends,
    UploadFile,
    Form,
    File,
    Header,
    Request,
    HTTPException,
)
from sqlalchemy.ext.asyncio import AsyncSession

from app import schema
from app.api.crud_helper import (
    fetch_chatbot_directly,
    fetch_existing_user,
    fetch_tickets,
    fetch_ticket,
    fetch_customer_tickets_email,
)
from app.api.deps import get_db
from app.api.s3_helper import upload_file_to_s3
from app.auth import validate_user
from app.db.tables import Tickets
from app.schema import TicketStatusChangeRequest

logger = logging.getLogger(__name__)

ticket_router = APIRouter()


# Endpoint to submit a ticket
@ticket_router.post("/submit_chatbot_ticket")
async def submit_chatbot_ticket(
    request: Request,
    chatbotId: UUID = Form(...),
    category: str = Form(...),
    name: str = Form(...),
    email: str = Form(...),
    issue: str = Form(...),
    userId: UUID = Form(None),
    authorization: str = Header(None),
    file: UploadFile = File(None),
    db: AsyncSession = Depends(get_db),
):
    chatbot = await fetch_chatbot_directly(db=db, chatbot_id=chatbotId)

    if chatbot:
        if not chatbot.isPublic:
            if authorization and userId:
                token_payload = await validate_user(request, authorization)
                if userId != UUID(token_payload["userId"]):
                    raise HTTPException(status_code=401, detail="Unauthorized")
            else:
                raise HTTPException(status_code=401, detail="Unauthorized")

        org_id = chatbot.orgId

        try:
            # Create a new ticket
            ticket = Tickets(
                chatbotId=chatbotId,
                category=category,
                status="Open",
                name=name,
                email=email,
                issue=issue,
                priority="Medium",
                fileType="",
                orgId=org_id,
            )

            # Add the ticket to the database session and commit
            db.add(ticket)
            await db.commit()
            await db.refresh(ticket)
            ticket_id = ticket.ticketId

            if file:
                async with aiofiles.tempfile.TemporaryDirectory() as temp_dir:
                    # Save the uploaded file to a temporary location
                    ticket_filepath = (
                        f"{temp_dir}/{chatbotId}_{ticket.ticketId}_{file.filename}"
                    )
                    async with aiofiles.open(ticket_filepath, "wb") as temp_ticket_file:
                        await temp_ticket_file.write(await file.read())

                    # Upload the file to S3 using the async function
                    ticket.fileURL = await upload_file_to_s3(
                        uploaded_file_path=ticket_filepath,
                        s3_key=f"{org_id}/tickets/{ticket_id}.{file.filename.split('.')[-1].lower()}",
                    )
                await db.commit()
            return {"message": "Ticket submitted successfully", "ticket_id": ticket_id}
        except Exception as e:
            # Rollback in case of any error
            await db.rollback()
            print("An unexpected error occurred:", e)
            return {"message": "An unexpected error occurred", "ticket_id": None}

    return {"message": "No such chatbot found", "ticket_id": None}


@ticket_router.get("/get_tickets")
async def get_tickets(
    userId: UUID,
    chatbotId: UUID | None = None,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )
    try:
        tickets = await fetch_tickets(
            org_id=token_payload["orgId"], chatbot_id=chatbotId, db=db
        )
        return {
            "userId": token_payload["userId"],
            "tickets": [
                {
                    "id": f"#ST-{str(ticket.ticketId)}",
                    "createdTime": ticket.createdTime.strftime("%d %B %Y,%I:%M %p"),
                    "name": ticket.name,
                    "email": ticket.email,
                    "issue": ticket.issue,
                    "status": ticket.status,
                    "category": ticket.category,
                    "priority": ticket.priority,
                    "chatbotId": ticket.chatbotId,
                }
                for ticket_index, ticket in enumerate(tickets)
            ],
        }
    except Exception as e:
        # Rollback in case of any error
        await db.rollback()
        print("An unexpected error occurred:", e)
        return {
            "message": "An unexpected error occurred",
            "userId": token_payload["userId"],
            "tickets": None,
        }


@ticket_router.post("/change_ticket_status")
async def change_ticket_status(
    body: TicketStatusChangeRequest,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )
    ticket_id = int(body.ticketId[4:])

    ticket = await fetch_ticket(
        db=db,
        ticket_id=ticket_id,
        org_id=token_payload["orgId"],
        chatbot_id=body.chatbotId,
    )

    if ticket:
        try:
            ticket.status = body.status
            await db.commit()
            return {
                "message": "Ticket status changed successfully",
                "userId": token_payload["userId"],
            }

        except Exception as e:
            # Rollback in case of any error
            await db.rollback()
            print("An unexpected error occurred:", e)
            return {
                "message": "An unexpected error occurred",
                "userId": token_payload["userId"],
            }

    return {
        "message": "No such ticket found",
        "userId": token_payload["userId"],
    }


@ticket_router.post("/ticket_data_using_email")
async def get_ticket_data_using_email(
    body: schema.TicketUsingEmailRequest,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    customer_tickets = await fetch_customer_tickets_email(
        db=db,
        customer_email=body.email,
        org_id=token_payload["orgId"],
        chatbot_id=body.chatbotId,
    )

    if customer_tickets:
        try:
            return schema.TicketUsingEmailResponse(
                name=customer_tickets[-1].name,
                email=body.email,
                userProfilePicUrl="https://github.com/shadcn.png",
                userFirstContactDateTime=customer_tickets[-1].createdTime.strftime(
                    "%d %B %Y,%I:%M %p"
                ),
                userLanguage="Undetected",
                tickets=[
                    {
                        "id": f"#ST-{str(customer_ticket.ticketId)}",
                        "createdTime": customer_ticket.createdTime.strftime(
                            "%d %B %Y,%I:%M %p"
                        ),
                        "issue": customer_ticket.issue,
                        "status": customer_ticket.status,
                        "category": customer_ticket.category,
                        "priority": customer_ticket.priority,
                        "chatbotId": customer_ticket.chatbotId,
                        "closedTime": (
                            customer_ticket.closedTime.strftime("%d %B %Y,%I:%M %p")
                            if customer_ticket.closedTime
                            else None
                        ),
                    }
                    for customer_ticket_index, customer_ticket in enumerate(
                        customer_tickets
                    )
                ],
            )

        except Exception as e:
            # Rollback in case of any error
            await db.rollback()
            print("An unexpected error occurred:", e)
            return {
                "message": "An unexpected error occurred",
                "userId": token_payload["userId"],
                "email": body.email,
                "tickets": None,
            }

    return {
        "message": "No such customer was found with tickets associated with this chatbot.",
        "userId": token_payload["userId"],
        "email": body.email,
        "tickets": [],
    }
