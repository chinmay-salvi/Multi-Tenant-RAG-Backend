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

from app.core.deps import get_db
from app.utils.s3_helper import upload_file_to_s3
from app.db.tables import Tickets, Organization
from sqlalchemy import update
from app.auth import validate_user
from app.user_org.crud import fetch_existing_user
from app.chatbot.crud import fetch_chatbot_directly

from .schemas import (
    TicketStatusChangeRequest,
    TicketUsingEmailRequest,
    TicketUsingEmailResponse,
)
from .crud import (
    fetch_tickets,
    fetch_ticket,
    fetch_customer_tickets_email,
)

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
    """
    Submits a support ticket associated with a specific chatbot.

    For private chatbots, JWT validation is executed to assert tenant identity constraints.
    Saves the ticket parameters and triggers an asynchronous file upload to AWS S3 if an attachment
    is provided.

    Args:
        request (Request): Active HTTP request object.
        chatbotId (UUID): Associated Chatbot ID.
        category (str): Ticket category (e.g. billing, technical).
        name (str): Submitter name.
        email (str): Submitter email.
        issue (str): Narrative description of the issue.
        userId (UUID, optional): JWT validation parameters checked for private bots.
        authorization (str, optional): HTTP Bearer token checked for private bots.
        file (UploadFile, optional): Optional ticket attachment file.
        db (AsyncSession): Active SQLAlchemy database session.

    Returns:
        dict: A JSON response containing 'message' and the generated 'ticket_id'.
    """
    # Retrieve the target chatbot configuration
    chatbot = await fetch_chatbot_directly(db=db, chatbot_id=chatbotId)

    if chatbot:
        # Enforce security constraints for private chatbots
        if not chatbot.isPublic:
            if authorization and userId:
                token_payload = await validate_user(request, authorization)
                if userId != UUID(token_payload["userId"]):
                    raise HTTPException(status_code=401, detail="Unauthorized")
            else:
                raise HTTPException(status_code=401, detail="Unauthorized")

        org_id = chatbot.orgId

        try:
            # Atomically increment organization counter & fetch new sequence ID
            result = await db.execute(
                update(Organization)
                .where(Organization.orgId == org_id)
                .values(lastTicketSequence=Organization.lastTicketSequence + 1)
                .returning(Organization.lastTicketSequence)
            )
            ticket_id = result.scalar_one()

            # Create a new ticket row with the explicitly set ticketId
            ticket = Tickets(
                ticketId=ticket_id,
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

            # Add and flush the ticket to obtain the generated sequential key
            db.add(ticket)
            await db.commit()
            await db.refresh(ticket)

            # Handle attachment uploads asynchronously using S3 helper utilities
            if file:
                async with aiofiles.tempfile.TemporaryDirectory() as temp_dir:
                    # Save the uploaded file to a temporary location
                    ticket_filepath = (
                        f"{temp_dir}/{chatbotId}_{ticket.ticketId}_{file.filename}"
                    )
                    async with aiofiles.open(ticket_filepath, "wb") as temp_ticket_file:
                        await temp_ticket_file.write(await file.read())

                    # Upload to S3, storing the URL mapping inside the database row
                    ticket.fileURL = await upload_file_to_s3(
                        uploaded_file_path=ticket_filepath,
                        s3_key=f"{org_id}/tickets/{ticket_id}.{file.filename.split('.')[-1].lower()}",
                    )
                await db.commit()
            return {"message": "Ticket submitted successfully", "ticket_id": ticket_id}
        except Exception as e:
            # Rollback transaction on failure
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
    """
    Retrieves all support tickets logged under a tenant organization.

    Optional filtering by chatbotId.

    Args:
        userId (UUID): Requesting user ID.
        chatbotId (UUID, optional): Filter by associated chatbot.
        token_payload (dict): Decoded and verified tenant token.
        db (AsyncSession): Active database session.

    Returns:
        dict: A JSON response containing the list of serialized tickets.
    """
    # Assert requesting user belongs to the tenant organization
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )
    try:
        # Fetch organization-scoped tickets
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
        # Rollback database changes on failure
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
    """
    Modifies the status of a specific support ticket.

    Args:
        body (TicketStatusChangeRequest): Unique ticket identifier (e.g. #ST-1) and target status string.
        token_payload (dict): Decoded and verified tenant JWT.
        db (AsyncSession): Active database session.

    Returns:
        dict: A success or error JSON message status mapping.
    """
    # Assert requesting user belongs to the tenant organization
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )
    # Parse the custom sequence integer from the prefix "#ST-"
    ticket_id = int(body.ticketId[4:])

    # Query target ticket matching composite organization constraints
    ticket = await fetch_ticket(
        db=db,
        ticket_id=ticket_id,
        org_id=token_payload["orgId"],
        chatbot_id=body.chatbotId,
    )

    if ticket:
        try:
            # Update and persist ticket status
            ticket.status = body.status
            await db.commit()
            return {
                "message": "Ticket status changed successfully",
                "userId": token_payload["userId"],
            }

        except Exception as e:
            # Rollback database changes on failure
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
    body: TicketUsingEmailRequest,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Queries and returns all tickets filed by a single customer based on their email.

    This enables customer support executives to immediately get a historical digest of support cases
    associated with a specific client email within their tenant environment.

    Args:
        body (TicketUsingEmailRequest): Target email string to query.
        token_payload (dict): Decoded and verified tenant JWT.
        db (AsyncSession): Active database session.

    Returns:
        TicketUsingEmailResponse: conformant object containing support history
    """
    # Assert requesting user belongs to the tenant organization
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    # Query customer tickets matching email and organization scoping
    customer_tickets = await fetch_customer_tickets_email(
        db=db,
        customer_email=body.email,
        org_id=token_payload["orgId"],
        chatbot_id=body.chatbotId,
    )

    if customer_tickets:
        try:
            # Return serialized digest of matching support history
            return TicketUsingEmailResponse(
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
            # Rollback database changes on failure
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
