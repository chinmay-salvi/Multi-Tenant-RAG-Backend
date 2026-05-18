from datetime import MAXYEAR, MINYEAR, datetime
import logging
from uuid import UUID, uuid4
from app.schema import SaveLeadFormRequest, SaveLeadRequest
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.crud_helper import (
    fetch_existing_user,
    fetch_chatbot_lead_form,
    fetch_leads,
)
from app.api.deps import get_db
from app.auth import validate_user
from app.db.tables import LeadForm, Lead


logger = logging.getLogger(__name__)


lead_router = APIRouter()


@lead_router.post("/save_lead_form")
async def save_lead_form(
    body: SaveLeadFormRequest,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        # Ensure the user exists
        await fetch_existing_user(
            org_id=token_payload["orgId"],
            user_id=token_payload["userId"],
            db=db,
            ensure=True,
        )

        # Fetch the chatbot
        chatbot = await fetch_chatbot_lead_form(
            org_id=token_payload["orgId"], chatbot_id=body.chatbotId, db=db
        )

        if not chatbot:
            return {"success": False, "message": "Chatbot does not exist"}

        chatbot.isLeadFormEnabled = body.isLeadFormEnabled

        # Update or create lead form template if necessary
        new_form_required = False

        if body.isLeadFormEnabled:
            if not (
                len(body.leadFormKeys)
                and len(body.leadFormKeys)
                == len(body.leadFormLabels)
                == len(body.leadFormInputTypes)
            ):
                return {"success": False, "message": "Invalid input."}

            if not chatbot.latestLeadForm:
                new_form_required = True
            else:
                new_form_required = (
                    chatbot.latestLeadForm.title != body.leadFormTitle
                    or chatbot.latestLeadForm.keys != body.leadFormKeys
                    or chatbot.latestLeadForm.labels != body.leadFormLabels
                    or chatbot.latestLeadForm.inputTypes != body.leadFormInputTypes
                    or chatbot.latestLeadForm.frequencyHours != body.frequencyHours
                    or chatbot.latestLeadForm.maxShowLimit != body.maxShowLimit
                )

        if new_form_required:
            new_form = LeadForm(
                leadFormId=uuid4(),
                title=body.leadFormTitle,
                keys=body.leadFormKeys,
                labels=body.leadFormLabels,
                inputTypes=body.leadFormInputTypes,
                frequencyHours=body.frequencyHours,
                maxShowLimit=body.maxShowLimit,
                chatbotId=chatbot.chatbotId,
                orgId=chatbot.orgId,
            )
            chatbot.latestLeadForm = new_form
            chatbot.latestLeadFormId = new_form.leadFormId
            db.add(new_form)

        await db.commit()

        return {"success": True, "message": "Lead form saved successfully."}

    except Exception as e:
        print("Unexpected Exception occurred while saving lead form: ", e)
        return {"success": False, "message": "Failed to save lead form."}


@lead_router.post("/save_lead")
async def save_lead(
    body: SaveLeadRequest,
    db: AsyncSession = Depends(get_db),
):
    try:
        # Fetch the chatbot along with lead form
        chatbot = await fetch_chatbot_lead_form(chatbot_id=body.chatbotId, db=db)

        if not chatbot:
            return {"success": False, "message": "Chatbot does not exist"}

        if not (chatbot.isLeadFormEnabled and chatbot.latestLeadForm):
            return {"success": False, "message": "Functionality not available."}

        form = chatbot.latestLeadForm
        if not (
            body.leadFormKeys
            and len(body.leadFormKeys)
            == len(body.leadFormLabels)
            == len(body.leadFormValues)
            == len(form.keys)
        ):
            return {"success": False, "message": "Invalid input."}

        data = dict()

        for index, (key, label) in enumerate(zip(form.keys, form.labels)):
            if body.leadFormKeys[index] != key or body.leadFormLabels[index] != label:
                return {"success": False, "message": "Invalid input."}
            data[key] = body.leadFormValues[index]

        db.add(
            Lead(
                leadFormId=form.leadFormId,
                data=data,
                conversationId=body.conversationId,
                chatbotId=chatbot.chatbotId,
                orgId=chatbot.orgId,
            )
        )

        await db.commit()

        return {"success": True, "message": "Lead saved successfully."}

    except Exception as e:
        print("Unexpected Exception occurred while saving lead: ", e)
        import traceback

        traceback.print_exc()
        return {
            "success": False,
            "message": "Failed to save data due to unexpected error.",
        }


@lead_router.get("/retrieve_leads")
async def retrieve_leads(
    userId: UUID,
    chatbotId: UUID,
    fromDate: datetime = datetime(MINYEAR, 1, 1),
    toDate: datetime = datetime(MAXYEAR, 12, 31, 23, 59, 59, 999999),
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    from_date = fromDate.replace(tzinfo=None) if fromDate.tzinfo else fromDate
    to_date = toDate.replace(tzinfo=None) if toDate.tzinfo else toDate

    try:
        leads = await fetch_leads(
            org_id=token_payload["orgId"],
            chatbot_id=chatbotId,
            from_date=from_date,
            to_date=to_date,
            db=db,
        )

        return {
            "message": "Leads fetched successfully.",
            "success": True,
            "userId": token_payload["userId"],
            "leads": [
                {
                    "leadValues": [
                        lead.data.get(key, None) for key in lead.leadForm.keys
                    ],
                    "leadKeys": lead.leadForm.keys,
                    "createdAt": lead.createdAt,
                }
                for lead in leads
            ],
        }
    except Exception as e:
        # Rollback in case of any error
        await db.rollback()
        print("An unexpected error occurred while fetching leads:", e)
        return {
            "message": "An unexpected error has occurred.",
            "sucess": False,
            "userId": token_payload["userId"],
            "leads": None,
        }
