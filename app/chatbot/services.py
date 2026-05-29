import base64
import asyncio
import json
import logging
import mimetypes
from typing import Optional, List
from uuid import UUID, uuid4

import aiofiles
from fastapi import UploadFile, HTTPException

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.tables import (
    Chatbot,
    ChatbotDataFeeds,
    ChatbotSupportTicketingCategories,
)
from app.payments.plans_helper import check_chatbot_limit
from app.utils.s3_helper import upload_file_to_s3
from app.datafeed.crud import fetch_datafeeds_by_ids
from app.datafeed.datafeed_helper import modify_chatbot_id_in_nodes
from app.user_org.crud import fetch_agent_type

from .crud import (
    count_existing_chatbot,
    fetch_existing_chatbot,
    fetch_chatbot_datafeeds,
    fetch_support_ticketing_categories_for_chatbot,
)

logger = logging.getLogger(__name__)


def normalize_field(value):
    return None if value == '""' else value


async def create_chatbot_instance(
    db: AsyncSession,
    org_id: UUID,
    chatbot_name: str,
    chatbot_intro_messages: List[str],
    data_feed_ids: List[UUID],
) -> dict:
    """
    Business logic for creating a versioned new AI chatbot agent under a tenant organization.
    Coordinates plan limit validation, duplicate checks, datafeed verification, pgvector sync, and DB persistence.
    """
    # Fetch current active chatbot deployments under the tenant
    chatbot_count = await count_existing_chatbot(org_id=org_id, db=db)

    message = await asyncio.to_thread(
        check_chatbot_limit, org_id, chatbot_count
    )

    if message:
        return {
            "message": message,
            "success": False,
            "chatbotId": None,
        }

    # Prevent duplicate naming conflicts
    existing_chatbot = await fetch_existing_chatbot(
        chatbot_name=chatbot_name, org_id=org_id, db=db
    )

    if existing_chatbot:
        return {
            "message": "Agent with provided name already exists.",
            "success": False,
            "chatbotId": None,
        }

    # Retrieve all valid, fully processed active knowledge datafeeds matching body IDs
    data_feeds = await fetch_datafeeds_by_ids(
        org_id=org_id,
        data_feed_ids=data_feed_ids,
        processed_only=True,
        db=db,
    )

    agent_type = await fetch_agent_type(
        agent_type_name="Support",
        db=db,
    )

    if not agent_type:
        return {
            "message": "No such agent type found.",
            "success": False,
            "chatbotId": None,
        }

    if len(data_feeds) != len(data_feed_ids):
        return {
            "message": "All data feeds not found.",
            "success": False,
            "chatbotId": None,
        }

    try:
        chatbot_id = uuid4()
        # Propagate changes to vector db nodes to tie them instantly to this new chatbot ID
        result = modify_chatbot_id_in_nodes(
            org_id=org_id,
            chatbot_id=str(chatbot_id),
            remove_datafeeds=set(),
            additional_datafeeds=set(map(str, data_feed_ids)),
        )

        if result:
            db.add(
                Chatbot(
                    chatbotId=chatbot_id,
                    agentTypeId=agent_type.agentTypeId,
                    chatbotName=chatbot_name,
                    orgId=org_id,
                    chatbotIntroMessages=[
                        intro_message
                        for intro_message in chatbot_intro_messages
                        if isinstance(intro_message, str)
                    ],
                )
            )
            db.add_all(
                [
                    ChatbotDataFeeds(chatbotId=chatbot_id, dataFeedId=data_feed_id)
                    for data_feed_id in data_feed_ids
                ]
            )
            await db.commit()
            return {
                "message": f"Agent '{chatbot_name}' created successfully.",
                "success": True,
                "chatbotId": chatbot_id,
            }
        else:
            return {
                "message": "Agent creation failed.",
                "success": False,
                "chatbotId": None,
            }
    except Exception as e:
        await db.rollback()
        logger.error(f"An unexpected error occurred during chatbot creation: {e}")
        return {
            "message": f"An unexpected error occurred: {e}",
            "success": False,
            "chatbotId": None,
        }


async def update_chatbot_configuration(
    db: AsyncSession,
    org_id: UUID,
    chatbot_id: UUID,
    chatbot_name: Optional[str] = None,
    chatbot_banner_message: Optional[str] = None,
    chatbot_intro_message: Optional[str] = None,
    chat_with_us_title: Optional[str] = None,
    chat_with_us_description: Optional[str] = None,
    raise_ticket_title: Optional[str] = None,
    raise_ticket_description: Optional[str] = None,
    chatbot_example_user_questions: Optional[str] = None,
    chatbot_user_message_color: Optional[str] = None,
    chatbot_for: Optional[str] = None,
    company_do: Optional[str] = None,
    hallucination_fixer: Optional[str] = None,
    business_contact_details: Optional[str] = None,
    chatbot_ticketing_visibility: Optional[str] = None,
    is_public: Optional[str] = None,
    support_ticketing_category_title_1: Optional[str] = None,
    support_ticketing_category_description_1: Optional[str] = None,
    support_ticketing_category_title_2: Optional[str] = None,
    support_ticketing_category_description_2: Optional[str] = None,
    support_ticketing_category_title_3: Optional[str] = None,
    support_ticketing_category_description_3: Optional[str] = None,
    company_logo: Optional[UploadFile] = None,
    widget_logo: Optional[UploadFile] = None,
) -> dict:
    """
    Business logic for updating configuration details of an existing chatbot widget.
    Coordinates S3 image uploads, base64 conversions, and support category syncs.
    """
    existing_chatbot = await fetch_existing_chatbot(
        chatbot_id=chatbot_id, org_id=org_id, db=db
    )

    if not existing_chatbot:
        return {"success": False, "message": "Chatbot does not exist"}

    def str_to_bool(value: Optional[str]) -> Optional[bool]:
        if value is None:
            return None
        if value.lower() in ["true", "1"]:
            return True
        if value.lower() in ["false", "0"]:
            return False
        raise ValueError(f"Cannot convert {value} to boolean")

    try:
        chatbot_ticketing_visibility_bool = str_to_bool(chatbot_ticketing_visibility)
        is_public_bool = str_to_bool(is_public)
    except ValueError as val_err:
        raise HTTPException(status_code=400, detail=str(val_err))

    # Update fields only if they are provided
    update_data = {
        "chatbotName": chatbot_name,
        "chatbotBannerMessage": chatbot_banner_message,
        "chatWithUsTitle": chat_with_us_title,
        "chatWithUsDescription": chat_with_us_description,
        "raiseTicketTitle": raise_ticket_title,
        "raiseTicketDescription": raise_ticket_description,
        "chatbotUserMessageColor": chatbot_user_message_color,
        "chatbotFor": chatbot_for,
        "companyDo": company_do,
        "hallucinationFixer": hallucination_fixer,
        "businessContactDetails": business_contact_details,
        "chatbotTicketingVisibility": chatbot_ticketing_visibility_bool,
        "isPublic": is_public_bool,
    }

    for key, value in update_data.items():
        if value is not None:
            if value == '""':
                setattr(existing_chatbot, key, None)
            else:
                setattr(existing_chatbot, key, value)

    if chatbot_intro_message:
        try:
            parsed_intro_messages = json.loads(chatbot_intro_message)
            if not isinstance(parsed_intro_messages, list) or not all(
                isinstance(item, str) for item in parsed_intro_messages
            ):
                raise ValueError("chatbotIntroMessage must be a list of strings")
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=400, detail="Invalid JSON format in chatbotIntroMessage"
            )
        existing_chatbot.chatbotIntroMessages = parsed_intro_messages

    if chatbot_example_user_questions:
        try:
            parsed_example_questions = json.loads(chatbot_example_user_questions)
            if not isinstance(parsed_example_questions, list) or not all(
                isinstance(item, str) for item in parsed_example_questions
            ):
                raise ValueError(
                    "chatbotExampleUserQuestions must be a list of strings"
                )
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=400,
                detail="Invalid JSON format in chatbotExampleUserQuestions",
            )
        existing_chatbot.chatbotExampleUserQuestions = parsed_example_questions

    # Handle file uploads
    async with aiofiles.tempfile.TemporaryDirectory() as temp_dir:
        if company_logo:
            company_logo_filepath = f"{temp_dir}/{chatbot_id}_{company_logo.filename}"
            async with aiofiles.open(
                company_logo_filepath, "wb"
            ) as temp_company_logo_file:
                company_logo_content = await company_logo.read()
                await temp_company_logo_file.write(company_logo_content)

            company_logo_mime_type, _ = mimetypes.guess_type(company_logo_filepath)
            if company_logo_mime_type is None:
                company_logo_mime_type = "application/octet-stream"

            existing_chatbot.companyLogoBase64 = f"data:{company_logo_mime_type};base64,{base64.b64encode(company_logo_content).decode('utf-8')}"

            existing_chatbot.companyLogo = await upload_file_to_s3(
                uploaded_file_path=company_logo_filepath,
                s3_key=f"{org_id}/{chatbot_id}/chatbot_data/company_logo.{company_logo.filename.split('.')[-1].lower()}",
            )
        if widget_logo:
            widget_logo_filepath = f"{temp_dir}/{chatbot_id}_{widget_logo.filename}"
            async with aiofiles.open(
                widget_logo_filepath, "wb"
            ) as temp_widget_logo_file:
                widget_logo_content = await widget_logo.read()
                await temp_widget_logo_file.write(widget_logo_content)

            widget_logo_mime_type, _ = mimetypes.guess_type(widget_logo_filepath)
            if widget_logo_mime_type is None:
                widget_logo_mime_type = "application/octet-stream"

            existing_chatbot.widgetLogoBase64 = f"data:{widget_logo_mime_type};base64,{base64.b64encode(widget_logo_content).decode('utf-8')}"

            existing_chatbot.widgetLogo = await upload_file_to_s3(
                uploaded_file_path=widget_logo_filepath,
                s3_key=f"{org_id}/{chatbot_id}/chatbot_data/widget_logo.{widget_logo.filename.split('.')[-1].lower()}",
            )

    db.add(existing_chatbot)
    await db.commit()
    await db.refresh(existing_chatbot)

    # Update support ticketing categories only if they are provided
    support_ticketing_categories = await fetch_support_ticketing_categories_for_chatbot(
        chatbot_id=chatbot_id, db=db
    )
    new_support_ticketing_categories = []

    if support_ticketing_category_title_1 and support_ticketing_category_description_1:
        if len(support_ticketing_categories) > 0:
            support_ticketing_categories[0][0].supportTicketingCategoryTitle = (
                normalize_field(support_ticketing_category_title_1)
            )
            support_ticketing_categories[0][0].supportTicketingCategoryDescription = (
                normalize_field(support_ticketing_category_description_1)
            )
        else:
            new_support_ticketing_categories.append(
                ChatbotSupportTicketingCategories(
                    supportTicketingCategoryTitle=normalize_field(
                        support_ticketing_category_title_1
                    ),
                    supportTicketingCategoryDescription=normalize_field(
                        support_ticketing_category_description_1
                    ),
                    supportTicketingCategorySequence=1,
                    chatbotId=existing_chatbot.chatbotId,
                )
            )

    if support_ticketing_category_title_2 and support_ticketing_category_description_2:
        if len(support_ticketing_categories) > 1:
            support_ticketing_categories[1][0].supportTicketingCategoryTitle = (
                normalize_field(support_ticketing_category_title_2)
            )
            support_ticketing_categories[1][0].supportTicketingCategoryDescription = (
                normalize_field(support_ticketing_category_description_2)
            )
        else:
            new_support_ticketing_categories.append(
                ChatbotSupportTicketingCategories(
                    supportTicketingCategoryTitle=normalize_field(
                        support_ticketing_category_title_2
                    ),
                    supportTicketingCategoryDescription=normalize_field(
                        support_ticketing_category_description_2
                    ),
                    supportTicketingCategorySequence=2,
                    chatbotId=existing_chatbot.chatbotId,
                )
            )

    if support_ticketing_category_title_3 and support_ticketing_category_description_3:
        if len(support_ticketing_categories) > 2:
            support_ticketing_categories[2][0].supportTicketingCategoryTitle = (
                normalize_field(support_ticketing_category_title_3)
            )
            support_ticketing_categories[2][0].supportTicketingCategoryDescription = (
                normalize_field(support_ticketing_category_description_3)
            )
        else:
            new_support_ticketing_categories.append(
                ChatbotSupportTicketingCategories(
                    supportTicketingCategoryTitle=normalize_field(
                        support_ticketing_category_title_3
                    ),
                    supportTicketingCategoryDescription=normalize_field(
                        support_ticketing_category_description_3
                    ),
                    supportTicketingCategorySequence=3,
                    chatbotId=existing_chatbot.chatbotId,
                )
            )

    if new_support_ticketing_categories:
        db.add_all(new_support_ticketing_categories)
    await db.commit()

    return {"success": True, "message": "Chatbot details successfully saved"}


async def deactivate_chatbot_instance(
    db: AsyncSession,
    org_id: UUID,
    chatbot_id: UUID,
) -> dict:
    """
    Business logic for deactivating a chatbot deployment and purging its vector node associations.
    """
    existing_chatbot = await fetch_existing_chatbot(
        chatbot_id=chatbot_id, org_id=org_id, db=db
    )

    if not existing_chatbot:
        return {"message": "No such active chatbot exists.", "success": False}

    # Fetch mapping relations linking knowledge feeds to this chatbot
    chatbot_data_feeds = await fetch_chatbot_datafeeds(chatbot_id=chatbot_id, db=db)

    try:
        remove_datafeeds_str_ids = set()

        # Delete database relation mappings between chatbot and knowledge feeds
        for chatbot_data_feed in chatbot_data_feeds:
            remove_datafeeds_str_ids.add(str(chatbot_data_feed.dataFeedId))
            await db.delete(chatbot_data_feed)

        # Propagate knowledge node metadata changes to the pgvector database
        result = modify_chatbot_id_in_nodes(
            org_id=org_id,
            chatbot_id=str(chatbot_id),
            remove_datafeeds=remove_datafeeds_str_ids,
            additional_datafeeds=set(),
        )

        if result:
            # Set soft-delete active status flag
            existing_chatbot.activeStatus = False
            # Commit the changes to the database
            await db.commit()

            return {"message": "Chatbot deleted successfully.", "success": True}

        return {"message": "Chatbot deletion failed.", "success": False}
    except Exception as e:
        # Rollback changes if any error occurs
        await db.rollback()
        logger.error(f"Unexpected error during chatbot deactivation: {e}")
        return {
            "message": f"Failed to delete chatbot due to error: {str(e)}",
            "success": False,
        }
