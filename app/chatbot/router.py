import base64
import json
import logging
import mimetypes
from typing import Optional
from uuid import UUID, uuid4

import aiofiles
from fastapi import (
    APIRouter,
    Depends,
    UploadFile,
    File,
    HTTPException,
    Form,
    Header,
    Request,
    status,
)
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_db
from app.payments.plans_helper import check_chatbot_limit
from app.utils.s3_helper import upload_file_to_s3
from app.auth import validate_user
from app.db.tables import (
    User,
    Tag,
    Chatbot,
    ChatbotDataFeeds,
    Organization,
    OrganizationUser,
    ChatbotSupportTicketingCategories,
    Conversation,
)
from app.user_org.crud import fetch_existing_user, fetch_agent_type
from app.datafeed.crud import fetch_datafeeds_by_ids
from app.datafeed.datafeed_helper import modify_chatbot_id_in_nodes

from .schemas import (
    BuildChatbotRequest,
    DeleteChatbotRequest,
    SaveChatbotDataFeedsRequest,
    ChatbotWidgetLogoBase64Response,
)
from .crud import (
    count_existing_chatbot,
    fetch_existing_chatbot,
    fetch_chatbot_directly,
    fetch_org_all_chatbots_id_name,
    fetch_support_ticketing_categories_for_chatbot,
    fetch_chatbot_datafeeds,
    fetch_chatbots_datafeeds_support_ticketing_categories,
    fetch_chatbots_support_ticketing_categories,
)
from .services import (
    create_chatbot_instance,
    update_chatbot_configuration,
    deactivate_chatbot_instance,
)

logger = logging.getLogger(__name__)

chatbot_router = APIRouter()


@chatbot_router.get("/fetch_chatbots_details")
async def fetch_chatbots_details(
    userId: UUID,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieves complete configuration details for all active chatbots owned by the tenant organization.

    This function executes the following workflow:
    1. Validates organization and user existence; auto-registers the organization and user in
       the backend database if they are not already cached (lazy-initialization of tenant assets).
    2. Provisions default organization tags (e.g., "None") to seed the tenant classification space.
    3. Fetches all chatbot records along with their associated knowledge datafeeds and support ticketing categories.
    4. Serializes the complex relational structure (nested list of datafeeds, sorted categories, lead capture templates)
       into a unified response schema.

    Args:
        userId (UUID): The user ID requesting the chatbot details.
        token_payload (dict): Decoded and verified tenant JWT details.
        db (AsyncSession): Active SQLAlchemy database session.

    Returns:
        list[dict]: A list of chatbot configuration profiles containing UI styling, datafeeds, and lead forms.
    """

    # Check if the requesting user exists within the organization mapping
    existing_user = await fetch_existing_user(
        org_id=token_payload["orgId"], user_id=token_payload["userId"], db=db
    )

    try:
        # Lazy provision user/org credentials if they exist in Supabase but not yet in the app database
        if not existing_user:
            # Check if organization profile has been recorded
            from app.user_org.crud import fetch_existing_organization
            existing_org = await fetch_existing_organization(
                org_id=token_payload["orgId"], db=db
            )

            if not existing_org:
                db.add(
                    Organization(
                        orgId=token_payload["orgId"],
                    )
                )

            # Lazy insert User record
            db.add(
                User(
                    userId=token_payload["userId"],
                    userCreateAt=token_payload["userCreateAt"].replace(tzinfo=None),
                )
            )

            # Link the user to the organization with their specific access role
            db.add(
                OrganizationUser(
                    orgId=token_payload["orgId"],
                    userId=token_payload["userId"],
                    userRole=token_payload["userRole"],
                )
            )

            # Provision the default 'None' tag for classification tasks
            db.add(Tag(tagName="None", orgId=token_payload["orgId"]))

        await db.commit()
    except Exception as e:
        print("Error occurred while adding user or org:", e)
        raise HTTPException(status_code=500, detail="Unexpected Error")

    # Query all active chatbot deployments, associated datafeeds, and support categories for this tenant
    chatbots_data = await fetch_chatbots_datafeeds_support_ticketing_categories(
        org_id=token_payload["orgId"], db=db
    )
    response = []

    for chatbot in chatbots_data:
        leadFormTitle = leadFormKeys = leadFormLabels = None
        leadFormInputTypes = leadFormFrequencyHours = leadFormMaxShowLimit = None

        if chatbot.latestLeadForm:
            leadFormTitle = chatbot.latestLeadForm.title
            leadFormKeys = chatbot.latestLeadForm.keys
            leadFormLabels = chatbot.latestLeadForm.labels
            leadFormInputTypes = chatbot.latestLeadForm.inputTypes
            leadFormFrequencyHours = chatbot.latestLeadForm.frequencyHours
            leadFormMaxShowLimit = chatbot.latestLeadForm.maxShowLimit

        response.append(
            {
                "chatbotId": chatbot.chatbotId,
                "chatbotName": chatbot.chatbotName,
                "chatbotCreationDate": chatbot.chatbotCreationDate.date(),
                "chatbotUpdationDate": chatbot.chatbotUpdationDate.date(),
                "chatbotBannerMessage": chatbot.chatbotBannerMessage,
                "chatbotIntroMessage": chatbot.chatbotIntroMessages,
                "chatWithUsTitle": chatbot.chatWithUsTitle,
                "chatWithUsDescription": chatbot.chatWithUsDescription,
                "raiseTicketTitle": chatbot.raiseTicketTitle,
                "raiseTicketDescription": chatbot.raiseTicketDescription,
                "chatbotExampleUserQuestions": chatbot.chatbotExampleUserQuestions,
                "companyLogoBase64": chatbot.companyLogoBase64,
                "widgetLogoBase64": chatbot.widgetLogoBase64,
                "chatbotUserMessageColor": chatbot.chatbotUserMessageColor,
                "chatbotLink": chatbot.chatbotLink,
                "companyDo": chatbot.companyDo,
                "chatbotFor": chatbot.chatbotFor,
                "hallucinationFixer": chatbot.hallucinationFixer,
                "businessContactDetails": chatbot.businessContactDetails,
                "isPublic": chatbot.isPublic,
                "isLeadFormEnabled": chatbot.isLeadFormEnabled,
                "leadFormTitle": leadFormTitle,
                "leadFormKeys": leadFormKeys,
                "leadFormLabels": leadFormLabels,
                "leadFormInputTypes": leadFormInputTypes,
                "leadFormFrequencyHours": leadFormFrequencyHours,
                "leadFormMaxShowLimit": leadFormMaxShowLimit,
                "chatbotTicketingVisibility": (
                    "on" if chatbot.chatbotTicketingVisibility else "off"
                ),
                "supportTicketingCategoryTitle1": (
                    chatbot.chatbotSupportTicketingCategories[
                        0
                    ].supportTicketingCategoryTitle
                    if len(chatbot.chatbotSupportTicketingCategories)
                    else None
                ),
                "supportTicketingCategoryTitle2": (
                    chatbot.chatbotSupportTicketingCategories[
                        1
                    ].supportTicketingCategoryTitle
                    if len(chatbot.chatbotSupportTicketingCategories) > 1
                    else None
                ),
                "supportTicketingCategoryTitle3": (
                    chatbot.chatbotSupportTicketingCategories[
                        2
                    ].supportTicketingCategoryTitle
                    if len(chatbot.chatbotSupportTicketingCategories) > 2
                    else None
                ),
                "supportTicketingCategoryDescription1": (
                    chatbot.chatbotSupportTicketingCategories[
                        0
                    ].supportTicketingCategoryDescription
                    if len(chatbot.chatbotSupportTicketingCategories)
                    else None
                ),
                "supportTicketingCategoryDescription2": (
                    chatbot.chatbotSupportTicketingCategories[
                        1
                    ].supportTicketingCategoryDescription
                    if len(chatbot.chatbotSupportTicketingCategories) > 1
                    else None
                ),
                "supportTicketingCategoryDescription3": (
                    chatbot.chatbotSupportTicketingCategories[
                        2
                    ].supportTicketingCategoryDescription
                    if len(chatbot.chatbotSupportTicketingCategories) > 2
                    else None
                ),
                "dataFeeds": [
                    {
                        "dataFeedId": chatbot_datafeed.datafeed.dataFeedId,
                        "dataFeedName": chatbot_datafeed.datafeed.dataFeedName,
                        "dataFeedTags": [
                            datafeed_tag.tag.tagName
                            for datafeed_tag in chatbot_datafeed.datafeed.dataFeedTags
                        ],
                        "dataType": chatbot_datafeed.datafeed.dataType,
                        "fileType": chatbot_datafeed.datafeed.fileType,
                        "createdDatetime": chatbot_datafeed.datafeed.createdDatetime.isoformat(),
                        "updatedDatetime": chatbot_datafeed.datafeed.updatedDatetime.isoformat(),
                        "tokenCount": chatbot_datafeed.datafeed.tokenCount,
                        "activeStatus": chatbot_datafeed.datafeed.activeStatus,
                    }
                    for chatbot_datafeed in chatbot.chatbotDataFeeds
                ],
            }
        )

    return response


@chatbot_router.post("/build_chatbot")
async def build_chatbot(
    body: BuildChatbotRequest,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Creates and initializes a versioned new AI chatbot agent under a tenant organization.
    """
    # Assert identity constraints
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    return await create_chatbot_instance(
        db=db,
        org_id=token_payload["orgId"],
        chatbot_name=body.chatbotName,
        chatbot_intro_messages=body.chatbotIntroMessages,
        data_feed_ids=body.dataFeedIds,
    )


def normalize_field(value):
    return None if value == '""' else value


@chatbot_router.post("/save_chatbot")
async def save_chatbot(
    userId: UUID = Form(...),
    chatbotId: UUID = Form(...),
    chatbotName: Optional[str] = Form(None),
    chatbotBannerMessage: Optional[str] = Form(None),
    chatbotIntroMessage: Optional[str] = Form(None),
    chatWithUsTitle: Optional[str] = Form(None),
    chatWithUsDescription: Optional[str] = Form(None),
    raiseTicketTitle: Optional[str] = Form(None),
    raiseTicketDescription: Optional[str] = Form(None),
    chatbotExampleUserQuestions: Optional[str] = Form(None),
    chatbotUserMessageColor: Optional[str] = Form(None),
    chatbotFor: Optional[str] = Form(None),
    companyDo: Optional[str] = Form(None),
    hallucinationFixer: Optional[str] = Form(None),
    businessContactDetails: Optional[str] = Form(None),
    chatbotTicketingVisibility: Optional[str] = Form(None),
    isPublic: Optional[str] = Form(None),
    supportTicketingCategoryTitle1: Optional[str] = Form(None),
    supportTicketingCategoryDescription1: Optional[str] = Form(None),
    supportTicketingCategoryTitle2: Optional[str] = Form(None),
    supportTicketingCategoryDescription2: Optional[str] = Form(None),
    supportTicketingCategoryTitle3: Optional[str] = Form(None),
    supportTicketingCategoryDescription3: Optional[str] = Form(None),
    companyLogo: Optional[UploadFile] = File(None),
    widgetLogo: Optional[UploadFile] = File(None),
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Saves and updates configuration details for an existing AI chatbot widget.

    This function processes multi-part form parameters containing styling options, logo images,
    behavior instructions (company goals, hallucination fixers), visibility switches, and
    support ticket categories.
    """
    # Enforce strict user token validation
    if userId != UUID(token_payload["userId"]):
        raise HTTPException(status_code=401, detail="Unauthorized")

    return await update_chatbot_configuration(
        db=db,
        org_id=token_payload["orgId"],
        chatbot_id=chatbotId,
        chatbot_name=chatbotName,
        chatbot_banner_message=chatbotBannerMessage,
        chatbot_intro_message=chatbotIntroMessage,
        chat_with_us_title=chatWithUsTitle,
        chat_with_us_description=chatWithUsDescription,
        raise_ticket_title=raiseTicketTitle,
        raise_ticket_description=raiseTicketDescription,
        chatbot_example_user_questions=chatbotExampleUserQuestions,
        chatbot_user_message_color=chatbotUserMessageColor,
        chatbot_for=chatbotFor,
        company_do=companyDo,
        hallucination_fixer=hallucinationFixer,
        business_contact_details=businessContactDetails,
        chatbot_ticketing_visibility=chatbotTicketingVisibility,
        is_public=isPublic,
        support_ticketing_category_title_1=supportTicketingCategoryTitle1,
        support_ticketing_category_description_1=supportTicketingCategoryDescription1,
        support_ticketing_category_title_2=supportTicketingCategoryTitle2,
        support_ticketing_category_description_2=supportTicketingCategoryDescription2,
        support_ticketing_category_title_3=supportTicketingCategoryTitle3,
        support_ticketing_category_description_3=supportTicketingCategoryDescription3,
        company_logo=companyLogo,
        widget_logo=widgetLogo,
    )


@chatbot_router.post("/delete_chatbot")
async def delete_chatbot(
    body: DeleteChatbotRequest,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Deactivates a chatbot deployment and purges its knowledge vector node associations.
    """
    # Verify the user is mapped inside the tenant organization
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    return await deactivate_chatbot_instance(
        db=db,
        org_id=token_payload["orgId"],
        chatbot_id=body.chatbotId,
    )


@chatbot_router.get("/fetch_chatbots_list")
async def fetch_chatbots_list(
    userId: UUID,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieves a simplified list of all active chatbot IDs and names under the tenant organization.
    """
    # Verify requesting user profile is active in the tenant database
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    # Fetch simple list of ID-name mappings for active chatbots
    all_active_chatbots_id_name = await fetch_org_all_chatbots_id_name(
        org_id=token_payload["orgId"], db=db
    )

    return {
        "userId": token_payload["userId"],
        "chatbots": [tuple(val) for val in all_active_chatbots_id_name],
    }


@chatbot_router.post("/save_chatbot_data_feeds")
async def save_chatbot_data_feeds(
    body: SaveChatbotDataFeedsRequest,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Updates the list of datafeeds mapped to a chatbot and synchronizes the vector store.
    """
    # Validate requesting user's identity under the tenant
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )
    
    # Confirm target chatbot is currently active under this organization
    existing_chatbot = await fetch_existing_chatbot(
        chatbot_id=body.chatbotId, org_id=token_payload["orgId"], db=db
    )

    if not existing_chatbot:
        return "No such active chatbot exists"
        
    # Retrieve all valid fully processed datafeeds matching target IDs
    data_feeds = await fetch_datafeeds_by_ids(
        org_id=token_payload["orgId"],
        data_feed_ids=body.dataFeedIds,
        processed_only=True,
        db=db,
    )

    if len(data_feeds) != len(body.dataFeedIds):
        return "All data feeds not found"

    try:
        # Fetch current database datafeed associations for this chatbot
        chatbot_data_feeds = await fetch_chatbot_datafeeds(
            chatbot_id=body.chatbotId, db=db
        )

        additional_data_feeds = set(body.dataFeedIds)
        remove_datafeeds = set()

        # Classify which links to retain, delete, or add
        for chatbot_data_feed in chatbot_data_feeds:
            if chatbot_data_feed.dataFeedId in additional_data_feeds:
                additional_data_feeds.remove(chatbot_data_feed.dataFeedId)
            else:
                remove_datafeeds.add(chatbot_data_feed.dataFeedId)
                await db.delete(chatbot_data_feed)

        # Propagate changes to pgvector nodes so the chatbot instantly queries correct documents
        result = modify_chatbot_id_in_nodes(
            org_id=token_payload["orgId"],
            chatbot_id=str(body.chatbotId),
            remove_datafeeds=set(map(str, remove_datafeeds)),
            additional_datafeeds=set(map(str, additional_data_feeds)),
        )

        if result:
            # Append new mapping records in relational database
            db.add_all(
                [
                    ChatbotDataFeeds(
                        chatbotId=body.chatbotId,
                        dataFeedId=data_feed_id,
                    )
                    for data_feed_id in additional_data_feeds
                ]
            )
            await db.commit()
            return {"message": "Chatbot data feeds updated successfully."}
        else:
            return {"message": "Chatbot data feeds update failed."}
    except Exception as e:
        # Rollback changes if any error occurs
        await db.rollback()
        raise HTTPException(
            status_code=500, detail=f"Failed to updated chatbot's data feed: {str(e)}"
        )


@chatbot_router.get("/get_bot_details")
async def get_bot_details(
    chatbotId: UUID,
    request: Request,
    authorization: str = Header(None),
    userId: UUID = None,
    conversationId: UUID = None,
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieves the complete configuration payload for a chatbot to populate the frontend widget.
    """
    # Fetch chatbot record alongside its ticketing categories and configurations
    chatbot = await fetch_chatbots_support_ticketing_categories(
        chatbot_id=chatbotId, db=db
    )

    if chatbot:
        # Enforce authentication guardrails for private chatbots
        if not chatbot.isPublic:
            if authorization and userId:
                _ = await validate_user(request, authorization)
            else:
                raise HTTPException(status_code=401, detail="Unauthorized")

        leadFormTitle = leadFormKeys = leadFormLabels = None
        leadFormInputTypes = leadFormFrequencyHours = leadFormMaxShowLimit = None

        # Load lead form properties if a template is active for this chatbot
        if chatbot.latestLeadForm:
            leadFormTitle = chatbot.latestLeadForm.title
            leadFormKeys = chatbot.latestLeadForm.keys
            leadFormLabels = chatbot.latestLeadForm.labels
            leadFormInputTypes = chatbot.latestLeadForm.inputTypes
            leadFormFrequencyHours = chatbot.latestLeadForm.frequencyHours
            leadFormMaxShowLimit = chatbot.latestLeadForm.maxShowLimit

        response = {
            "conversationId": conversationId,
            "chatbotId": chatbot.chatbotId,
            "chatbotName": chatbot.chatbotName,
            "chatbotBannerMessage": chatbot.chatbotBannerMessage,
            "chatbotIntroMessage": chatbot.chatbotIntroMessages,
            "chatWithUsTitle": chatbot.chatWithUsTitle,
            "chatWithUsDescription": chatbot.chatWithUsDescription,
            "raiseTicketTitle": chatbot.raiseTicketTitle,
            "raiseTicketDescription": chatbot.raiseTicketDescription,
            "chatbotExampleUserQuestions": chatbot.chatbotExampleUserQuestions,
            "companyLogoBase64": chatbot.companyLogoBase64,
            "widgetLogoBase64": chatbot.widgetLogoBase64,
            "chatbotUserMessageColor": chatbot.chatbotUserMessageColor,
            "chatbotLink": chatbot.chatbotLink,
            "chatbotTicketingVisibility": (
                "on" if chatbot.chatbotTicketingVisibility else "off"
            ),
            "isPublic": chatbot.isPublic,
            "isLeadFormEnabled": chatbot.isLeadFormEnabled,
            "leadFormTitle": leadFormTitle,
            "leadFormKeys": leadFormKeys,
            "leadFormLabels": leadFormLabels,
            "leadFormInputTypes": leadFormInputTypes,
            "leadFormFrequencyHours": leadFormFrequencyHours,
            "leadFormMaxShowLimit": leadFormMaxShowLimit,
            "supportCategories": list(
                sorted(
                    (
                        [
                            {
                                "supportCategoryId": category.supportTicketingCategorySequence,
                                "type": category.supportTicketingCategoryTitle,
                                "description": category.supportTicketingCategoryDescription,
                            }
                            for category in chatbot.chatbotSupportTicketingCategories
                        ]
                        if chatbot.chatbotTicketingVisibility
                        else []
                    ),
                    key=lambda x: x["supportCategoryId"],
                )
            ),
        }
        
        # Lazy provision a conversation identifier for new visitor sessions
        if not conversationId:
            conversation = Conversation(chatbotId=chatbotId, orgId=chatbot.orgId)
            db.add(conversation)
            await db.commit()
            await db.refresh(conversation)
            response["conversationId"] = conversation.conversationId

        return response

    return {"message": "No such chatbot found"}


@chatbot_router.get(
    "/chatbot_widget_logo", response_model=ChatbotWidgetLogoBase64Response
)
async def get_chatbot_widget_logo(
    chatbotId: UUID,
    db: AsyncSession = Depends(get_db),
):
    """
    Fetches the base64-encoded widget logo string for a given chatbot ID.
    """
    try:
        # Fetch the chatbot record from the DB directly without pre-loading relationships
        chatbot: Chatbot = await fetch_chatbot_directly(db=db, chatbot_id=chatbotId)

        if not chatbot:
            return ChatbotWidgetLogoBase64Response(
                message="Chatbot not found.",
                success=False,
            )

        # Assert widget logo exists for this chatbot configuration
        if not chatbot.widgetLogoBase64:
            return ChatbotWidgetLogoBase64Response(
                message="Chatbot widget logo not found.",
                success=False,
                chatbotId=chatbotId,
            )

        return ChatbotWidgetLogoBase64Response(
            message="Chatbot widget logo found.",
            success=True,
            chatbotId=chatbotId,
            widgetLogoBase64=chatbot.widgetLogoBase64,
        )

    except HTTPException as http_exc:
        raise http_exc

    except Exception as e:
        logger.error(f"Unexpected error occurred while fetching widget logo: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected error has occurred while fetching widget logo.",
        )
