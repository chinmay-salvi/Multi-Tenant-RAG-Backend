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
from app.api.crud_helper import (
    fetch_existing_organization,
    fetch_existing_user,
    fetch_existing_chatbot,
    fetch_datafeeds_by_ids,
    fetch_org_all_chatbots_id_name,
    fetch_chatbot_datafeeds,
    fetch_chatbots_datafeeds_support_ticketing_categories,
    fetch_support_ticketing_categories_for_chatbot,
    fetch_chatbots_support_ticketing_categories,
    fetch_agent_type,
    count_existing_chatbot,
    fetch_chatbot_directly,
)
from app.api.datafeed_helper import modify_chatbot_id_in_nodes
from app.api.deps import get_db
from app.api.plans_helper import check_chatbot_limit
from app.api.s3_helper import upload_file_to_s3
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
from app.schema import (
    BuildChatbotRequest,
    DeleteChatbotRequest,
    SaveChatbotDataFeedsRequest,
    ChatbotWidgetLogoBase64Response,
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
                # "companyLogoLink": chatbot.companyLogo,
                # "widgetLogoLink": chatbot.widgetLogo,
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

    This function does the following:
    1. Validates that the requesting user belongs to the target tenant organization.
    2. Runs quota guardrails to verify the organization has not exceeded their plan limits
       on maximum allowed chatbots.
    3. Prevents duplicate chatbot deployments by verifying the agent name is unique per tenant.
    4. Fetches and locks all associated active datafeeds.
    5. Propagates metadata alterations inside pgvector nodes so the chatbot retains immediate search scoping.

    Args:
        body (BuildChatbotRequest): Chatbot payload parameters (name, intro, and list of datafeed IDs).
        token_payload (dict): Decoded and verified tenant token.
        db (AsyncSession): Active SQLAlchemy database session.

    Returns:
        dict: A JSON response containing 'message', 'success' flag, and the generated 'chatbotId'.
    """
    # Assert identity constraints
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    # Fetch current active chatbot deployments under the tenant
    chatbot_count = await count_existing_chatbot(org_id=token_payload["orgId"], db=db)

    # Enforce plan quota guardrails
    message = check_chatbot_limit(
        org_id=token_payload["orgId"], utilised_chatbot_count=chatbot_count
    )

    if message:
        return {
            "message": message,
            "success": False,
            "chatbotId": None,
        }

    # Prevent duplicate naming conflicts
    existing_chatbot = await fetch_existing_chatbot(
        chatbot_name=body.chatbotName, org_id=token_payload["orgId"], db=db
    )

    if existing_chatbot:
        return {
            "message": "Agent with provided name already exists.",
            "success": False,
            "chatbotId": None,
        }

    # Retrieve all valid, fully processed active knowledge datafeeds matching body IDs
    data_feeds = await fetch_datafeeds_by_ids(
        org_id=token_payload["orgId"],
        data_feed_ids=body.dataFeedIds,
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

    if len(data_feeds) != len(body.dataFeedIds):
        return {
            "message": "All data feeds not found.",
            "success": False,
            "chatbotId": None,
        }

    try:
        chatbot_id = uuid4()
        # Propagate changes to vector db nodes to tie them instantly to this new chatbot ID
        result = modify_chatbot_id_in_nodes(
            org_id=token_payload["orgId"],
            chatbot_id=str(chatbot_id),
            remove_datafeeds=set(),
            additional_datafeeds=set(map(str, body.dataFeedIds)),
        )

        if result:
            db.add(
                Chatbot(
                    chatbotId=chatbot_id,
                    agentTypeId=agent_type.agentTypeId,
                    chatbotName=body.chatbotName,
                    orgId=token_payload["orgId"],
                    chatbotIntroMessages=[
                        intro_message
                        for intro_message in body.chatbotIntroMessages
                        if isinstance(intro_message, str)
                    ],
                )
            )
            db.add_all(
                [
                    ChatbotDataFeeds(chatbotId=chatbot_id, dataFeedId=data_feed_id)
                    for data_feed_id in body.dataFeedIds
                ]
            )
            await db.commit()
            return {
                "message": f"Agent '{body.chatbotName}' created successfully.",
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
        # Handle other exceptions
        print("An unexpected error occurred:", e)
        return {
            "message": f"An unexpected error occurred: {e}",
            "success": False,
            "chatbotId": None,
        }


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
    # chatbotTicketingVisibility: Optional[bool] = Form(None),
    # isPublic: Optional[bool] = Form(None),
    chatbotTicketingVisibility: Optional[str] = Form(None),
    isPublic: Optional[str] = Form(None),  # Change to str to handle boolean from form
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

    Workflow:
    1. Authenticates the requesting user to prevent unauthorized access across tenants.
    2. Validates that the target chatbot exists and belongs to the requesting user's organization.
    3. Norms string inputs, parsing custom values like empty string placeholders ('""') to database Null values.
    4. Safe-converts stringified booleans ('true'/'false') into pythonic booleans.
    5. Saves uploaded logo files (company and widget) to a secure local temporary workspace,
       converts them into Base64 data URLs for immediate widget render performance, and
       simultaneously uploads them to long-term AWS S3 storage.
    6. Synchronizes support ticketing categories, creating new sequences or editing titles/descriptions.

    Args:
        userId (UUID): Requesting user ID.
        chatbotId (UUID): Chatbot target ID to update.
        chatbotName (str, optional): Custom updated display name.
        chatbotBannerMessage (str, optional): Custom banner message.
        chatbotIntroMessage (str, optional): Stringified JSON array of welcome messages.
        chatWithUsTitle (str, optional): Title for chat workspace.
        chatWithUsDescription (str, optional): Short description for chat workspace.
        raiseTicketTitle (str, optional): Title for support ticket wizard.
        raiseTicketDescription (str, optional): Description for support ticket wizard.
        chatbotExampleUserQuestions (str, optional): Stringified JSON array of quick starter questions.
        chatbotUserMessageColor (str, optional): Hex code of chat user message bubble.
        chatbotFor (str, optional): Detailed role instruction.
        companyDo (str, optional): Detailed company objectives.
        hallucinationFixer (str, optional): Explicit custom negative guidelines.
        businessContactDetails (str, optional): Out-of-hours phone or email info.
        chatbotTicketingVisibility (str, optional): Visibility status for support wizard ('true' or 'false').
        isPublic (str, optional): Visibility status for widget availability ('true' or 'false').
        supportTicketingCategoryTitle1 (str, optional): Title of primary support category.
        supportTicketingCategoryDescription1 (str, optional): Description of primary support category.
        supportTicketingCategoryTitle2 (str, optional): Title of secondary support category.
        supportTicketingCategoryDescription2 (str, optional): Description of secondary support category.
        supportTicketingCategoryTitle3 (str, optional): Title of tertiary support category.
        supportTicketingCategoryDescription3 (str, optional): Description of tertiary support category.
        companyLogo (UploadFile, optional): Company branding image file payload.
        widgetLogo (UploadFile, optional): Small widget-bubble launcher image file.
        token_payload (dict): Decoded and verified tenant JWT details.
        db (AsyncSession): Active SQLAlchemy database session.

    Returns:
        dict: A status success response containing 'success' and 'message'.

    Raises:
        HTTPException:
            - 401 Unauthorized: If user ID does not match JWT user ID.
            - 400 Bad Request: If JSON parse for list arrays fails.
    """
    # Enforce strict user token validation
    if userId != UUID(token_payload["userId"]):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Fetch existing chatbot deployment under tenant organization
    existing_chatbot = await fetch_existing_chatbot(
        chatbot_id=chatbotId, org_id=token_payload["orgId"], db=db
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

    chatbot_ticketing_visibility_bool = str_to_bool(chatbotTicketingVisibility)
    is_public_bool = str_to_bool(isPublic)

    # Update fields only if they are provided
    update_data = {
        "chatbotName": chatbotName,
        "chatbotBannerMessage": chatbotBannerMessage,
        "chatWithUsTitle": chatWithUsTitle,
        "chatWithUsDescription": chatWithUsDescription,
        "raiseTicketTitle": raiseTicketTitle,
        "raiseTicketDescription": raiseTicketDescription,
        "chatbotUserMessageColor": chatbotUserMessageColor,
        "chatbotFor": chatbotFor,
        "companyDo": companyDo,
        "hallucinationFixer": hallucinationFixer,
        "businessContactDetails": businessContactDetails,
        "chatbotTicketingVisibility": chatbot_ticketing_visibility_bool,
        "isPublic": is_public_bool,
    }

    for key, value in update_data.items():
        if value is not None:
            if value == '""':
                setattr(existing_chatbot, key, None)  # Explicitly set to None
            else:
                setattr(existing_chatbot, key, value)

    if chatbotIntroMessage:
        try:
            parsed_intro_messages = json.loads(chatbotIntroMessage)
            if not isinstance(parsed_intro_messages, list) or not all(
                isinstance(item, str) for item in parsed_intro_messages
            ):
                raise ValueError("chatbotIntroMessage must be a list of strings")
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=400, detail="Invalid JSON format in chatbotIntroMessage"
            )
        existing_chatbot.chatbotIntroMessages = parsed_intro_messages

    if chatbotExampleUserQuestions:
        try:
            parsed_example_questions = json.loads(chatbotExampleUserQuestions)
            if not isinstance(parsed_example_questions, list) or not all(
                isinstance(item, str) for item in parsed_example_questions
            ):
                raise ValueError(
                    "chatbotExampleUserQuestions must be a list of strings"
                )
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=400,
                detail="Invalid JSON format in chatbotExampleUserQuestions",
            )
        existing_chatbot.chatbotExampleUserQuestions = parsed_example_questions

    # Handle file uploads
    async with aiofiles.tempfile.TemporaryDirectory() as temp_dir:
        if companyLogo:
            company_logo_filepath = f"{temp_dir}/{chatbotId}_{companyLogo.filename}"
            async with aiofiles.open(
                company_logo_filepath, "wb"
            ) as temp_company_logo_file:
                company_logo_content = await companyLogo.read()
                await temp_company_logo_file.write(company_logo_content)

            company_logo_mime_type, _ = mimetypes.guess_type(company_logo_filepath)
            if company_logo_mime_type is None:
                company_logo_mime_type = "application/octet-stream"

            existing_chatbot.companyLogoBase64 = f"data:{company_logo_mime_type};base64,{base64.b64encode(company_logo_content).decode('utf-8')}"

            existing_chatbot.companyLogo = await upload_file_to_s3(
                uploaded_file_path=company_logo_filepath,
                s3_key=f"{token_payload['orgId']}/{chatbotId}/chatbot_data/company_logo.{companyLogo.filename.split('.')[-1].lower()}",
            )
        if widgetLogo:
            widget_logo_filepath = f"{temp_dir}/{chatbotId}_{widgetLogo.filename}"
            async with aiofiles.open(
                widget_logo_filepath, "wb"
            ) as temp_widget_logo_file:
                widget_logo_content = await widgetLogo.read()
                await temp_widget_logo_file.write(widget_logo_content)

            widget_logo_mime_type, _ = mimetypes.guess_type(widget_logo_filepath)
            if widget_logo_mime_type is None:
                widget_logo_mime_type = "application/octet-stream"

            existing_chatbot.widgetLogoBase64 = f"data:{widget_logo_mime_type};base64,{base64.b64encode(widget_logo_content).decode('utf-8')}"

            existing_chatbot.widgetLogo = await upload_file_to_s3(
                uploaded_file_path=widget_logo_filepath,
                s3_key=f"{token_payload['orgId']}/{chatbotId}/chatbot_data/widget_logo.{widgetLogo.filename.split('.')[-1].lower()}",
            )

    db.add(existing_chatbot)
    await db.commit()
    await db.refresh(existing_chatbot)

    # Update support ticketing categories only if they are provided
    support_ticketing_categories = await fetch_support_ticketing_categories_for_chatbot(
        chatbot_id=chatbotId, db=db
    )
    new_support_ticketing_categories = []

    if supportTicketingCategoryTitle1 and supportTicketingCategoryDescription1:
        if len(support_ticketing_categories) > 0:
            support_ticketing_categories[0][0].supportTicketingCategoryTitle = (
                normalize_field(supportTicketingCategoryTitle1)
            )
            support_ticketing_categories[0][0].supportTicketingCategoryDescription = (
                normalize_field(supportTicketingCategoryDescription1)
            )
        else:
            new_support_ticketing_categories.append(
                ChatbotSupportTicketingCategories(
                    supportTicketingCategoryTitle=normalize_field(
                        supportTicketingCategoryTitle1
                    ),
                    supportTicketingCategoryDescription=normalize_field(
                        supportTicketingCategoryDescription1
                    ),
                    supportTicketingCategorySequence=1,
                    chatbotId=existing_chatbot.chatbotId,
                )
            )

    if supportTicketingCategoryTitle2 and supportTicketingCategoryDescription2:
        if len(support_ticketing_categories) > 1:
            support_ticketing_categories[1][0].supportTicketingCategoryTitle = (
                normalize_field(supportTicketingCategoryTitle2)
            )
            support_ticketing_categories[1][0].supportTicketingCategoryDescription = (
                normalize_field(supportTicketingCategoryDescription2)
            )
        else:
            new_support_ticketing_categories.append(
                ChatbotSupportTicketingCategories(
                    supportTicketingCategoryTitle=normalize_field(
                        supportTicketingCategoryTitle2
                    ),
                    supportTicketingCategoryDescription=normalize_field(
                        supportTicketingCategoryDescription2
                    ),
                    supportTicketingCategorySequence=2,
                    chatbotId=existing_chatbot.chatbotId,
                )
            )

    if supportTicketingCategoryTitle3 and supportTicketingCategoryDescription3:
        if len(support_ticketing_categories) > 2:
            support_ticketing_categories[2][0].supportTicketingCategoryTitle = (
                normalize_field(supportTicketingCategoryTitle3)
            )
            support_ticketing_categories[2][0].supportTicketingCategoryDescription = (
                normalize_field(supportTicketingCategoryDescription3)
            )
        else:
            new_support_ticketing_categories.append(
                ChatbotSupportTicketingCategories(
                    supportTicketingCategoryTitle=normalize_field(
                        supportTicketingCategoryTitle3
                    ),
                    supportTicketingCategoryDescription=normalize_field(
                        supportTicketingCategoryDescription3
                    ),
                    supportTicketingCategorySequence=3,
                    chatbotId=existing_chatbot.chatbotId,
                )
            )

    if new_support_ticketing_categories:
        db.add_all(new_support_ticketing_categories)
    await db.commit()

    return {"success": True, "message": "Chatbot details successfully saved"}


@chatbot_router.post("/delete_chatbot")
async def delete_chatbot(
    body: DeleteChatbotRequest,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Deactivates a chatbot deployment and purges its knowledge vector node associations.

    This function executes the following workflow:
    1. Asserts that the requesting user belongs to the target tenant organization.
    2. Verifies that the chatbot exists and is active under the organization.
    3. Fetches all active datafeed mappings associated with the chatbot.
    4. Deletes these chatbot-to-datafeed relations from the database.
    5. Syncs the de-allocation with the vector database by calling `modify_chatbot_id_in_nodes`,
       removing the chatbot ID reference from all related pgvector documents.
    6. Sets the chatbot's `activeStatus` to `False` (soft-delete).

    Args:
        body (DeleteChatbotRequest): Struct containing target chatbotId parameter.
        token_payload (dict): Decoded and verified tenant JWT.
        db (AsyncSession): Active SQLAlchemy database session.

    Returns:
        dict: Success state confirmation containing 'message' and 'success' status.

    Raises:
        HTTPException:
            - 500 Internal Server Error: If deactivation fails or a database exception is caught.
    """
    # Verify the user is mapped inside the tenant organization
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    # Confirm that target chatbot is currently active under this organization
    existing_chatbot = await fetch_existing_chatbot(
        chatbot_id=body.chatbotId, org_id=token_payload["orgId"], db=db
    )

    if not existing_chatbot:
        return {"message": "No such active chatbot exists.", "success": False}

    # Fetch mapping relations linking knowledge feeds to this chatbot
    chatbot_data_feeds = await fetch_chatbot_datafeeds(chatbot_id=body.chatbotId, db=db)

    try:
        remove_datafeeds_str_ids = set()

        # Delete database relation mappings between chatbot and knowledge feeds
        for chatbot_data_feed in chatbot_data_feeds:
            remove_datafeeds_str_ids.add(str(chatbot_data_feed.dataFeedId))
            await db.delete(chatbot_data_feed)

        # Propagate knowledge node metadata changes to the pgvector database
        result = modify_chatbot_id_in_nodes(
            org_id=token_payload["orgId"],
            chatbot_id=str(body.chatbotId),
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
        raise HTTPException(
            status_code=500, detail=f"Failed to delete chatbot due to error: {str(e)}"
        )


@chatbot_router.get("/fetch_chatbots_list")
async def fetch_chatbots_list(
    userId: UUID,
    token_payload: dict = Depends(validate_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieves a simplified list of all active chatbot IDs and names under the tenant organization.

    Args:
        userId (UUID): Requesting user ID.
        token_payload (dict): Decoded and verified tenant JWT.
        db (AsyncSession): Active SQLAlchemy database session.

    Returns:
        dict: Object containing the user ID and a list of (chatbotId, chatbotName) tuples.
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

    This function executes the following workflow:
    1. Asserts requesting user membership in the tenant organization.
    2. Confirms that the target chatbot exists under the organization.
    3. Verifies that all requested datafeed IDs are active and fully processed under the tenant.
    4. Compares the existing mappings against the new payload to determine:
       - Which datafeeds were added (need to be indexed in the vector store).
       - Which datafeeds were removed (need to be detached in the vector store).
    5. Propagates metadata updates to the pgvector knowledge nodes via `modify_chatbot_id_in_nodes`.
    6. Deletes old relational links and inserts new ones in the relational database.

    Args:
        body (SaveChatbotDataFeedsRequest): Struct with target chatbotId and updated list of dataFeedIds.
        token_payload (dict): Decoded and verified tenant JWT.
        db (AsyncSession): Active SQLAlchemy database session.

    Returns:
        dict: Confirmation JSON status message.

    Raises:
        HTTPException:
            - 500 Internal Server Error: If the node modification or database update fails.
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

    This function executes the following workflow:
    1. Fetches chatbot ticketing categories and configuration details.
    2. Validates user session credentials if the chatbot's `isPublic` flag is set to `False`
       (preventing direct API snooping).
    3. Formats and includes the active lead form configurations (keys, input types, and limits).
    4. Formats support ticketing categories, sorting them sequentially based on their designated
       sequence ID (1st, 2nd, or 3rd category).
    5. Checks for an active `conversationId`; if not provided, lazy-creates a new persistent
       conversation record in the database and returns the new ID to the widget.

    Args:
        chatbotId (UUID): Associated chatbot identifier.
        request (Request): Active HTTP request object.
        authorization (str, optional): Bearer JWT authentication header for private widgets.
        userId (UUID, optional): Accessing user ID for private widgets.
        conversationId (UUID, optional): Requesting client's ongoing conversation ID.
        db (AsyncSession): Active SQLAlchemy database session.

    Returns:
        dict: Complete widget initialization configuration dictionary (colors, banners, questions, logos).

    Raises:
        HTTPException:
            - 401 Unauthorized: If chatbot is private and auth parameters are missing or invalid.
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

        # if chatbot.companyLogo:
        #     company_logo = await create_presigned_url(
        #         "/".join(chatbot.companyLogo.split("/")[3:])
        #     )
        # else:
        #     company_logo = None
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
            # "companyLogoLink": company_logo,
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

    This function does the following:
    1. Queries the database directly for the target Chatbot profile using `fetch_chatbot_directly`.
    2. Handles missing chatbot records by returning a non-success model payload.
    3. Handles cases where the widget logo base64 field is empty by returning a descriptive error model payload.
    4. Returns the base64 widget logo data URL to allow fast client-side widget rendering without S3 fetches.

    Args:
        chatbotId (UUID): Associated chatbot identifier.
        db (AsyncSession): Active SQLAlchemy database session.

    Returns:
        ChatbotWidgetLogoBase64Response: Pydantic response containing search success and the base64 string.

    Raises:
        HTTPException:
            - 500 Internal Server Error: If an unexpected database or system exception occurs.
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
