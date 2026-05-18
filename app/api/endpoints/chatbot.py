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
    Fetches chatbots details for a given user ID.
    :param userId: The user ID for which chatbots details are requested.
    :param db:
    :param token_payload:
    :return: A list of chatbots details for the given user ID.
    """

    existing_user = await fetch_existing_user(
        org_id=token_payload["orgId"], user_id=token_payload["userId"], db=db
    )

    try:
        if not existing_user:
            # Check if an organization with the given name already exists
            existing_org = await fetch_existing_organization(
                org_id=token_payload["orgId"], db=db
            )

            if not existing_org:
                db.add(
                    Organization(
                        orgId=token_payload["orgId"],
                    )
                )

            # Create a new user
            db.add(
                User(
                    userId=token_payload["userId"],
                    userCreateAt=token_payload["userCreateAt"].replace(tzinfo=None),
                )
            )

            db.add(
                OrganizationUser(
                    orgId=token_payload["orgId"],
                    userId=token_payload["userId"],
                    userRole=token_payload["userRole"],
                )
            )

            db.add(Tag(tagName="None", orgId=token_payload["orgId"]))

        await db.commit()
    except Exception as e:
        print("Error occurred while adding user or org:", e)
        raise HTTPException(status_code=500, detail="Unexpected Error")

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
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    chatbot_count = await count_existing_chatbot(org_id=token_payload["orgId"], db=db)

    message = check_chatbot_limit(
        org_id=token_payload["orgId"], utilised_chatbot_count=chatbot_count
    )

    if message:
        return {
            "message": message,
            "success": False,
            "chatbotId": None,
        }

    # Check if the tag already exists for the given orgId and tagName
    existing_chatbot = await fetch_existing_chatbot(
        chatbot_name=body.chatbotName, org_id=token_payload["orgId"], db=db
    )

    if existing_chatbot:
        return {
            "message": "Agent with provided name already exists.",
            "success": False,
            "chatbotId": None,
        }

    # Fetch the data feeds by IDs
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
    if userId != UUID(token_payload["userId"]):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Fetch existing chatbot
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
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

    existing_chatbot = await fetch_existing_chatbot(
        chatbot_id=body.chatbotId, org_id=token_payload["orgId"], db=db
    )

    if not existing_chatbot:
        return {"message": "No such active chatbot exists.", "success": False}

    chatbot_data_feeds = await fetch_chatbot_datafeeds(chatbot_id=body.chatbotId, db=db)

    try:
        remove_datafeeds_str_ids = set()

        for chatbot_data_feed in chatbot_data_feeds:
            remove_datafeeds_str_ids.add(str(chatbot_data_feed.dataFeedId))
            await db.delete(chatbot_data_feed)

        result = modify_chatbot_id_in_nodes(
            org_id=token_payload["orgId"],
            chatbot_id=str(body.chatbotId),
            remove_datafeeds=remove_datafeeds_str_ids,
            additional_datafeeds=set(),
        )

        if result:
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
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )

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
    _ = await fetch_existing_user(
        org_id=token_payload["orgId"],
        user_id=token_payload["userId"],
        db=db,
        ensure=True,
    )
    # Check if the tag already exists for the given orgId and tagName
    existing_chatbot = await fetch_existing_chatbot(
        chatbot_id=body.chatbotId, org_id=token_payload["orgId"], db=db
    )

    if not existing_chatbot:
        return "No such active chatbot exists"
    # Fetch the data feeds by IDs
    data_feeds = await fetch_datafeeds_by_ids(
        org_id=token_payload["orgId"],
        data_feed_ids=body.dataFeedIds,
        processed_only=True,
        db=db,
    )

    if len(data_feeds) != len(body.dataFeedIds):
        return "All data feeds not found"

    try:
        chatbot_data_feeds = await fetch_chatbot_datafeeds(
            chatbot_id=body.chatbotId, db=db
        )

        additional_data_feeds = set(body.dataFeedIds)
        remove_datafeeds = set()

        for chatbot_data_feed in chatbot_data_feeds:
            if chatbot_data_feed.dataFeedId in additional_data_feeds:
                additional_data_feeds.remove(chatbot_data_feed.dataFeedId)
            else:
                remove_datafeeds.add(chatbot_data_feed.dataFeedId)
                await db.delete(chatbot_data_feed)

        result = modify_chatbot_id_in_nodes(
            org_id=token_payload["orgId"],
            chatbot_id=str(body.chatbotId),
            remove_datafeeds=set(map(str, remove_datafeeds)),
            additional_datafeeds=set(map(str, additional_data_feeds)),
        )

        if result:
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
    Fetches chatbots details for a given customer.
    :param chatbotId:
    :param db:
    :return: A list of chatbots details for the given user ID.
    """

    chatbot = await fetch_chatbots_support_ticketing_categories(
        chatbot_id=chatbotId, db=db
    )

    if chatbot:
        if not chatbot.isPublic:
            if authorization and userId:
                _ = await validate_user(request, authorization)
            else:
                raise HTTPException(status_code=401, detail="Unauthorized")

        leadFormTitle = leadFormKeys = leadFormLabels = None
        leadFormInputTypes = leadFormFrequencyHours = leadFormMaxShowLimit = None

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
    try:
        chatbot: Chatbot = await fetch_chatbot_directly(db=db, chatbot_id=chatbotId)

        if not chatbot:
            return ChatbotWidgetLogoBase64Response(
                message="Chatbot not found.",
                success=False,
            )

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
