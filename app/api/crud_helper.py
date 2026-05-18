from datetime import datetime, MINYEAR, MAXYEAR
from typing import Sequence, List
from uuid import UUID
from aiocache import cached, SimpleMemoryCache
from fastapi import HTTPException
from sqlalchemy import (
    Row,
    asc,
    desc,
    select,
    delete,
    func,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload, joinedload
from app.db.tables import (
    Organization,
    Tag,
    DataFeed,
    DataFeedTag,
    Chatbot,
    OrganizationUser,
    ChatbotSupportTicketingCategories,
    ChatbotDataFeeds,
    Tickets,
    Conversation,
    Message,
    DataFeedEmbeddingMessageQueue,
    MessageQueueStatusEnum,
    AgentType,
    TempUrlDataFeed,
    SysAuthCred,
    URLScrapingMessageQueue,
    Lead,
)


async def fetch_existing_organization(
    org_id: UUID, db: AsyncSession, ensure: bool = False
):
    # Create a query to select the organization with the given org_id
    stmt = select(Organization).filter(Organization.orgId == org_id)

    # Execute the query and fetch the result
    result = await db.execute(stmt)
    organization = result.scalar_one_or_none()

    # Raise exception
    if ensure and organization is None:
        raise HTTPException(status_code=404, detail="Organization not found")

    return organization


async def fetch_existing_user(
    org_id: UUID, user_id: UUID, db: AsyncSession, ensure: bool = False
) -> OrganizationUser | None:
    # Define the query to search for the organization by name
    query = select(OrganizationUser).filter(
        OrganizationUser.userId == user_id, OrganizationUser.orgId == org_id
    )

    # Execute the query and fetch the first result
    result = await db.execute(query)
    user = result.scalar_one_or_none()

    # Raise exception
    if ensure and user is None:
        raise HTTPException(status_code=404, detail="User not found")

    # Return True if the organization exists, False otherwise
    return user


async def fetch_existing_tag(
    tag: str, org_id: UUID, db: AsyncSession, ensure: bool = False
):
    stmt_tag = select(Tag).filter(Tag.tagName == tag, Tag.orgId == org_id)
    result_tag = await db.execute(stmt_tag)
    existing_tag = result_tag.scalar_one_or_none()

    if ensure and existing_tag is None:
        raise HTTPException(status_code=404, detail="Tag not found")

    return existing_tag


async def fetch_all_tags_for_org(
    org_id: UUID, db: AsyncSession
) -> Sequence[Row[tuple[UUID, str]]]:
    stmt_tag = select(Tag.tagId, Tag.tagName).filter(Tag.orgId == org_id)
    result_tag = await db.execute(stmt_tag)
    return result_tag.fetchall()


@cached(cache=SimpleMemoryCache, key=lambda *args, **kwargs: args[0], ttl=30)
async def count_token_utilization(org_id: UUID, db: AsyncSession) -> int:
    stmt = select(func.sum(DataFeed.tokenCount)).where(
        DataFeed.tokenCount != -1, DataFeed.activeStatus == 1, DataFeed.orgId == org_id
    )
    result = await db.execute(stmt)
    return result.scalar() or 0


async def fetch_datafeeds_with_tags(
    org_id: UUID, db: AsyncSession
) -> Sequence[DataFeed]:
    stmt = (
        select(DataFeed)
        .filter(DataFeed.orgId == org_id)
        .options(selectinload(DataFeed.dataFeedTags).selectinload(DataFeedTag.tag))
        .order_by(desc(DataFeed.createdDatetime))
    )
    data_feeds = await db.execute(stmt)
    return data_feeds.scalars().fetchall()


async def fetch_datafeed_tags(
    data_feed_ids: List[UUID], db: AsyncSession
) -> Sequence[DataFeedTag]:
    stmt = select(DataFeedTag).filter(DataFeedTag.dataFeedId.in_(data_feed_ids))
    datafeed_tags = await db.execute(stmt)
    return datafeed_tags.scalars().fetchall()


async def fetch_datafeed_by_name(
    org_id: UUID, data_feed_name: str, db: AsyncSession
) -> DataFeed | None:
    stmt = (
        select(DataFeed)
        .filter(DataFeed.orgId == org_id, DataFeed.dataFeedName == data_feed_name)
        .options(selectinload(DataFeed.dataFeedTags).selectinload(DataFeedTag.tag))
    )
    data_feeds = await db.execute(stmt)
    return data_feeds.scalars().first()


async def fetch_datafeeds_by_ids(
    org_id: UUID, data_feed_ids: List[UUID], processed_only: bool, db: AsyncSession
) -> Sequence[DataFeed]:
    if processed_only:
        stmt = (
            select(DataFeed)
            .filter(
                DataFeed.orgId == org_id,
                DataFeed.activeStatus == 1,
                DataFeed.dataFeedId.in_(data_feed_ids),
            )
            .order_by(asc(DataFeed.createdDatetime))
        )
    else:
        stmt = (
            select(DataFeed)
            .filter(
                DataFeed.orgId == org_id,
                DataFeed.dataFeedId.in_(data_feed_ids),
            )
            .order_by(asc(DataFeed.createdDatetime))
        )
    data_feeds = await db.execute(stmt)
    return data_feeds.scalars().fetchall()


async def fetch_url_datafeed_by_ids(
    user_id: UUID, url_datafeed_ids: List[UUID], db: AsyncSession
) -> Sequence[TempUrlDataFeed]:
    stmt = select(TempUrlDataFeed).filter(
        TempUrlDataFeed.userId == user_id,
        TempUrlDataFeed.urlId.in_(url_datafeed_ids),
    )
    data_feeds = await db.execute(stmt)
    return data_feeds.scalars().fetchall()


async def delete_url_datafeed_group(user_id: UUID, group_id: UUID, db: AsyncSession):
    stmt = delete(TempUrlDataFeed).filter(
        TempUrlDataFeed.userId == user_id, TempUrlDataFeed.groupId == group_id
    )
    await db.execute(stmt)


async def count_existing_chatbot(org_id: UUID, db: AsyncSession) -> int:
    stmt = select(func.count()).where(
        Chatbot.orgId == org_id, Chatbot.activeStatus == True
    )
    result = await db.execute(stmt)
    return result.scalar()


async def fetch_existing_chatbot(
    org_id: UUID,
    db: AsyncSession,
    chatbot_name: str = None,
    chatbot_id: UUID = None,
) -> Chatbot:
    if chatbot_id:
        stmt_tag = select(Chatbot).filter(
            Chatbot.chatbotId == chatbot_id,
            Chatbot.orgId == org_id,
            Chatbot.activeStatus == True,
        )
    else:
        stmt_tag = select(Chatbot).filter(
            Chatbot.chatbotName == chatbot_name,
            Chatbot.orgId == org_id,
            Chatbot.activeStatus == True,
        )
    result_tag = await db.execute(stmt_tag)
    existing_chatbot = result_tag.scalar_one_or_none()
    return existing_chatbot


async def fetch_chatbot_directly(
    db: AsyncSession, chatbot_id: UUID, ensure: bool = False
) -> Chatbot:
    stmt_tag = select(Chatbot).filter(
        Chatbot.chatbotId == chatbot_id,
        Chatbot.activeStatus == True,
    )
    result_tag = await db.execute(stmt_tag)
    chatbot = result_tag.scalar_one_or_none()

    if ensure and chatbot is None:
        raise HTTPException(status_code=404, detail="No such chatbot found")

    return chatbot


async def fetch_org_all_chatbots_id_name(
    org_id: UUID,
    db: AsyncSession,
) -> Sequence[Row[tuple[UUID, str]]]:
    stmt_tag = select(Chatbot.chatbotId, Chatbot.chatbotName).filter(
        Chatbot.orgId == org_id, Chatbot.activeStatus == True
    )
    result_tag = await db.execute(stmt_tag)
    all_active_chatbots_id_name = result_tag.fetchall()
    return all_active_chatbots_id_name


async def fetch_support_ticketing_categories_for_chatbot(
    chatbot_id: UUID, db: AsyncSession
) -> Sequence[Row[tuple[ChatbotSupportTicketingCategories]]]:
    stmt_tag = (
        select(ChatbotSupportTicketingCategories)
        .filter(ChatbotSupportTicketingCategories.chatbotId == chatbot_id)
        .order_by(
            asc(ChatbotSupportTicketingCategories.supportTicketingCategorySequence)
        )
    )

    result_tag = await db.execute(stmt_tag)
    return result_tag.fetchall()


async def fetch_chatbot_datafeeds(
    db: AsyncSession,
    chatbot_id: UUID = None,
    data_feed_ids: List[UUID] = None,
) -> Sequence[ChatbotDataFeeds]:
    if chatbot_id:
        stmt = select(ChatbotDataFeeds).filter(ChatbotDataFeeds.chatbotId == chatbot_id)
    else:
        stmt = select(ChatbotDataFeeds).filter(
            ChatbotDataFeeds.dataFeedId.in_(data_feed_ids)
        )
    chatbot_datafeeds = await db.execute(stmt)
    return chatbot_datafeeds.scalars().fetchall()


async def fetch_chatbots_datafeeds_support_ticketing_categories(
    org_id: UUID, db: AsyncSession
) -> Sequence[Chatbot]:
    # Create a query using the select function
    query = (
        select(Chatbot)
        .filter(
            Chatbot.orgId == org_id, Chatbot.activeStatus == True
        )  # Filter chatbots by the given org_id
        .options(
            selectinload(Chatbot.chatbotDataFeeds)
            .selectinload(ChatbotDataFeeds.datafeed)
            .selectinload(DataFeed.dataFeedTags)
            .selectinload(DataFeedTag.tag),
            selectinload(Chatbot.chatbotSupportTicketingCategories),
            selectinload(Chatbot.latestLeadForm),
        )
        .order_by(desc(Chatbot.chatbotUpdationDate))
    )

    # Execute the query
    result = await db.execute(query)
    return result.scalars().fetchall()


async def fetch_chatbots_support_ticketing_categories(
    chatbot_id: UUID, db: AsyncSession
) -> Chatbot | None:
    # Create a query using the select function
    query = (
        select(Chatbot)
        .filter(Chatbot.chatbotId == chatbot_id, Chatbot.activeStatus == True)
        .options(
            selectinload(Chatbot.chatbotSupportTicketingCategories),
            selectinload(Chatbot.latestLeadForm),
        )
    )

    # Execute the query
    result = await db.execute(query)
    return result.scalar()


async def fetch_tickets(
    org_id: UUID, chatbot_id: UUID, db: AsyncSession
) -> Sequence[Tickets]:
    # Create a query using the select function
    if chatbot_id:
        query = (
            select(Tickets)
            .filter(Tickets.orgId == org_id, Tickets.chatbotId == chatbot_id)
            .order_by(asc(Tickets.createdTime))
        )
    else:
        query = (
            select(Tickets)
            .filter(Tickets.orgId == org_id)
            .order_by(asc(Tickets.createdTime))
        )

    # Execute the query
    result = await db.execute(query)
    return result.scalars().fetchall()


async def fetch_ticket(
    ticket_id: int, org_id: UUID, chatbot_id: UUID, db: AsyncSession
) -> Tickets | None:
    # Create a query using the select function
    query = select(Tickets).filter(
        Tickets.ticketId == ticket_id,
        Tickets.orgId == org_id,
        Tickets.chatbotId == chatbot_id,
    )

    # Execute the query
    result = await db.execute(query)
    return result.scalar()


async def fetch_customer_tickets_email(
    customer_email: str, org_id: UUID, chatbot_id: UUID, db: AsyncSession
) -> Sequence[Tickets]:
    # Create a query using the select function
    if chatbot_id:
        query = (
            select(Tickets)
            .filter(
                Tickets.email == customer_email,
                Tickets.orgId == org_id,
                Tickets.chatbotId == chatbot_id,
            )
            .order_by(desc(Tickets.createdTime))
        )
    else:
        query = (
            select(Tickets)
            .filter(Tickets.email == customer_email, Tickets.orgId == org_id)
            .order_by(desc(Tickets.createdTime))
        )

    result = await db.execute(query)
    return result.scalars().fetchall()


@cached(cache=SimpleMemoryCache, key=lambda *args, **kwargs: args[0], ttl=300)
async def count_message_utilization(
    org_id: UUID, from_datetime: datetime, to_datetime: datetime, db: AsyncSession
) -> int:
    stmt = select(func.sum(Conversation.totalConversationMessage)).where(
        Conversation.createdAt.between(
            from_datetime.replace(tzinfo=None), to_datetime.replace(tzinfo=None)
        ),
        Conversation.orgId == org_id,
    )
    result = await db.execute(stmt)
    return result.scalar()


async def fetch_conversations(
    db: AsyncSession,
    org_id: UUID,
    chatbot_id: UUID = None,
    from_date: datetime = datetime(MINYEAR, 1, 1),
    to_date: datetime = datetime(MAXYEAR, 12, 31, 23, 59, 59, 999999),
) -> Sequence[Conversation]:
    if chatbot_id:
        # Create a query using the select function
        query = (
            select(Conversation)
            .filter(
                Conversation.orgId == org_id,
                Conversation.chatbotId == chatbot_id,
                Conversation.createdAt >= from_date,
                Conversation.createdAt <= to_date,
            )
            .order_by(desc(Conversation.createdAt))
        )
    else:
        query = (
            select(Conversation)
            .filter(
                Conversation.orgId == org_id,
                Conversation.createdAt >= from_date,
                Conversation.createdAt <= to_date,
            )
            .order_by(desc(Conversation.createdAt))
        )

    # Execute the query
    result = await db.execute(query)
    return result.scalars().fetchall()


async def fetch_conversation(
    db: AsyncSession,
    conversation_id: UUID,
) -> Conversation | None:
    query = select(Conversation).filter(
        Conversation.conversationId == conversation_id,
    )
    # Execute the query
    result = await db.execute(query)
    return result.scalar_one_or_none()


async def fetch_messages(db: AsyncSession, conversation_id: UUID) -> Sequence[Message]:
    # Create a query using the select function
    query = (
        select(Message)
        .filter(Message.conversationId == conversation_id)
        .order_by(asc(Message.createdAt))
    )

    # Execute the query
    result = await db.execute(query)
    return result.scalars().fetchall()


async def fetch_data_feed_embedding_message_directly(
    db: AsyncSession, datafeed_id: UUID
) -> DataFeedEmbeddingMessageQueue | None:
    stmt_tag = select(DataFeedEmbeddingMessageQueue).filter(
        DataFeedEmbeddingMessageQueue.dataFeedId == datafeed_id,
        DataFeedEmbeddingMessageQueue.messageStatus == MessageQueueStatusEnum.PENDING,
    )
    result_tag = await db.execute(stmt_tag)
    queue_message = result_tag.scalar_one_or_none()

    return queue_message


async def fetch_conversation_with_messages(
    db: AsyncSession, conversation_id: UUID
) -> Conversation | None:
    """
    Fetch a conversation with its messages + messagesubprocesses
    return None if the conversation with the given id does not exist
    """
    stmt = (
        select(Conversation)
        .options(joinedload(Conversation.messages).subqueryload(Message.subProcesses))
        .where(Conversation.conversationId == conversation_id)
    )

    result = await db.execute(stmt)  # execute the statement
    return result.scalars().first()


async def fetch_managed_conversation_with_messages(
    db: AsyncSession, managed_conversation_id: int
) -> Conversation | None:
    """
    Fetch a conversation with its messages + messagesubprocesses
    return None if the conversation with the given id does not exist
    """
    stmt = (
        select(Conversation)
        .options(joinedload(Conversation.messages).subqueryload(Message.subProcesses))
        .where(Conversation.managedConversationId == managed_conversation_id)
    )

    result = await db.execute(stmt)  # execute the statement
    return result.scalars().first()


async def fetch_message_with_sub_processes(
    db: AsyncSession, message_id: str
) -> Message | None:
    """
    Fetch a message with its sub processes
    return None if the message with the given id does not exist
    """
    # Eagerly load required relationships
    stmt = (
        select(Message)
        .options(joinedload(Message.subProcesses))
        .where(Message.messageId == message_id)
    )
    result = await db.execute(stmt)  # execute the statement
    return result.scalars().first()  # get the first result


async def fetch_agent_type(db: AsyncSession, agent_type_name: str) -> AgentType | None:
    result = await db.execute(
        select(AgentType).where(AgentType.agentTypeName == agent_type_name)
    )
    agent_type = result.scalars().first()

    if not agent_type:
        db.add(AgentType(agentTypeName=agent_type_name))
        await db.commit()
        result = await db.execute(
            select(AgentType).where(AgentType.agentTypeName == agent_type_name)
        )
        agent_type = result.scalars().first()

    return agent_type


async def fetch_random_auth_cred(
    db: AsyncSession, auth_cred_type: str
) -> Sequence[str] | None | str:
    result = await db.execute(
        select(SysAuthCred.authCred).filter(SysAuthCred.type == auth_cred_type)
    )
    return result.scalars().all()


async def fetch_url_scraping_message(
    db: AsyncSession, group_id: UUID, user_id: UUID
) -> URLScrapingMessageQueue | None:
    stmt = select(URLScrapingMessageQueue).where(
        URLScrapingMessageQueue.groupId == group_id,
        URLScrapingMessageQueue.userId == user_id,
    )
    result = await db.execute(stmt)
    return result.scalars().first()


async def fetch_scraped_urls(
    db: AsyncSession, group_id: UUID, user_id: UUID
) -> Sequence[TempUrlDataFeed]:
    stmt = select(TempUrlDataFeed).where(
        TempUrlDataFeed.groupId == group_id,
        TempUrlDataFeed.userId == user_id,
    )
    result = await db.execute(stmt)
    return result.scalars().all()


async def fetch_chatbot_lead_form(
    db: AsyncSession,
    chatbot_id: UUID,
    org_id: UUID | None = None,
) -> Chatbot | None:
    stmt = (
        select(Chatbot)
        .filter(Chatbot.chatbotId == chatbot_id)
        .options(selectinload(Chatbot.latestLeadForm))
    )

    if org_id:
        stmt = stmt.filter(Chatbot.orgId == org_id)

    result = await db.execute(stmt)
    return result.scalars().first()


async def fetch_leads(
    db: AsyncSession,
    org_id: UUID,
    chatbot_id: UUID = None,
    from_date: datetime = datetime(MINYEAR, 1, 1),
    to_date: datetime = datetime(MAXYEAR, 12, 31, 23, 59, 59, 999999),
) -> Sequence[Lead]:
    # Create a query using the select function
    query = (
        select(Lead)
        .filter(
            Lead.orgId == org_id,
            Lead.chatbotId == chatbot_id,
            Lead.createdAt >= from_date,
            Lead.createdAt <= to_date,
        )
        .options(selectinload(Lead.leadForm))
        .order_by(desc(Lead.createdAt))
    )

    # Execute the query
    result = await db.execute(query)
    return result.scalars().fetchall()
